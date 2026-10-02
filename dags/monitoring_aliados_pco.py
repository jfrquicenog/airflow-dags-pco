"""
DAG: monitoring_aliados_pco
Rutina operativa: monitoreo de transaccionalidad de aliados PCO en Dynatrace.

Detecta aliados con metricas en cero o lineales en los ultimos 3 dias
y notifica a un canal de Teams si encuentra anomalias.

Frecuencia: cada hora
Log retention: 2 dias (dia actual + dia anterior)
"""

import os
import json
import logging
import urllib.request
import urllib.error
import urllib.parse
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ===========================================================================
# Configuracion
# ===========================================================================

DT_BASE_URL = "https://rtq26220.live.dynatrace.com"
DT_TOKEN = Variable.get("dt_api_token", default_var=os.environ.get("DT_API_TOKEN", ""))
TEAMS_WEBHOOK = Variable.get("teams_webhook_url", default_var="")

# Aliados a excluir del monitoreo (test, internos, genericos)
PARTNER_EXCLUDE = {"PUNTOS", "TEST", "000000000", "00000000", "SIN_PARTNER"}

# Metricas a consultar (nombre_interno, metric_selector)
# Todas con :sum o :value segun el tipo de metrica
METRICS_CONFIG = [
    ("pos_acum",     "log.val-pos-puntosacumulados:sum:splitBy(partnerCode)"),
    ("mkp_acum",     "log.val-mkp-puntosacumulados:sum:splitBy(partnerCode)"),
    ("pos_redim",    "log.val-pos-puntosredmidos:sum:splitBy(partnerCode)"),
    ("mkp_redim",    "log.val-mkp-puntosredimidos:sum:splitBy(partnerCode)"),
    ("pos_count",    "log.cnt-pos-acumulaciones:value:splitBy(partnerCode)"),
    ("if016_count",  "log.cnt-trn-acumulaciones-if016:value:splitBy(partnerCode)"),
]

# Ventana de analisis
METRICS_FROM = "now-3d"
METRICS_RESOLUTION = "1d"
ALIADOS_FROM = "now-7d"


# ===========================================================================
# Cliente Dynatrace Metrics API v2
# ===========================================================================

class DynatraceMetricsClient:
    """Cliente minimalista para Metrics API v2 de Dynatrace."""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def query(self, metric_selector: str, from_time: str = "now-1h",
              resolution: str = None) -> dict:
        """Ejecuta una consulta de metricas y retorna el JSON completo."""
        params = {"metricSelector": metric_selector, "from": from_time}
        if resolution:
            params["resolution"] = resolution

        url = f"{self.base_url}/api/v2/metrics/query?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Api-Token {self.token}"}
        )

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"DT API HTTP {e.code}: {err[:500]}")
        except Exception as e:
            raise RuntimeError(f"DT API error: {e}")

    def query_all_partners(self, metric_selector: str, from_time: str,
                           resolution: str = None) -> dict:
        """Retorna {partnerCode: [valores_diarios]} para una metrica."""
        data = self.query(metric_selector, from_time, resolution)
        result = {}
        for series in data.get("result", [{}])[0].get("data", []):
            dims = series.get("dimensionMap", {})
            partner = dims.get("partnerCode", "")
            values = series.get("values", [])
            if partner:
                result[partner] = values
        return result

    def get_aliados(self) -> dict:
        """Obtiene {partnerCode: nombre} desde la metrica de nombres."""
        data = self.query(
            "log.cnt-nombre-aliados-pco:value:splitBy(partnerCode,name)",
            from_time=ALIADOS_FROM
        )
        aliados = {}
        for series in data.get("result", [{}])[0].get("data", []):
            dims = series.get("dimensionMap", {})
            partner = dims.get("partnerCode", "")
            name = dims.get("name", "")
            if partner and name and partner not in PARTNER_EXCLUDE:
                aliados[partner] = name
        return aliados


# ===========================================================================
# Task 1: Obtener lista de aliados
# ===========================================================================

def get_aliados_list(**context):
    """Obtiene la lista de aliados con su nombre desde Dynatrace."""
    log.info("Obteniendo lista de aliados desde Dynatrace...")

    client = DynatraceMetricsClient(DT_BASE_URL, DT_TOKEN)
    aliados = client.get_aliados()

    log.info(f"Total aliados obtenidos: {len(aliados)}")
    for code, name in list(aliados.items())[:5]:
        log.info(f"  {code} -> {name}")
    log.info(f"  ... y {len(aliados) - 5} mas" if len(aliados) > 5 else "")

    # Pasar al siguiente task via XCom
    return aliados


# ===========================================================================
# Task 2: Consultar metricas de los ultimos 3 dias
# ===========================================================================

def fetch_metrics_3d(**context):
    """Consulta 6 metricas en paralelo para todos los aliados."""
    log.info(f"Consultando {len(METRICS_CONFIG)} metricas, ventana={METRICS_FROM}, resolution={METRICS_RESOLUTION}")

    client = DynatraceMetricsClient(DT_BASE_URL, DT_TOKEN)

    # Ejecutar queries en paralelo
    results = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_metric = {
            executor.submit(
                client.query_all_partners,
                selector,
                METRICS_FROM,
                METRICS_RESOLUTION
            ): name
            for name, selector in METRICS_CONFIG
        }

        for future in as_completed(future_to_metric):
            metric_name = future_to_metric[future]
            try:
                data = future.result()
                results[metric_name] = data
                log.info(f"  {metric_name}: {len(data)} partnerCodes con data")
            except Exception as e:
                log.error(f"  {metric_name}: ERROR - {e}")
                results[metric_name] = {}

    # Construir estructura combinada: {partnerCode: {metric: [values]}}
    all_partners = set()
    for metric_data in results.values():
        all_partners.update(metric_data.keys())

    combined = {}
    for partner in all_partners:
        if partner in PARTNER_EXCLUDE:
            continue
        combined[partner] = {}
        for metric_name, metric_data in results.items():
            combined[partner][metric_name] = metric_data.get(partner, [])

    log.info(f"Total partnerCodes con data: {len(combined)}")
    return combined


# ===========================================================================
# Task 3: Detectar anomalias
# ===========================================================================

def _safe_sum(*arrays):
    """Suma arrays elemento a elemento, None = 0."""
    if not arrays:
        return []
    max_len = max((len(a) for a in arrays if a), default=0)
    result = []
    for i in range(max_len):
        total = 0
        has_value = False
        for a in arrays:
            if a and i < len(a) and a[i] is not None:
                total += a[i]
                has_value = True
        result.append(total if has_value else None)
    return result


def _detect_anomaly(partner_code, partner_name, metrics):
    """Aplica reglas de deteccion y retorna anomalia o None."""
    # Combinar metricas
    puntos_acum = _safe_sum(
        metrics.get("pos_acum", []),
        metrics.get("mkp_acum", [])
    )
    puntos_redim = _safe_sum(
        metrics.get("pos_redim", []),
        metrics.get("mkp_redim", [])
    )
    tx_count = _safe_sum(
        metrics.get("pos_count", []),
        metrics.get("if016_count", [])
    )
    total_puntos = _safe_sum(puntos_acum, puntos_redim)

    # Valores numericos (ignorar None)
    puntos_nums = [v for v in total_puntos if v is not None]
    tx_nums = [v for v in tx_count if v is not None]

    # Si no hay ningun data point, es INACTIVE
    if not puntos_nums and not tx_nums:
        return {
            "partnerCode": partner_code,
            "name": partner_name,
            "type": "INACTIVE",
            "severity": "INFO",
            "puntos_diarios": total_puntos,
            "tx_diarios": tx_count,
            "detail": "Sin data points en los ultimos 3 dias"
        }

    # Verificar si todos los dias estan en cero
    all_zero_puntos = all(v == 0 for v in puntos_nums) if puntos_nums else True
    all_zero_tx = all(v == 0 for v in tx_nums) if tx_nums else True

    if all_zero_puntos and all_zero_tx and len(puntos_nums) >= 2:
        return {
            "partnerCode": partner_code,
            "name": partner_name,
            "type": "ZERO",
            "severity": "CRITICAL",
            "puntos_diarios": total_puntos,
            "tx_diarios": tx_count,
            "detail": f"Metricas en cero por {len(puntos_nums)} dias consecutivos"
        }

    # Verificar FLAT (desviacion estandar ~ 0 y media > 0)
    if len(puntos_nums) >= 3:
        mean_val = statistics.mean(puntos_nums)
        try:
            stdev_val = statistics.stdev(puntos_nums)
        except statistics.StatisticsError:
            stdev_val = 0

        if mean_val > 0 and stdev_val < (mean_val * 0.01):
            return {
                "partnerCode": partner_code,
                "name": partner_name,
                "type": "FLAT",
                "severity": "WARNING",
                "puntos_diarios": total_puntos,
                "tx_diarios": tx_count,
                "detail": f"Metrica lineal (stdev={stdev_val:.2f}, mean={mean_val:.2f})"
            }

    # Verificar DECLINING (tendencia descendente consistente)
    if len(puntos_nums) >= 3:
        is_declining = all(
            puntos_nums[i] > puntos_nums[i + 1]
            for i in range(len(puntos_nums) - 1)
        )
        if is_declining and puntos_nums[-1] < (puntos_nums[0] * 0.10):
            return {
                "partnerCode": partner_code,
                "name": partner_name,
                "type": "DECLINING",
                "severity": "WARNING",
                "puntos_diarios": total_puntos,
                "tx_diarios": tx_count,
                "detail": f"Declive: {puntos_nums[0]:.0f} -> {puntos_nums[-1]:.0f}"
            }

    return None


def detect_anomalies(**context):
    """Detecta anomalias en todos los aliados."""
    aliados = context["ti"].xcom_pull(task_ids="get_aliados_list")
    metrics_data = context["ti"].xcom_pull(task_ids="fetch_metrics_3d")

    log.info(f"Analizando {len(metrics_data)} partnerCodes...")

    anomalies = []
    for partner_code, partner_metrics in metrics_data.items():
        partner_name = aliados.get(partner_code, "SIN_NOMBRE")
        anomaly = _detect_anomaly(partner_code, partner_name, partner_metrics)
        if anomaly:
            anomalies.append(anomaly)

    # Ordenar por severidad
    severity_order = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    anomalies.sort(key=lambda a: severity_order.get(a["severity"], 99))

    log.info(f"Anomalias detectadas: {len(anomalies)}")
    for a in anomalies:
        log.info(
            f"  [{a['severity']}] {a['partnerCode']} - {a['name']} "
            f"| {a['type']} | {a['detail']}"
        )

    return anomalies


# ===========================================================================
# Task 4: Notificar a Teams
# ===========================================================================

def notify_teams(**context):
    """Envia notificacion a Teams si hay anomalias."""
    anomalies = context["ti"].xcom_pull(task_ids="detect_anomalies")

    if not anomalies:
        log.info("No hay anomalias para notificar.")
        return

    # Construir mensaje
    critical = [a for a in anomalies if a["severity"] == "CRITICAL"]
    warning = [a for a in anomalies if a["severity"] == "WARNING"]
    info = [a for a in anomalies if a["severity"] == "INFO"]

    lines = []
    lines.append(f"**Monitoreo Aliados PCO - {datetime.now().strftime('%Y-%m-%d %H:%M')}**")
    lines.append("")
    lines.append(f"Anomalias detectadas: **{len(anomalies)}** "
                 f"(🔴 {len(critical)} Criticas, 🟡 {len(warning)} Warnings, 🟠 {len(info)} Info)")
    lines.append("")
    lines.append("| partnerCode | Nombre | Tipo | Severidad | Detalle |")
    lines.append("|---|---|---|---|---|")

    for a in anomalies[:50]:
        icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🟠"}.get(a["severity"], "⚪")
        lines.append(
            f"| {a['partnerCode']} | {a['name']} | {a['type']} | {icon} {a['severity']} | {a['detail']} |"
        )

    if len(anomalies) > 50:
        lines.append(f"| ... | ... | ... | ... | {len(anomalies) - 50} anomalias mas |")

    message = "\n".join(lines)
    log.info(f"Mensaje a enviar:\n{message}")

    if not TEAMS_WEBHOOK:
        log.warning("TEAMS_WEBHOOK no configurado. Notificacion solo en logs.")
        return

    # Enviar a Teams
    body = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        TEAMS_WEBHOOK,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            log.info(f"Notificacion enviada a Teams. HTTP {resp.status}")
    except Exception as e:
        log.error(f"Error enviando a Teams: {e}")


# ===========================================================================
# Task 5: Limpieza de logs
# ===========================================================================

CLEANUP_COMMAND = (
    "find /opt/airflow/logs -type f -mtime +2 -delete 2>/dev/null; "
    "echo 'Logs older than 2 days cleaned up'"
)


# ===========================================================================
# DAG Definition
# ===========================================================================

default_args = {
    "owner": "pco-ops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="monitoring_aliados_pco",
    default_args=default_args,
    description="Monitoreo de transaccionalidad de aliados PCO en Dynatrace",
    schedule="0 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["pco", "dynatrace", "operativo", "aliados"],
    doc_md=__doc__,
) as dag:

    t_get_aliados = PythonOperator(
        task_id="get_aliados_list",
        python_callable=get_aliados_list,
    )

    t_fetch_metrics = PythonOperator(
        task_id="fetch_metrics_3d",
        python_callable=fetch_metrics_3d,
    )

    t_detect = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    t_notify = PythonOperator(
        task_id="notify_teams",
        python_callable=notify_teams,
    )

    t_cleanup = BashOperator(
        task_id="cleanup_logs",
        bash_command=CLEANUP_COMMAND,
        trigger_rule="all_done",
    )

    t_get_aliados >> t_fetch_metrics >> t_detect >> t_notify >> t_cleanup
