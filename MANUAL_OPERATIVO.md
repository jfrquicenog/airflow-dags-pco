# Manual Operativo — Monitoreo de Transaccionalidad de Aliados PCO

**Versión:** 1.0  
**Fecha:** Octubre 2026  
**DAG:** `monitoring_aliados_pco`  
**Plataforma:** Apache Airflow 3.2.2 / Dynatrace SaaS  
**Audiencia:** Equipos de operaciones, soporte y monitoreo PCO

---

## 1. ¿Qué hace esta automatización?

Este DAG (Directed Acyclic Graph) de Airflow ejecuta una **rutina operativa automática** que monitorea la transaccionalidad de los aliados comerciales de Puntos Colombia en Dynatrace.

### En palabras simples

Cada hora, el sistema:

1. **Consulta Dynatrace** y obtiene la lista completa de aliados comerciales (~1.191 partnerCodes con su nombre)
2. **Extrae 6 métricas** de los últimos 3 días para cada aliado:
   - Puntos acumulados POS (datáfonos)
   - Puntos acumulados MKP (marketplace)
   - Puntos redimidos POS
   - Puntos redimidos MKP
   - Conteo de transacciones POS
   - Conteo de transacciones IF016
3. **Analiza el comportamiento** y detecta aliados con actividad anormal
4. **Notifica a Teams** si encuentra anomalías
5. **Limpia logs** antiguos para no llenar el disco

### ¿De dónde salen los datos?

Los datos provienen del tenant de Dynatrace de PCO (`https://rtq26220.live.dynatrace.com`), específicamente de las métricas que los procesadores de OpenPipeline extraen de los logs de los microservicios backend. Estas son las mismas métricas que alimentan el tablero **BUS | LEI | Resumen Transaccional Aliados**.

---

## 2. Tiempos de ejecución

| Parámetro | Valor |
|---|---|
| **Frecuencia** | Cada hora (`0 * * * *`) |
| **Ventana de análisis** | Últimos 3 días completos |
| **Duración típica** | ~12 segundos |
| **Timeout por task** | 10 minutos |
| **Reintentos** | 2 (con 5 minutos de espera entre cada uno) |

### Cronograma de ejecución

```
00:00 → ejecución
01:00 → ejecución
02:00 → ejecución
...
23:00 → ejecución
```

Cada ejecución es independiente. Si una falla, se reintenta automáticamente hasta 2 veces.

---

## 3. Tipos de anomalías y qué significan

El sistema detecta **4 tipos de problemas**. Cada uno tiene un nivel de severidad y una acción recomendada.

### 🔴 ZERO — Métricas en cero (CRÍTICA)

**Qué significa:** El aliado tiene registros en Dynatrace pero todos los puntos y transacciones de los últimos 3 días están en cero.

**Posibles causas:**
- El aliado dejó de transaccionar completamente
- Caída del servicio del aliado (API, conectividad)
- Problema en el pipeline de ingesta de logs
- El aliado fue suspendido o desactivado

**¿Qué hacer?**
1. Verificar en el tablero de Dynatrace **BUS | LEI | Resumen Transaccional Aliados** si el aliado muestra actividad
2. Si el tablero también muestra cero → contactar al aliado o revisar su conectividad
3. Si el tablero muestra data pero el DAG reporta cero → escalar a equipo de observabilidad (posible problema de ingesta)

---

### 🟡 FLAT — Métrica lineal (WARNING)

**Qué significa:** El aliado tiene actividad pero los valores son idénticos todos los días (desviación estándar ≈ 0). Esto es estadísticamente improbable en transacciones reales.

**Posibles causas:**
- Datos repetidos o cacheados incorrectamente
- Un job batch que inyecta siempre el mismo valor
- Problema en el procesador de OpenPipeline

**¿Qué hacer?**
1. Revisar el tablero de Dynatrace para confirmar si los valores son realmente idénticos
2. Si son idénticos → escalar a equipo de observabilidad para revisar el pipeline
3. Si el tablero muestra variación → reportar falso positivo al equipo de automatización

---

### 🟡 DECLINING — Tendencia descendente (WARNING)

**Qué significa:** Los puntos acumulados disminuyen consistentemente día tras día y el último día cae a menos del 10% del primer día.

**Ejemplo:** `[164.451 → 110.245 → 494]` (cayó 99.7%)

**Posibles causas:**
- Degradación progresiva del servicio del aliado
- Campaña promocional que terminó (normal)
- Pérdida gradual de conectividad
- Aliado en proceso de migración o cierre

**¿Qué hacer?**
1. Revisar el tablero de Dynatrace para entender la tendencia
2. Verificar si el aliado tuvo una campaña o promoción que terminó recientemente (puede ser comportamiento esperado)
3. Si no hay causa conocida → contactar al aliado para verificar su estado operativo
4. Si la tendencia continúa en la siguiente ejecución → escalar a soporte de aliados

---

### 🟠 INACTIVE — Sin datos (INFO)

**Qué significa:** El aliado aparece en el catálogo pero no tiene ningún data point en las métricas de los últimos 3 días.

**Posibles causas:**
- Aliado nuevo que aún no transacciona
- Aliado inactivo temporalmente (ej. mantenimiento)
- Aliado de baja frecuencia que no transaccionó en la ventana

**¿Qué hacer?**
- Revisar si es un aliado conocido y si la inactividad es esperada
- Si es un aliado que debería estar activo → investigar conectividad
- No requiere acción inmediata a menos que se repita por múltiples ejecuciones

---

## 4. La notificación de Teams

Cuando se detectan anomalías, el DAG envía un mensaje al canal de Teams configurado. El mensaje tiene este formato:

```
**Monitoreo Aliados PCO - 2026-10-02 17:15**

Anomalias detectadas: **4** (🔴 0 Criticas, 🟡 4 Warnings, 🟠 0 Info)

| partnerCode | Nombre        | Tipo       | Severidad     | Detalle                        |
|-------------|---------------|------------|---------------|--------------------------------|
| 805026021   | RTA Design    | DECLINING  | 🟡 WARNING    | Declive: 164451 -> 494        |
| 860534221   | Pan Pa Ya     | DECLINING  | 🟡 WARNING    | Declive: 45398 -> 2754        |
| 900167786   | Kanú          | DECLINING  | 🟡 WARNING    | Declive: 56715 -> 4950        |
| 900447351   | Virtual Llantas| DECLINING | 🟡 WARNING    | Declive: 183221 -> 3972       |
```

### Cómo leer la tabla

| Columna | Significado |
|---|---|
| **partnerCode** | NIT del aliado comercial |
| **Nombre** | Nombre comercial del aliado |
| **Tipo** | Tipo de anomalía (ZERO, FLAT, DECLINING, INACTIVE) |
| **Severidad** | 🔴 CRÍTICA / 🟡 WARNING / 🟠 INFO |
| **Detalle** | Resumen cuantitativo de la anomalía |

### Cuándo NO se envía notificación

Si no hay anomalías, **no se envía nada**. El DAG se ejecuta silenciosamente. La ausencia de mensajes significa que todos los aliados están transaccionando normalmente.

---

## 5. Procedimiento de revisión

### Paso a paso cuando llega una notificación

```
┌─────────────────────────────────────────────────────┐
│  LLEGA NOTIFICACIÓN DE TEAMS                       │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  PASO 1: Identificar severidad                       │
│  🔴 CRÍTICA → actuar en < 30 min                     │
│  🟡 WARNING → actuar en < 2 horas                    │
│  🟠 INFO → revisar en el turno                       │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  PASO 2: Abrir el tablero de Dynatrace               │
│  BUS | LEI | Resumen Transaccional Aliados           │
│  Seleccionar el NIT del aliado afectado              │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  PASO 3: Comparar lo que ve el tablero               │
│  vs lo que reporta la notificación                   │
│  ¿Coinciden los valores? ¿Hay data en el tablero?    │
└──────────────────────┬──────────────────────────────┘
                       │
              ┌────────┴────────┐
              ▼                 ▼
     ┌─────────────────┐  ┌──────────────────┐
     │  TABLERO TAMBIÉN │  │  TABLERO SÍ TIENE│
     │  MUESTRA ANOMALÍA│  │  DATA NORMAL     │
     └────────┬────────┘  └────────┬─────────┘
              ▼                      ▼
     ┌─────────────────┐  ┌──────────────────┐
     │  PASO 4a:        │  │  PASO 4b:        │
     │  Problema real   │  │  Falso positivo  │
     │  Contactar aliado│  │  Escalar a obs.  │
     │  o investigar    │  │  (gap de métrica)│
     └─────────────────┘  └──────────────────┘
```

### Contactos de escalamiento

| Nivel | Contactar a | Cuándo |
|---|---|---|
| **Nivel 1** | Equipo de operaciones PCO | Anomalías WARNING o INFO |
| **Nivel 2** | Soporte de aliados PCO | DECLINING confirmado o ZERO |
| **Nivel 3** | Equipo de observabilidad | Discrepancia entre DAG y tablero |
| **Nivel 4** | Equipo de automatización | Falsos positivos recurrentes |

---

## 6. Cómo acceder al DAG en Airflow

### Credenciales

| Campo | Valor |
|---|---|
| **URL** | `http://172.16.4.8:8080` (vía port-forward) |
| **Usuario** | `admin` |
| **Password** | `admin` |

### Acceso por port-forward

```bash
ssh arusobs@172.16.4.8
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

Luego abrir `http://localhost:8080` en el navegador.

### Qué ver en Airflow

1. **DAGs** → buscar `monitoring_aliados_pco`
2. Click sobre el DAG → ver historial de ejecuciones
3. Click sobre una ejecución → ver el estado de cada task
4. Click sobre un task → ver logs detallados

---

## 7. Métricas que se monitorean

| Métrica | Origen | Descripción |
|---|---|---|
| `log.val-pos-puntosacumulados` | OpenPipeline | Puntos acumulados en datáfonos POS |
| `log.val-mkp-puntosacumulados` | OpenPipeline | Puntos acumulados en Marketplace |
| `log.val-pos-puntosredmidos` | OpenPipeline | Puntos redimidos en POS |
| `log.val-mkp-puntosredimidos` | OpenPipeline | Puntos redimidos en Marketplace |
| `log.cnt-pos-acumulaciones` | OpenPipeline | Conteo de transacciones POS |
| `log.cnt-trn-acumulaciones-if016` | OpenPipeline | Conteo de transacciones IF016 |
| `log.cnt-nombre-aliados-pco` | OpenPipeline | Catálogo de NIT → Nombre |

Todas se consultan vía **Dynatrace Metrics API v2** con resolución diaria (`1d`) y ventana de 3 días.

---

## 8. Preguntas frecuentes

**¿Por qué un aliado aparece como anómalo si en el tablero se ve normal?**

Puede haber una diferencia de granularidad. El DAG usa resolución diaria (totales por día), mientras que el tablero usa resolución de 30 minutos. El **total** debe coincidir, pero el promedio/máximo/mínimo pueden diferir. Comparar siempre el total.

**¿Por qué no llegó notificación pero el aliado está en cero?**

Posibles causas:
- El aliado no aparece en el catálogo (`log.cnt-nombre-aliados-pco`) → no se monitorea
- El aliado está en la lista de exclusión (partnerCodes de test o genéricos)
- El DAG aún no se ha ejecutado en la hora actual

**¿Puedo agregar un aliado a la lista de exclusión?**

Sí, modificando la variable `PARTNER_EXCLUDE` en el archivo `dags/monitoring_aliados_pco.py` del repositorio `airflow-dags-pco`. El cambio se propaga automáticamente en ~60 segundos vía GitSync.

**¿Qué pasa si Dynatrace no responde?**

El DAG reintenta 2 veces con 5 minutos de espera. Si las 3 ejecuciones fallan, el DAG run queda en estado `failed` y se puede ver en la UI de Airflow.

**¿Cómo sé que el DAG está corriendo?**

En Airflow UI, el DAG `monitoring_aliados_pco` debe estar en estado `active` (verde). Si está pausado (gris), no se ejecutará. Se puede despausar con el botón toggle en la UI.

---

## 9. Glosario

| Término | Definición |
|---|---|
| **DAG** | Directed Acyclic Graph — flujo de tareas en Airflow |
| **partnerCode** | NIT del aliado comercial en PCO |
| **OpenPipeline** | Sistema de Dynatrace que procesa logs antes de almacenarlos en Grail |
| **Metrics API v2** | API de Dynatrace para consultar métricas |
| **GitSync** | Sidecar que sincroniza el repo de DAGs cada 60 segundos |
| **CeleryExecutor** | Executor de Airflow que distribuye tasks a workers |
| **XCom** | Mecanismo de Airflow para pasar datos entre tasks |
| **Port-forward** | Técnica para acceder un servicio de Kubernetes desde local |

---

## 10. Archivos relacionados

| Archivo | Ubicación | Propósito |
|---|---|---|
| `monitoring_aliados_pco.py` | `dags/` | Código del DAG |
| `README.md` | raíz del repo | Documentación del repo |
| `MANUAL_OPERATIVO.md` | raíz del repo | Este manual |
| `.env` | `automatizaciones/` | Token de Dynatrace y credenciales |

---

*Documento mantenido por equipo de automatización PCO.  
Para sugerencias o cambios, crear un issue en `https://github.com/jfrquicenog/airflow-dags-pco`.*
