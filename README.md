# Airflow DAGs — Laboratorio PCO

Repositorio de DAGs para Apache Airflow 3.2.2 desplegado en cluster K3s (VMDECOPLABOBS01).

## Estructura

```
airflow-dags-pco/
├── dags/           # Archivos DAG (.py) — sincronizados via GitSync
├── plugins/        # Plugins custom de Airflow
├── include/        # Recursos auxiliares (JSON, YAML, plantillas)
├── config/         # Configuraciones y variables de Airflow
├── scripts/        # Scripts utilitarios
├── tests/          # Tests de validación de DAGs
└── requirements.txt
```

## Sincronización

Los DAGs se sincronizan automáticamente al cluster vía **GitSync** (sidecar container en cada pod de Airflow). El intervalo de sync es de 60 segundos.

- **Repo:** https://github.com/jfrquicenog/airflow-dags-pco
- **Branch:** main
- **SubPath:** dags/
- **Destino en pods:** /opt/airflow/dags/

## Convenciones

- Un DAG por archivo, nombrado en kebab-case: `mi_dag.py`
- Cada DAG debe tener `dag_id` en snake_case
- Usar `tags` para categorizar: `["pco", "dynatrace", "lab"]`
- Documentar el propósito en el docstring del archivo

## Deploy

```bash
git add dags/mi_dag.py
git commit -m "feat: nuevo DAG para ..."
git push origin main
# GitSync lo recoge en ~60s
```
