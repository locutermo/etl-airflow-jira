"""
DAG Principal: Extracción Incremental y Carga Analítica de Jira Cloud.
Frecuencia: Cada 30 minutos.
Implementa el buffer de seguridad de 15 minutos, paginación por nextPageToken,
storing raw JSONB (Zero-API backfill), normalización dimensional y cálculo de tiempos.
"""

import os
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import pytz

from jira_client import JiraClient
from jira_transformer import JiraTransformer
from db import DWRepository, get_dw_connection, dw_cursor
from alerts import notify_webhook_failure

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gobierno-ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': notify_webhook_failure,
}

def task_sync_metadata(**context):
    """Sincroniza el catálogo de proyectos y campos desde Jira Cloud hacia el DW."""
    client = JiraClient()
    logger.info("Sincronizando proyectos desde Jira Cloud...")
    projects = client.get_projects()
    DWRepository.upsert_projects(projects)

    logger.info("Sincronizando catálogo de campos de Jira...")
    fields = client.get_fields()
    DWRepository.upsert_fields(fields)

def task_extract_and_stage(**context):
    """
    Lee el watermark actual o parámetro manual, construye el JQL con filtro opcional de proyectos,
    extrae los issues de Jira y los inserta en bruto en stg_jira_issues_raw.
    """
    params = context.get('params', {})
    project_keys_param = params.get('project_keys', 'ALL')
    since_date_param = params.get('since_date', '')
    max_records_param = int(params.get('max_records', 500) or 500)

    # 1. Determinar fecha base (Watermark o manual)
    if since_date_param and str(since_date_param).strip():
        watermark_str = str(since_date_param).strip()
        try:
            buffered_sync = datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))
        except Exception:
            buffered_sync = datetime.strptime(watermark_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=pytz.UTC)
        max_updated_at = buffered_sync
    else:
        pipeline_name = "etl_jira_incremental_sync"
        watermark_info = DWRepository.get_watermark(pipeline_name)
        if watermark_info and watermark_info.get("last_successful_sync"):
            last_sync = watermark_info["last_successful_sync"]
        else:
            last_sync = datetime(2020, 1, 1, tzinfo=pytz.UTC)
        buffered_sync = last_sync - timedelta(minutes=15)
        watermark_str = buffered_sync.strftime("%Y-%m-%d %H:%M")
        max_updated_at = last_sync

    # 2. Construir filtro de proyectos (JQL)
    jql_parts = []
    if project_keys_param and str(project_keys_param).strip() != 'ALL':
        proj_list = [p.strip().upper() for p in str(project_keys_param).split(',') if p.strip()]
        if proj_list:
            proj_str = ", ".join([f'"{p}"' for p in proj_list])
            jql_parts.append(f"project IN ({proj_str})")
            logger.info(f"Filtro selectivo de proyectos activo: {proj_list}")
    else:
        # Verificar si hay proyectos deshabilitados en dim_jira_project
        with get_dw_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT project_key FROM dim_jira_project WHERE is_sync_enabled = TRUE;")
                enabled_keys = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT COUNT(*) FROM dim_jira_project;")
                total_keys_count = cur.fetchone()[0]
                if 0 < len(enabled_keys) < total_keys_count:
                    proj_str = ", ".join([f'"{p}"' for p in enabled_keys])
                    jql_parts.append(f"project IN ({proj_str})")
                    logger.info(f"Sincronizando únicamente los {len(enabled_keys)} proyectos marcados como activos.")

    jql_parts.append(f'updated >= "{watermark_str}"')
    jql = " AND ".join(jql_parts) + " ORDER BY updated ASC"

    client = JiraClient()
    total_staged = 0

    for batch in client.search_issues_incremental(jql=jql, batch_size=100):
        DWRepository.upsert_raw_staging(batch)
        total_staged += len(batch)

        for issue in batch:
            upd_str = issue.get("fields", {}).get("updated")
            if upd_str:
                try:
                    dt = datetime.fromisoformat(upd_str.replace("Z", "+00:00"))
                    if dt > max_updated_at:
                        max_updated_at = dt
                except Exception:
                    pass

        if max_records_param and total_staged >= max_records_param:
            logger.info(f"Límite de lote alcanzado ({total_staged}/{max_records_param} tickets). El siguiente lote continuará desde {max_updated_at}.")
            break

    context['ti'].xcom_push(key='total_staged', value=total_staged)
    context['ti'].xcom_push(key='buffered_sync_iso', value=buffered_sync.isoformat())
    context['ti'].xcom_push(key='max_updated_iso', value=max_updated_at.isoformat())
    context['ti'].xcom_push(key='project_keys_param', value=str(project_keys_param))
    logger.info(f"Extracción finalizada. Total registros en Staging: {total_staged}")

def task_transform_and_load(**context):
    """
    Toma los registros de Staging afectados desde la fecha del buffer,
    los procesa con JiraTransformer y los carga en las tablas dimensionales y hechos.
    """
    ti = context['ti']
    total_staged = ti.xcom_pull(task_ids='extract_and_stage', key='total_staged') or 0
    buffered_sync_iso = ti.xcom_pull(task_ids='extract_and_stage', key='buffered_sync_iso')

    if total_staged == 0 and not buffered_sync_iso:
        logger.info("No hay registros nuevos o modificados para transformar.")
        return

    buffered_sync = datetime.fromisoformat(buffered_sync_iso) if buffered_sync_iso else datetime(2020, 1, 1, tzinfo=pytz.UTC)

    # Cargar reglas de homologación
    rules = DWRepository.get_homologation_rules()
    transformer = JiraTransformer(homologation_rules=rules)

    # Leer lotes desde Staging en chunks
    chunk_size = 500
    offset = 0

    conn = get_dw_connection()
    try:
        while True:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT raw_payload
                    FROM stg_jira_issues_raw
                    WHERE updated_at >= %s
                    ORDER BY updated_at ASC
                    LIMIT %s OFFSET %s;
                """, (buffered_sync, chunk_size, offset))
                rows = cur.fetchall()

            if not rows:
                break

            transformed_batch = []
            for row in rows:
                raw_payload = row[0]
                transformed = transformer.transform_issue(raw_payload)
                transformed_batch.append(transformed)

            DWRepository.upsert_transformed_data(transformed_batch)
            offset += len(rows)
            logger.info(f"Transformados y cargados {offset} tickets...")

            if len(rows) < chunk_size:
                break
    finally:
        conn.close()

def task_calculate_intervals_and_stats(**context):
    """
    Ejecuta el cálculo de tiempos en sp_calculate_changelog_intervals,
    refresca meta_project_sync_stats y actualiza el watermark oficial.
    """
    ti = context['ti']
    buffered_sync_iso = ti.xcom_pull(task_ids='extract_and_stage', key='buffered_sync_iso')
    max_updated_iso = ti.xcom_pull(task_ids='extract_and_stage', key='max_updated_iso')
    total_staged = ti.xcom_pull(task_ids='extract_and_stage', key='total_staged') or 0

    buffered_sync = datetime.fromisoformat(buffered_sync_iso) if buffered_sync_iso else datetime.now(pytz.UTC)
    max_updated = datetime.fromisoformat(max_updated_iso) if max_updated_iso else datetime.now(pytz.UTC)

    # 1. Calcular permanencia en segundos (sp_calculate_changelog_intervals)
    DWRepository.call_sp_calculate_changelog_intervals(buffered_sync)

    # 2. Refrescar estadísticas agregadas para el tablero diario
    DWRepository.refresh_project_sync_stats()

    # 3. Actualizar Watermark
    DWRepository.update_watermark(
        pipeline_name="etl_jira_incremental_sync",
        sync_dt=max_updated,
        records_processed=total_staged,
        status="SUCCESS"
    )
    logger.info(f"Ciclo incremental completado con éxito. Watermark fijado en: {max_updated}")

with DAG(
    'etl_jira_incremental_sync',
    default_args=default_args,
    description='Sincronización incremental cada 30 min de Jira Cloud a PostgreSQL DW',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'project_keys': Param(
            'ALL',
            type='string',
            description='Prefijos de proyectos a extraer separados por coma (ej. MESA, TI) o ALL para todos los activos'
        ),
        'max_records': Param(
            500,
            type='integer',
            description='Cantidad máxima de tickets a extraer en este lote'
        ),
        'since_date': Param(
            '',
            type='string',
            description='Fecha inicial opcional YYYY-MM-DD HH:mm (vacío para usar watermark automático)'
        ),
    },
    tags=['jira', 'etl', 'gobierno-ti', 'incremental'],
) as dag:

    sync_metadata = PythonOperator(
        task_id='sync_metadata_and_projects',
        python_callable=task_sync_metadata,
    )

    extract_and_stage = PythonOperator(
        task_id='extract_and_stage',
        python_callable=task_extract_and_stage,
    )

    transform_and_load = PythonOperator(
        task_id='transform_and_load_dw',
        python_callable=task_transform_and_load,
    )

    calculate_stats = PythonOperator(
        task_id='calculate_intervals_and_stats',
        python_callable=task_calculate_intervals_and_stats,
    )

    sync_metadata >> extract_and_stage >> transform_and_load >> calculate_stats

