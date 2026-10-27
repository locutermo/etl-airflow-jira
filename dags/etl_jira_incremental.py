"""
DAG Principal: Extracción Incremental y Carga Analítica de Jira Cloud.
Frecuencia: Cada hora (ajustable).
Sin límite de registros por defecto: extrae TODOS los issues disponibles.
Implementa buffer de seguridad de 15 minutos, paginación por nextPageToken,
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
from jira_transformer import JiraTransformer, parse_iso_datetime
from db import DWRepository, get_dw_connection, dw_cursor
from alerts import notify_webhook_failure

logger = logging.getLogger(__name__)

LIMA_TZ = pytz.timezone("America/Lima")

default_args = {
    'owner': 'gobierno-ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=8),
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
    max_records_param = int(params.get('max_records', 0) or 0)

    # 1. Determinar fecha base (Watermark o manual) en America/Lima
    if since_date_param and str(since_date_param).strip():
        watermark_str = str(since_date_param).strip()
        buffered_sync = parse_iso_datetime(watermark_str)
        if not buffered_sync:
            try:
                dt_naive = datetime.strptime(watermark_str[:16], "%Y-%m-%d %H:%M")
                buffered_sync = LIMA_TZ.localize(dt_naive)
            except Exception:
                buffered_sync = datetime(2020, 1, 1, 0, 0, 0, tzinfo=LIMA_TZ)
        max_updated_at = buffered_sync
    else:
        pipeline_name = "etl_jira_incremental_sync"
        watermark_info = DWRepository.get_watermark(pipeline_name)
        if watermark_info and watermark_info.get("last_successful_sync"):
            last_sync = watermark_info["last_successful_sync"]
            if last_sync.tzinfo is None:
                last_sync = LIMA_TZ.localize(last_sync)
            else:
                last_sync = last_sync.astimezone(LIMA_TZ)
        else:
            last_sync = datetime(2020, 1, 1, 0, 0, 0, tzinfo=LIMA_TZ)
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
    rules = DWRepository.get_homologation_rules()
    transformer = JiraTransformer(homologation_rules=rules)
    total_staged = 0

    for batch in client.search_issues_incremental(jql=jql, batch_size=100):
        # 1. Guardar en Staging (JSONB Crudo para Zero-API Backfill)
        DWRepository.upsert_raw_staging(batch)

        # 2. Transformar y cargar atómicamente en Core/EAV/Changelog
        transformed_batch = [transformer.transform_issue(issue) for issue in batch]
        DWRepository.upsert_transformed_data(transformed_batch)

        total_staged += len(batch)

        # 3. Rastrear avance temporal real
        for issue in batch:
            upd_str = issue.get("fields", {}).get("updated")
            if upd_str:
                dt = parse_iso_datetime(upd_str)
                if dt and dt > max_updated_at:
                    max_updated_at = dt

        # 4. Checkpoint de seguridad en BD cada 500 tickets
        if total_staged % 500 == 0:
            DWRepository.update_watermark(
                pipeline_name="etl_jira_incremental_sync",
                sync_dt=max_updated_at,
                records_processed=total_staged,
                status="RUNNING"
            )
            logger.info(f"Checkpoint intermedio: {total_staged} tickets confirmados en DW. Watermark: {max_updated_at}")
        elif total_staged % 100 == 0:
            logger.info(f"Progreso: {total_staged} tickets extraídos. Último updated_at: {max_updated_at}")

        if max_records_param and max_records_param > 0 and total_staged >= max_records_param:
            logger.info(f"Límite de lote alcanzado ({total_staged}/{max_records_param} tickets). El siguiente lote continuará desde {max_updated_at}.")
            break

    context['ti'].xcom_push(key='total_staged', value=total_staged)
    context['ti'].xcom_push(key='buffered_sync_iso', value=buffered_sync.isoformat())
    context['ti'].xcom_push(key='max_updated_iso', value=max_updated_at.isoformat())
    context['ti'].xcom_push(key='project_keys_param', value=str(project_keys_param))
    logger.info(f"Ciclo finalizado. Total acumulado y confirmado en DW: {total_staged} registros.")

def task_transform_and_load(**context):
    """
    Capa de reconciliación y garantía de consistencia:
    Verifica que todo registro presente en Staging esté íntegramente transformado en Core.
    Si algún registro quedó rezagado por fallos previos, lo transforma y confirma.
    """
    ti = context['ti']
    total_staged = ti.xcom_pull(task_ids='extract_and_stage', key='total_staged') or 0
    buffered_sync_iso = ti.xcom_pull(task_ids='extract_and_stage', key='buffered_sync_iso')

    buffered_sync = parse_iso_datetime(buffered_sync_iso) if buffered_sync_iso else datetime(2020, 1, 1, 0, 0, 0, tzinfo=LIMA_TZ)

    rules = DWRepository.get_homologation_rules()
    transformer = JiraTransformer(homologation_rules=rules)

    conn = get_dw_connection()
    reconciled_count = 0
    try:
        with conn.cursor() as cur:
            # Buscar tickets en staging que no estén en core o estén desactualizados
            cur.execute("""
                SELECT s.raw_payload
                FROM stg_jira_issues_raw s
                LEFT JOIN fact_jira_issues_core c ON s.issue_id = c.issue_id
                WHERE s.updated_at >= %s
                  AND (c.issue_id IS NULL OR c.updated_at < s.updated_at)
                LIMIT 1000;
            """, (buffered_sync,))
            missing_rows = cur.fetchall()

        if missing_rows:
            transformed = [transformer.transform_issue(r[0]) for r in missing_rows]
            DWRepository.upsert_transformed_data(transformed)
            reconciled_count = len(missing_rows)
            logger.info(f"Reconciliación: {reconciled_count} registros rezagados fueron transformados y sincronizados en Core.")
        else:
            logger.info("Verificación de consistencia: 100% de los tickets en Staging ya están confirmados en Core.")
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

    buffered_sync = parse_iso_datetime(buffered_sync_iso) if buffered_sync_iso else datetime.now(LIMA_TZ)
    max_updated = parse_iso_datetime(max_updated_iso) if max_updated_iso else datetime.now(LIMA_TZ)

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
    description='Sincronización incremental cada hora de Jira Cloud a PostgreSQL DW',
    schedule_interval='0 * * * *',
    start_date=datetime(2026, 1, 1, tzinfo=LIMA_TZ),
    catchup=False,
    max_active_runs=1,
    params={
        'project_keys': Param(
            'ALL',
            type='string',
            description='Prefijos de proyectos a extraer separados por coma (ej. MESA, TI) o ALL para todos los activos'
        ),
        'max_records': Param(
            0,
            type='integer',
            description='Cantidad máxima de tickets a extraer (0 = sin límite, extrae TODO)'
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

