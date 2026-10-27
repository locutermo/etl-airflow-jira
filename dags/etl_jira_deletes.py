"""
DAG de Reconciliación de Borrados Físicos en Jira Cloud.
Utiliza Jira Expressions API para extraer hasta 10,000 IDs por request sin costo de descarga de tickets.
Detecta issues eliminados y los marca con borrado lógico (is_deleted = TRUE).
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

from jira_client import JiraClient
from db import get_dw_connection, DWRepository
from alerts import notify_webhook_failure

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gobierno-ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_webhook_failure,
}

def task_reconcile_deletes(**context):
    """
    Recorre los espacios de trabajo activos y concilia los IDs existentes en Jira Cloud
    frente a fact_jira_issues_core mediante UNNEST en PostgreSQL.
    """
    client = JiraClient()
    conn = get_dw_connection()

    try:
        with conn.cursor() as cur:
            # Obtener proyectos activos
            cur.execute("""
                SELECT project_key, project_name
                FROM dim_jira_project
                WHERE is_sync_enabled = TRUE
                ORDER BY project_key ASC;
            """)
            projects = cur.fetchall()

        logger.info(f"Iniciando reconciliación de borrados para {len(projects)} proyectos...")
        total_deleted_found = 0

        for p_key, p_name in projects:
            try:
                active_ids = client.get_active_issue_ids_via_expression(project_key=p_key)
                if not active_ids:
                    logger.warning(f"No se obtuvieron IDs activos para '{p_key}'. Omitiendo borrado para evitar falsos positivos.")
                    continue

                with conn.cursor() as cur:
                    # Marcar como eliminados los que están en DW pero no vinieron en la expresión de Jira
                    cur.execute("""
                        UPDATE fact_jira_issues_core f
                        SET is_deleted = TRUE,
                            deleted_detected_at = CURRENT_TIMESTAMP
                        WHERE f.project_key = %s
                          AND f.is_deleted = FALSE
                          AND NOT EXISTS (
                              SELECT 1 FROM UNNEST(%s::BIGINT[]) AS active_id
                              WHERE active_id = f.issue_id
                          );
                    """, (p_key, active_ids))
                    deleted_count = cur.rowcount
                    conn.commit()

                    if deleted_count > 0:
                        total_deleted_found += deleted_count
                        logger.info(f"[{p_key}] Detectados y marcados {deleted_count} tickets eliminados físicamente.")
                    else:
                        logger.info(f"[{p_key}] Sincronizado: 0 eliminados detectados.")

            except Exception as e:
                logger.error(f"Error conciliando eliminados para el proyecto {p_key}: {e}")

        # Refrescar estadísticas en meta_project_sync_stats
        DWRepository.refresh_project_sync_stats()
        logger.info(f"Reconciliación de borrados completada. Total eliminados detectados: {total_deleted_found}")

    finally:
        conn.close()

with DAG(
    'etl_jira_reconciliation_deletes',
    default_args=default_args,
    description='Reconciliación nocturna de tickets eliminados en Jira Cloud vía Jira Expressions API',
    schedule_interval='0 3 * * *', # 3:00 AM diario
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['jira', 'reconciliation', 'deletes', 'gobierno-ti'],
) as dag:

    reconcile_deletes = PythonOperator(
        task_id='reconcile_deleted_issues',
        python_callable=task_reconcile_deletes,
    )

