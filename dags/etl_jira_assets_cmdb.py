"""
DAG Diario: Extracción e Integración de la CMDB de Jira Assets (Insight).
Ejecución: 1 vez al día a las 07:00 AM (Zona Horaria America/Lima).
Alcance: Esquema CMDB (ID: 8) con todos sus tipos de objeto y atributos completos.
"""

import os
import logging
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator

from jira_client import JiraClient
from db import DWRepository, get_dw_connection
from alerts import notify_webhook_failure
from constants import LIMA_TZ, JIRA_ASSETS_CMDB_SCHEMA_ID, ASSETS_PAGE_SIZE

logger = logging.getLogger(__name__)

CMDB_SCHEMA_ID = JIRA_ASSETS_CMDB_SCHEMA_ID

default_args = {
    "owner": "gobierno_ti",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_webhook_failure
}

def sync_cmdb_metadata_task(**kwargs):
    """
    Sincroniza el catálogo de esquemas y los tipos de objetos (Object Types)
    del esquema CMDB de Jira Assets hacia el Data Warehouse.
    """
    client = JiraClient()
    logger.info("Obteniendo Workspace ID de Jira Assets...")
    workspace_id = client.get_assets_workspace_id()
    logger.info(f"Workspace ID detectado: {workspace_id}")

    logger.info("Consultando lista de esquemas de Assets...")
    schemas = client.get_assets_schemas(workspace_id)
    DWRepository.upsert_cmdb_schemas(schemas)
    logger.info(f"Esquemas sincronizados: {len(schemas)}")

    logger.info(f"Consultando tipos de objetos para el esquema CMDB (ID: {CMDB_SCHEMA_ID})...")
    object_types = client.get_assets_object_types(CMDB_SCHEMA_ID, workspace_id)
    DWRepository.upsert_cmdb_object_types(object_types, CMDB_SCHEMA_ID)
    logger.info(f"Tipos de objetos sincronizados para CMDB: {len(object_types)}")

def sync_cmdb_objects_task(**kwargs):
    """
    Extrae paginadamente todos los objetos y atributos del esquema CMDB (ID: 8)
    mediante AQL (Asset Query Language) y los almacena en dim_cmdb_object.
    """
    client = JiraClient()
    workspace_id = client.get_assets_workspace_id()

    logger.info(f"Iniciando extracción masiva de objetos del esquema CMDB (ID: {CMDB_SCHEMA_ID})...")
    total_synced = 0
    batch_num = 0

    for batch in client.search_assets_objects_aql(schema_id=CMDB_SCHEMA_ID, workspace_id=workspace_id, batch_size=100):
        if not batch:
            break
        batch_num += 1
        DWRepository.upsert_cmdb_objects(batch, workspace_id, CMDB_SCHEMA_ID)
        total_synced += len(batch)
        if batch_num % 10 == 0:
            logger.info(f"[Lote {batch_num}] Objetos de CMDB sincronizados acumulados: {total_synced}")

    logger.info(f"Extracción de CMDB finalizada con éxito. Total objetos sincronizados: {total_synced}")

def refresh_datamarts_task(**kwargs):
    """
    Reconstruye las vistas analíticas para reflejar los nuevos nombres y estados de activos resueltos.
    """
    with get_dw_connection() as conn:
        with conn.cursor() as cur:
            logger.info("Actualizando vistas analíticas con referencias de CMDB...")
            cur.execute("""
                SELECT DISTINCT project_key 
                FROM meta_field_homologation 
                WHERE project_key != 'ALL';
            """)
            projects = [row[0] for row in cur.fetchall()]
            for p_key in projects:
                try:
                    cur.execute("CALL sp_rebuild_project_datamart_view(%s);", (p_key,))
                except Exception as e:
                    logger.warning(f"Aviso regenerando vista para {p_key}: {e}")
        conn.commit()
    logger.info("Vistas de Datamart actualizadas exitosamente.")

with DAG(
    dag_id="etl_jira_assets_cmdb",
    default_args=default_args,
    description="Extracción diaria de catálogo completo y objetos de la CMDB (Jira Assets)",
    schedule="0 7 * * *",  # Diario a las 7:00 AM (Lima)
    start_date=datetime(2026, 1, 1, tzinfo=LIMA_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["gobierno_ti", "cmdb", "assets", "diario"]
) as dag:

    t1_metadata = PythonOperator(
        task_id="sync_cmdb_metadata",
        python_callable=sync_cmdb_metadata_task
    )

    t2_objects = PythonOperator(
        task_id="sync_cmdb_objects",
        python_callable=sync_cmdb_objects_task
    )

    t3_refresh_views = PythonOperator(
        task_id="refresh_datamarts",
        python_callable=refresh_datamarts_task
    )

    t1_metadata >> t2_objects >> t3_refresh_views
