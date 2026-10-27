"""
DAG de Mantenimiento Administrativo y Soporte L1/L2.
Permite ejecutar bajo demanda tareas de regularización histórica (Zero-API Backfill),
migración de prefijos de proyectos, reconstrucción de vistas para Power BI y recálculo de tiempos.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import pytz

from db import get_dw_connection, DWRepository
from alerts import notify_webhook_failure

logger = logging.getLogger(__name__)

LIMA_TZ = pytz.timezone("America/Lima")

default_args = {
    'owner': 'gobierno-ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': notify_webhook_failure,
}

def execute_maintenance_action(**context):
    params = context.get('params', {})
    action = params.get('action')
    logger.info(f"Iniciando acción de mantenimiento: {action} con parámetros {params}")

    conn = get_dw_connection()
    try:
        with conn.cursor() as cur:
            if action == 'backfill_custom_field':
                proj_key = params.get('project_key', 'ALL')
                field_id = params.get('field_id')
                canonical = params.get('canonical_name')
                strategy = params.get('normalization_strategy', 'STRING')
                
                if not field_id or not canonical:
                    raise ValueError("Se requieren 'field_id' y 'canonical_name' para backfill_custom_field.")

                logger.info(f"Ejecutando backfill para campo {field_id} ({canonical}) con estrategia {strategy} en {proj_key}...")
                cur.execute("CALL sp_backfill_custom_field_from_raw(%s, %s, %s, %s);", (proj_key, field_id, canonical, strategy))
                conn.commit()

                # Reconstruir vista para Power BI si aplica a un proyecto específico
                if proj_key != 'ALL':
                    logger.info(f"Reconstruyendo vista de Power BI para {proj_key}...")
                    cur.execute("CALL sp_rebuild_project_datamart_view(%s);", (proj_key,))
                    conn.commit()

            elif action == 'migrate_project_key':
                old_key = params.get('old_project_key')
                new_key = params.get('new_project_key')
                if not old_key or not new_key:
                    raise ValueError("Se requieren 'old_project_key' y 'new_project_key'.")

                logger.info(f"Migrando prefijo de proyecto: {old_key} -> {new_key}...")
                cur.execute("CALL sp_migrate_project_key(%s, %s);", (old_key, new_key))
                conn.commit()

            elif action == 'rebuild_datamart_views':
                proj_key = params.get('project_key', 'ALL')
                if proj_key == 'ALL':
                    cur.execute("SELECT project_key FROM dim_jira_project WHERE is_sync_enabled = TRUE;")
                    projects = [row[0] for row in cur.fetchall()]
                else:
                    projects = [proj_key]

                for pk in projects:
                    logger.info(f"Reconstruyendo vista Power BI para {pk}...")
                    cur.execute("CALL sp_rebuild_project_datamart_view(%s);", (pk,))
                conn.commit()

            elif action == 'recalculate_intervals':
                since_str = params.get('since_date', '2020-01-01 00:00:00')
                logger.info(f"Recalculando intervalos de changelog desde {since_str}...")
                cur.execute("CALL sp_calculate_changelog_intervals(%s::timestamptz);", (since_str,))
                conn.commit()

            elif action == 'refresh_stats':
                logger.info("Refrescando estadísticas completas de proyectos...")
                DWRepository.refresh_project_sync_stats()

            else:
                raise ValueError(f"Acción '{action}' no reconocida.")

        logger.info(f"Acción de mantenimiento '{action}' finalizada con éxito.")
    finally:
        conn.close()

with DAG(
    'admin_backfill_and_maintenance',
    default_args=default_args,
    description='Mantenimiento y regularización histórica (Zero-API Backfill, migración de prefijos y vistas)',
    schedule_interval=None, # Ejecución manual / API
    start_date=datetime(2026, 1, 1, tzinfo=LIMA_TZ),
    catchup=False,
    max_active_runs=1,
    params={
        'action': Param(
            'backfill_custom_field',
            type='string',
            enum=['backfill_custom_field', 'migrate_project_key', 'rebuild_datamart_views', 'recalculate_intervals', 'refresh_stats'],
            description='Acción operativa a ejecutar'
        ),
        'project_key': Param('ALL', type='string', description='Prefijo del proyecto Jira o ALL'),
        'field_id': Param('customfield_10010', type='string', description='ID del campo en Jira (ej. customfield_10010)'),
        'canonical_name': Param('area_solicitante', type='string', description='Nombre estandarizado sin espacios'),
        'normalization_strategy': Param('STRING', type='string', enum=['STRING', 'NUMBER', 'DATE', 'CASCADE_SPLIT', 'USER_DIM']),
        'old_project_key': Param('', type='string', description='Prefijo antiguo (solo para migrate_project_key)'),
        'new_project_key': Param('', type='string', description='Nuevo prefijo (solo para migrate_project_key)'),
        'since_date': Param('2020-01-01 00:00:00', type='string', description='Fecha inicial para recálculo de transiciones')
    },
    tags=['admin', 'maintenance', 'backfill', 'gobierno-ti'],
) as dag:

    run_action = PythonOperator(
        task_id='execute_maintenance_action',
        python_callable=execute_maintenance_action,
    )

