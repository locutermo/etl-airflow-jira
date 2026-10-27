"""
Script de Pruebas Unitarias e Integración para el Pipeline ETL Jira -> PostgreSQL DW.
Valida conexión a Jira, inserción en staging, transformaciones, carga dimensional y SPs.
"""

import sys
import os
import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("test_etl")

# Asegurar import de plugins
sys.path.insert(0, "/opt/airflow/plugins")
sys.path.insert(0, os.path.abspath("plugins"))

from jira_client import JiraClient
from jira_transformer import JiraTransformer
from db import DWRepository, get_dw_connection, dw_cursor

def run_tests():
    logger.info("=== 1. TEST CONEXIÓN POSTGRESQL DW ===")
    try:
        conn = get_dw_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            v = cur.fetchone()[0]
            logger.info(f"PostgreSQL DW conectado exitosamente: {v}")
        conn.close()
    except Exception as e:
        logger.error(f"Fallo conectando a postgres-dw: {e}")
        return False

    logger.info("=== 2. TEST JIRA API (SEARCH ISSUES) ===")
    try:
        client = JiraClient()
        issues_batch = None
        # Probar extrayendo solo 2 issues de cualquier proyecto
        for batch in client.search_issues_incremental(jql="ORDER BY updated DESC", batch_size=2):
            issues_batch = batch
            break

        if not issues_batch:
            logger.warning("No se obtuvieron issues en la prueba de Jira.")
            return False
        logger.info(f"Se extrajeron {len(issues_batch)} issues de prueba desde Jira.")
    except Exception as e:
        logger.error(f"Fallo en consulta a Jira Cloud API: {e}")
        return False

    logger.info("=== 3. TEST UPSERT RAW STAGING (stg_jira_issues_raw) ===")
    try:
        DWRepository.upsert_raw_staging(issues_batch)
        logger.info("TEST EXITOSO: Inserción en stg_jira_issues_raw completada sin errores de columnas.")
    except Exception as e:
        logger.error(f"Fallo en upsert_raw_staging: {e}")
        return False

    logger.info("=== 4. TEST TRANSFORMACIÓN CON JIRATRANSFORMER ===")
    try:
        rules = DWRepository.get_homologation_rules()
        transformer = JiraTransformer(homologation_rules=rules)
        transformed_list = []
        for issue in issues_batch:
            t = transformer.transform_issue(issue)
            transformed_list.append(t)
            logger.info(f"Issue {t['core']['issue_key']} transformado: Tipo={t['core']['issue_type_name']}, Status={t['core']['status_name']}")
        logger.info("TEST EXITOSO: Transformación de objetos completada.")
    except Exception as e:
        logger.error(f"Fallo en JiraTransformer: {e}")
        return False

    logger.info("=== 5. TEST CARGA DIMENSIONAL Y FACT (DWRepository.upsert_transformed_data) ===")
    try:
        DWRepository.upsert_transformed_data(transformed_list)
        logger.info("TEST EXITOSO: Carga dimensional y de hechos completada en PostgreSQL DW.")
    except Exception as e:
        logger.error(f"Fallo en upsert_transformed_data: {e}")
        return False

    logger.info("=== 6. TEST STORED PROCEDURE: sp_calculate_changelog_intervals ===")
    try:
        DWRepository.call_sp_calculate_changelog_intervals(datetime(2020, 1, 1, tzinfo=pytz.UTC))
        logger.info("TEST EXITOSO: Procedimiento sp_calculate_changelog_intervals ejecutado correctamente.")
    except Exception as e:
        logger.error(f"Fallo en sp_calculate_changelog_intervals: {e}")
        return False

    logger.info("=== 7. TEST REFRESH STATS: refresh_project_sync_stats ===")
    try:
        DWRepository.refresh_project_sync_stats()
        logger.info("TEST EXITOSO: Estadísticas agregadas refrescadas en meta_project_sync_stats.")
    except Exception as e:
        logger.error(f"Fallo en refresh_project_sync_stats: {e}")
        return False

    logger.info("🎉 ¡TODAS LAS PRUEBAS UNITARIAS Y DE INTEGRACIÓN PASARON AL 100%! 🎉")
    return True

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

