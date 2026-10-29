"""
Script de regularizacion historica (Backfill Quirurgico) para Jira Service Management.
Extrae customfield_10010 (Request Type) y customfield_10002 (Organizations)
para todos los tickets del proyecto REQ y actualiza fact_jsm_requests y stg_jira_issues_raw.
"""

import sys
import os
import time
import logging
from typing import List, Dict, Any

# Agregar plugins al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'plugins')))

from jira_client import JiraClient
from db import get_dw_connection
from psycopg2.extras import execute_values, Json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("backfill_req_jsm")

def run_jsm_backfill(project_key: str = "REQ"):
    logger.info(f"[INICIO] Iniciando backfill de JSM Request Type y Organizaciones para proyecto: {project_key}")
    
    jira_client = JiraClient()
    conn = get_dw_connection()

    jql = f"project = {project_key} AND cf[10010] is not EMPTY ORDER BY created DESC"
    target_fields = ["customfield_10010", "customfield_10002"]
    
    total_processed = 0
    total_updated_jsm = 0
    batch_num = 0

    t_start = time.time()

    try:
        issue_generator = jira_client.search_issues_incremental(
            jql=jql,
            fields=target_fields,
            expand="",
            batch_size=100
        )

        for batch in issue_generator:
            batch_num += 1
            if not batch:
                continue

            jsm_tuples = []
            raw_updates = []

            for iss in batch:
                issue_id = int(iss["id"])
                issue_key = iss["key"]
                fields = iss.get("fields", {})

                # Request Type
                f10010 = fields.get("customfield_10010") or {}
                rt_data = f10010.get("requestType") if isinstance(f10010, dict) else {}
                if not isinstance(rt_data, dict):
                    rt_data = {}

                rt_id = str(rt_data.get("id") or f10010.get("id") or "")
                rt_name = rt_data.get("name") or f10010.get("name")
                rt_group = rt_data.get("groupName") or f10010.get("groupName")
                sd_id = str(rt_data.get("serviceDeskId") or f10010.get("serviceDeskId") or "")

                # Organization
                f10002 = fields.get("customfield_10002") or []
                org_id = None
                org_name = None
                if isinstance(f10002, list) and len(f10002) > 0 and isinstance(f10002[0], dict):
                    org_id = str(f10002[0].get("id", ""))
                    org_name = f10002[0].get("name")

                jsm_tuples.append((
                    issue_id, project_key, sd_id, rt_id, rt_name, rt_group, org_id, org_name
                ))

                if f10010:
                    raw_updates.append((Json(f10010), issue_id))

            # Upsert en fact_jsm_requests
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TEMP TABLE tmp_jsm_stage (
                        issue_id BIGINT,
                        project_key VARCHAR(30),
                        service_desk_id VARCHAR(50),
                        request_type_id VARCHAR(50),
                        request_type_name VARCHAR(255),
                        request_type_group VARCHAR(150),
                        organization_id VARCHAR(50),
                        organization_name VARCHAR(255)
                    ) ON COMMIT DROP;
                """)

                execute_values(cur, """
                    INSERT INTO tmp_jsm_stage (
                        issue_id, project_key, service_desk_id, request_type_id,
                        request_type_name, request_type_group, organization_id, organization_name
                    ) VALUES %s;
                """, jsm_tuples)

                cur.execute("""
                    INSERT INTO fact_jsm_requests (
                        issue_id, project_key, service_desk_id, request_type_id,
                        request_type_name, request_type_group, organization_id, organization_name
                    )
                    SELECT t.issue_id, t.project_key, t.service_desk_id, t.request_type_id,
                           t.request_type_name, t.request_type_group, t.organization_id, t.organization_name
                    FROM tmp_jsm_stage t
                    JOIN fact_jira_issues_core c ON t.issue_id = c.issue_id
                    ON CONFLICT (issue_id) DO UPDATE SET
                        service_desk_id = COALESCE(EXCLUDED.service_desk_id, fact_jsm_requests.service_desk_id),
                        request_type_id = COALESCE(EXCLUDED.request_type_id, fact_jsm_requests.request_type_id),
                        request_type_name = COALESCE(EXCLUDED.request_type_name, fact_jsm_requests.request_type_name),
                        request_type_group = COALESCE(EXCLUDED.request_type_group, fact_jsm_requests.request_type_group),
                        organization_id = COALESCE(EXCLUDED.organization_id, fact_jsm_requests.organization_id),
                        organization_name = COALESCE(EXCLUDED.organization_name, fact_jsm_requests.organization_name),
                        dw_updated_at = CURRENT_TIMESTAMP;
                """)
                updated_count = cur.rowcount
                total_updated_jsm += updated_count

                # Actualizar stg_jira_issues_raw
                if raw_updates:
                    execute_values(cur, """
                        UPDATE stg_jira_issues_raw AS s
                        SET raw_payload = jsonb_set(
                            s.raw_payload,
                            '{fields,customfield_10010}',
                            v.payload::jsonb,
                            true
                        )
                        FROM (VALUES %s) AS v(payload, issue_id)
                        WHERE s.issue_id = v.issue_id;
                    """, raw_updates)

            conn.commit()
            total_processed += len(batch)
            logger.info(f"[PROGRESO] Lote {batch_num:03d}: {len(batch)} procesados | {total_processed} acumulados en {time.time()-t_start:.1f}s")

        logger.info(f"[OK] Regularizacion JSM completada: {total_processed} issues procesadas, {total_updated_jsm} registros actualizados en fact_jsm_requests en {time.time()-t_start:.1f}s.")

    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Error durante regularizacion JSM: {e}", exc_info=True)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run_jsm_backfill("REQ")
