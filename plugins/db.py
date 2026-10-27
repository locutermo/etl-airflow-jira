"""
Módulo de Conexión y Operaciones Batch en PostgreSQL Data Warehouse.
Utiliza psycopg2 con execute_values y execute_batch para rendimiento óptimo en cargas masivas.
"""

import os
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)

def get_dw_connection():
    """Obtiene una conexión a postgres-dw utilizando DW_CONN_URI o variables de entorno individuales con timezone America/Lima."""
    tz_options = "-c timezone=America/Lima"
    conn_uri = os.getenv("DW_CONN_URI")
    if conn_uri:
        return psycopg2.connect(conn_uri, options=tz_options)
    
    return psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres-dw"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.getenv("DW_DB", "jira_dw"),
        user=os.getenv("DW_USER", "dw_admin"),
        password=os.getenv("DW_PASSWORD", "dw_pass_gobierno_2026"),
        options=tz_options
    )

@contextmanager
def dw_cursor():
    """Context manager para manejar transacciones y cursores de PostgreSQL de forma segura."""
    conn = get_dw_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

class DWRepository:
    """Repositorio de operaciones analíticas en PostgreSQL Data Warehouse."""

    @staticmethod
    def get_watermark(pipeline_name: str) -> Optional[Dict[str, Any]]:
        with dw_cursor() as cur:
            cur.execute("""
                SELECT pipeline_name, last_successful_sync, last_run_at, status
                FROM meta_etl_watermarks
                WHERE pipeline_name = %s;
            """, (pipeline_name,))
            row = cur.fetchone()
            if row:
                return {
                    "pipeline_name": row[0],
                    "last_successful_sync": row[1],
                    "last_run_at": row[2],
                    "status": row[3]
                }
            return None

    @staticmethod
    def update_watermark(pipeline_name: str, sync_dt, records_processed: int, status: str = "SUCCESS", error_msg: Optional[str] = None):
        with dw_cursor() as cur:
            cur.execute("""
                INSERT INTO meta_etl_watermarks (pipeline_name, last_successful_sync, last_run_at, records_processed, status, error_message)
                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s)
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    last_successful_sync = CASE WHEN EXCLUDED.status = 'SUCCESS' THEN EXCLUDED.last_successful_sync ELSE meta_etl_watermarks.last_successful_sync END,
                    last_run_at = CURRENT_TIMESTAMP,
                    records_processed = EXCLUDED.records_processed,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message;
            """, (pipeline_name, sync_dt, records_processed, status, error_msg))

    @staticmethod
    def upsert_projects(projects: List[Dict[str, Any]]):
        """Inserta o actualiza proyectos en dim_jira_project y detecta cambios de project_key."""
        if not projects:
            return

        with dw_cursor() as cur:
            for p in projects:
                p_id = int(p["id"])
                p_key = p["key"]
                p_name = p.get("name", p_key)
                p_type = p.get("projectTypeKey", "software")
                p_cat = p.get("projectCategory", {}).get("name") if isinstance(p.get("projectCategory"), dict) else None
                lead_id = p.get("lead", {}).get("accountId") if isinstance(p.get("lead"), dict) else None

                # Verificar si ya existía con otra key
                cur.execute("SELECT project_key FROM dim_jira_project WHERE project_id = %s;", (p_id,))
                existing = cur.fetchone()

                if existing and existing[0] != p_key:
                    old_key = existing[0]
                    logger.info(f"Detectado cambio de project_key para project_id {p_id}: {old_key} -> {p_key}. Disparando sp_migrate_project_key.")
                    cur.execute("CALL sp_migrate_project_key(%s, %s);", (old_key, p_key))

                cur.execute("""
                    INSERT INTO dim_jira_project (project_id, project_key, project_name, project_type, project_category, lead_account_id, last_synced_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (project_id) DO UPDATE SET
                        project_name = EXCLUDED.project_name,
                        project_type = EXCLUDED.project_type,
                        project_category = EXCLUDED.project_category,
                        lead_account_id = EXCLUDED.lead_account_id,
                        last_synced_at = CURRENT_TIMESTAMP;
                """, (p_id, p_key, p_name, p_type, p_cat, lead_id))

    @staticmethod
    def upsert_fields(fields: List[Dict[str, Any]]):
        if not fields:
            return
        field_dict = {f["id"]: f for f in fields if "id" in f}
        tuples = []
        for f in field_dict.values():
            schema = f.get("schema", {})
            tuples.append((
                f["id"],
                f.get("name", f["id"]),
                schema.get("type"),
                schema.get("custom"),
                f.get("custom", True)
            ))
        with dw_cursor() as cur:
            execute_values(cur, """
                INSERT INTO meta_jira_field_catalog (field_id, field_name_jira, schema_type, schema_custom, is_custom)
                VALUES %s
                ON CONFLICT (field_id) DO UPDATE SET
                    field_name_jira = EXCLUDED.field_name_jira,
                    schema_type = EXCLUDED.schema_type,
                    schema_custom = EXCLUDED.schema_custom;
            """, tuples)

    @staticmethod
    def get_homologation_rules() -> List[Dict[str, Any]]:
        with dw_cursor() as cur:
            cur.execute("""
                SELECT homologation_id, project_key, request_type_name, field_id, canonical_name, normalization_strategy, expose_in_datamart, track_in_transitions
                FROM meta_field_homologation;
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def upsert_raw_staging(issues: List[Dict[str, Any]]):
        """Guarda los lotes JSON íntegros en stg_jira_issues_raw para Zero-API Backfill."""
        if not issues:
            return
        
        # Deduplicar por issue_id dentro del mismo lote para evitar CardinalityViolation
        issue_dict = {int(issue["id"]): issue for issue in issues if "id" in issue}
        tuples = []
        for i_id, issue in issue_dict.items():
            i_key = issue["key"]
            fields = issue.get("fields", {})
            proj = fields.get("project", {})
            p_id = int(proj.get("id", 0))
            p_key = proj.get("key", i_key.split("-")[0])
            upd_str = fields.get("updated")
            tuples.append((i_id, p_id, p_key, i_key, upd_str, Json(issue)))

        with dw_cursor() as cur:
            execute_values(cur, """
                INSERT INTO stg_jira_issues_raw (issue_id, project_id, project_key, issue_key, updated_at, raw_payload)
                VALUES %s
                ON CONFLICT (issue_id) DO UPDATE SET
                    project_id = EXCLUDED.project_id,
                    project_key = EXCLUDED.project_key,
                    issue_key = EXCLUDED.issue_key,
                    updated_at = EXCLUDED.updated_at,
                    raw_payload = EXCLUDED.raw_payload,
                    ingested_at = CURRENT_TIMESTAMP;
            """, tuples)

    @staticmethod
    def upsert_transformed_data(transformed_batch: List[Dict[str, Any]]):
        """Inserta en bloque las capas dimensionales y de hechos garantizando unicidad estricta para evitar CardinalityViolation."""
        all_users = []
        all_core = []
        all_jsm = []
        all_custom = []
        all_changelog = []

        for item in transformed_batch:
            all_users.extend(item.get("users", []))
            if item.get("core"):
                all_core.append(item["core"])
            if item.get("jsm"):
                all_jsm.append(item["jsm"])
            all_custom.extend(item.get("custom_values", []))
            all_changelog.extend(item.get("changelog", []))

        with dw_cursor() as cur:
            # 1. Dim Users (Deduplicar por account_id)
            if all_users:
                user_dict = {u["account_id"]: u for u in all_users if u.get("account_id")}
                u_tuples = [(u["account_id"], u["display_name"], u["email_address"], u["is_active"], u["account_type"]) for u in user_dict.values()]
                execute_values(cur, """
                    INSERT INTO dim_jira_user (account_id, display_name, email_address, is_active, account_type)
                    VALUES %s
                    ON CONFLICT (account_id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        email_address = COALESCE(EXCLUDED.email_address, dim_jira_user.email_address),
                        is_active = EXCLUDED.is_active,
                        last_synced_at = CURRENT_TIMESTAMP;
                """, u_tuples)

            # 2. Fact Core (Deduplicar por issue_id)
            if all_core:
                core_dict = {c["issue_id"]: c for c in all_core if c.get("issue_id")}
                core_tuples = [
                    (
                        c["issue_id"], c["issue_key"], c["project_id"], c["project_key"], c["summary"], c["description_plain"],
                        c["issue_type_id"], c["issue_type_name"], c["is_subtask"], c["hierarchy_level"],
                        c["parent_issue_id"], c["parent_issue_key"], c["epic_issue_id"], c["epic_issue_key"], c["epic_summary"],
                        c["status_id"], c["status_name"], c["status_category"], c["priority_name"], c["resolution_name"],
                        c["reporter_account_id"], c["assignee_account_id"], c["creator_account_id"],
                        c["sprint_name"], c["story_points"], c["labels_csv"], c["components_csv"],
                        c["created_at"], c["updated_at"], c["resolved_at"], c["due_date"], c["lead_time_seconds"], c["is_deleted"]
                    )
                    for c in core_dict.values()
                ]
                execute_values(cur, """
                    INSERT INTO fact_jira_issues_core (
                        issue_id, issue_key, project_id, project_key, summary, description_plain,
                        issue_type_id, issue_type_name, is_subtask, hierarchy_level,
                        parent_issue_id, parent_issue_key, epic_issue_id, epic_issue_key, epic_summary,
                        status_id, status_name, status_category, priority_name, resolution_name,
                        reporter_account_id, assignee_account_id, creator_account_id,
                        sprint_name, story_points, labels_csv, components_csv,
                        created_at, updated_at, resolved_at, due_date, lead_time_seconds, is_deleted
                    )
                    VALUES %s
                    ON CONFLICT (issue_id) DO UPDATE SET
                        issue_key = EXCLUDED.issue_key,
                        project_id = EXCLUDED.project_id,
                        project_key = EXCLUDED.project_key,
                        summary = EXCLUDED.summary,
                        description_plain = EXCLUDED.description_plain,
                        issue_type_id = EXCLUDED.issue_type_id,
                        issue_type_name = EXCLUDED.issue_type_name,
                        is_subtask = EXCLUDED.is_subtask,
                        hierarchy_level = EXCLUDED.hierarchy_level,
                        parent_issue_id = EXCLUDED.parent_issue_id,
                        parent_issue_key = EXCLUDED.parent_issue_key,
                        epic_issue_id = EXCLUDED.epic_issue_id,
                        epic_issue_key = EXCLUDED.epic_issue_key,
                        epic_summary = EXCLUDED.epic_summary,
                        status_id = EXCLUDED.status_id,
                        status_name = EXCLUDED.status_name,
                        status_category = EXCLUDED.status_category,
                        priority_name = EXCLUDED.priority_name,
                        resolution_name = EXCLUDED.resolution_name,
                        reporter_account_id = EXCLUDED.reporter_account_id,
                        assignee_account_id = EXCLUDED.assignee_account_id,
                        creator_account_id = EXCLUDED.creator_account_id,
                        labels_csv = EXCLUDED.labels_csv,
                        components_csv = EXCLUDED.components_csv,
                        updated_at = EXCLUDED.updated_at,
                        resolved_at = EXCLUDED.resolved_at,
                        due_date = EXCLUDED.due_date,
                        lead_time_seconds = EXCLUDED.lead_time_seconds,
                        is_deleted = FALSE,
                        dw_updated_at = CURRENT_TIMESTAMP;
                """, core_tuples)

            # 3. Fact JSM (Deduplicar por issue_id)
            if all_jsm:
                jsm_dict = {j["issue_id"]: j for j in all_jsm if j.get("issue_id")}
                jsm_tuples = [
                    (
                        j["issue_id"], j["project_key"], j["service_desk_id"], j["request_type_id"],
                        j["request_type_name"], j["request_type_group"], j["organization_id"], j["organization_name"],
                        j["sla_first_response_target_sec"], j["sla_first_response_elapsed_sec"], j["sla_first_response_breached"],
                        j["sla_resolution_target_sec"], j["sla_resolution_elapsed_sec"], j["sla_resolution_breached"],
                        j["csat_rating"], j["csat_comment"]
                    )
                    for j in jsm_dict.values()
                ]
                execute_values(cur, """
                    INSERT INTO fact_jsm_requests (
                        issue_id, project_key, service_desk_id, request_type_id,
                        request_type_name, request_type_group, organization_id, organization_name,
                        sla_first_response_target_sec, sla_first_response_elapsed_sec, sla_first_response_breached,
                        sla_resolution_target_sec, sla_resolution_elapsed_sec, sla_resolution_breached,
                        csat_rating, csat_comment
                    )
                    VALUES %s
                    ON CONFLICT (issue_id) DO UPDATE SET
                        project_key = EXCLUDED.project_key,
                        service_desk_id = EXCLUDED.service_desk_id,
                        request_type_id = EXCLUDED.request_type_id,
                        request_type_name = EXCLUDED.request_type_name,
                        request_type_group = EXCLUDED.request_type_group,
                        sla_first_response_target_sec = EXCLUDED.sla_first_response_target_sec,
                        sla_first_response_elapsed_sec = EXCLUDED.sla_first_response_elapsed_sec,
                        sla_first_response_breached = EXCLUDED.sla_first_response_breached,
                        sla_resolution_target_sec = EXCLUDED.sla_resolution_target_sec,
                        sla_resolution_elapsed_sec = EXCLUDED.sla_resolution_elapsed_sec,
                        sla_resolution_breached = EXCLUDED.sla_resolution_breached,
                        dw_updated_at = CURRENT_TIMESTAMP;
                """, jsm_tuples)

            # 4. Custom Values EAV (Deduplicar por (issue_id, canonical_name))
            if all_custom:
                custom_dict = {(cv["issue_id"], cv["canonical_name"]): cv for cv in all_custom if cv.get("issue_id") and cv.get("canonical_name")}
                cv_tuples = [
                    (
                        cv["issue_id"], cv["project_key"], cv["field_id"], cv["canonical_name"],
                        cv["val_text"], cv["val_number"], cv["val_timestamp"], cv["val_account_id"]
                    )
                    for cv in custom_dict.values()
                ]
                execute_values(cur, """
                    INSERT INTO fact_issue_custom_values (
                        issue_id, project_key, field_id, canonical_name,
                        val_text, val_number, val_timestamp, val_account_id
                    )
                    VALUES %s
                    ON CONFLICT (issue_id, canonical_name) DO UPDATE SET
                        val_text = EXCLUDED.val_text,
                        val_number = EXCLUDED.val_number,
                        val_timestamp = EXCLUDED.val_timestamp,
                        val_account_id = EXCLUDED.val_account_id;
                """, cv_tuples)

            # 5. Changelog (Deduplicar por changelog_item_key)
            if all_changelog:
                ch_dict = {ch["changelog_item_key"]: ch for ch in all_changelog if ch.get("changelog_item_key")}
                ch_tuples = [
                    (
                        ch["changelog_item_key"], ch["changelog_id"], ch["issue_id"], ch["project_key"],
                        ch["author_account_id"], ch["field_name"], ch["field_id"],
                        ch["from_string"], ch["from_value_id"], ch["to_string"], ch["to_value_id"],
                        ch["changed_at"]
                    )
                    for ch in ch_dict.values()
                ]
                execute_values(cur, """
                    INSERT INTO fact_jira_changelog (
                        changelog_item_key, changelog_id, issue_id, project_key,
                        author_account_id, field_name, field_id,
                        from_string, from_value_id, to_string, to_value_id, changed_at
                    )
                    VALUES %s
                    ON CONFLICT (changelog_item_key) DO UPDATE SET
                        from_string = EXCLUDED.from_string,
                        to_string = EXCLUDED.to_string,
                        changed_at = EXCLUDED.changed_at;
                """, ch_tuples)

    @staticmethod
    def call_sp_calculate_changelog_intervals(since_dt):
        with dw_cursor() as cur:
            logger.info(f"Ejecutando CALL sp_calculate_changelog_intervals('{since_dt}')")
            cur.execute("CALL sp_calculate_changelog_intervals(%s);", (since_dt,))

    @staticmethod
    def refresh_project_sync_stats():
        """Refresca la tabla meta_project_sync_stats agregando estadísticas por espacio."""
        with dw_cursor() as cur:
            cur.execute("""
                INSERT INTO meta_project_sync_stats (
                    project_id, project_key, project_name, project_type, is_sync_enabled,
                    total_active_issues, total_deleted_issues, mapped_custom_fields_count,
                    last_issue_updated_in_jira, last_etl_sync_at, last_sync_status
                )
                SELECT 
                    p.project_id,
                    p.project_key,
                    p.project_name,
                    p.project_type,
                    p.is_sync_enabled,
                    COALESCE(SUM(CASE WHEN c.is_deleted = FALSE THEN 1 ELSE 0 END), 0)::INT AS total_active,
                    COALESCE(SUM(CASE WHEN c.is_deleted = TRUE THEN 1 ELSE 0 END), 0)::INT AS total_deleted,
                    (SELECT COUNT(DISTINCT field_id) FROM meta_field_homologation h WHERE h.project_key = p.project_key OR h.project_key = 'ALL')::INT,
                    MAX(c.updated_at),
                    CURRENT_TIMESTAMP,
                    'SUCCESS'
                FROM dim_jira_project p
                LEFT JOIN fact_jira_issues_core c ON p.project_id = c.project_id
                GROUP BY p.project_id, p.project_key, p.project_name, p.project_type, p.is_sync_enabled
                ON CONFLICT (project_id) DO UPDATE SET
                    project_key = EXCLUDED.project_key,
                    project_name = EXCLUDED.project_name,
                    project_type = EXCLUDED.project_type,
                    is_sync_enabled = EXCLUDED.is_sync_enabled,
                    total_active_issues = EXCLUDED.total_active_issues,
                    total_deleted_issues = EXCLUDED.total_deleted_issues,
                    mapped_custom_fields_count = EXCLUDED.mapped_custom_fields_count,
                    last_issue_updated_in_jira = EXCLUDED.last_issue_updated_in_jira,
                    last_etl_sync_at = CURRENT_TIMESTAMP,
                    last_sync_status = 'SUCCESS';
            """)
            logger.info("Estadísticas en meta_project_sync_stats refrescadas exitosamente.")

