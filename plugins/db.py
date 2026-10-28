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
        try:
            if conn and not conn.closed:
                conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if conn and not conn.closed:
                conn.close()
        except Exception:
            pass

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
    def get_active_projects() -> List[Dict[str, Any]]:
        """Retorna la lista de proyectos habilitados para sincronización en dim_jira_project."""
        with dw_cursor() as cur:
            cur.execute("""
                SELECT project_id, project_key, project_name 
                FROM dim_jira_project 
                WHERE is_sync_enabled = TRUE 
                ORDER BY project_key ASC;
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def sync_project_fields_differential(project_id: int, project_key: str, jira_fields: Dict[str, Dict[str, Any]]):
        """
        Sincroniza diferencialmente la asociación Proyecto-Campos en meta_project_field_association.
        - Nuevos campos en Jira -> INSERT con is_active = TRUE
        - Reactivados (estaban is_active = FALSE) -> UPDATE is_active = TRUE
        - Removidos de Jira -> UPDATE is_active = FALSE
        - Actualiza last_verified_at = CURRENT_TIMESTAMP
        """
        if not jira_fields:
            return

        with dw_cursor() as cur:
            # 1. Asegurar que los campos existan en meta_jira_field_catalog para evitar violaciones de FK
            catalog_tuples = [
                (fid, fmeta["name"], "custom" if fmeta["is_custom"] else "standard", None, fmeta["is_custom"])
                for fid, fmeta in jira_fields.items()
            ]
            execute_values(cur, """
                INSERT INTO meta_jira_field_catalog (field_id, field_name_jira, schema_type, schema_custom, is_custom)
                VALUES %s
                ON CONFLICT (field_id) DO UPDATE SET
                    field_name_jira = EXCLUDED.field_name_jira;
            """, catalog_tuples)

            # 2. Leer campos actuales en BD para este proyecto
            cur.execute("""
                SELECT field_id, is_active 
                FROM meta_project_field_association 
                WHERE project_id = %s;
            """, (project_id,))
            db_fields = dict(cur.fetchall())

            # 3. Determinar altas, reactivaciones y bajas
            jira_field_ids = set(jira_fields.keys())
            db_field_ids = set(db_fields.keys())

            to_insert = []
            to_reactivate = []
            to_deactivate = list(db_field_ids - jira_field_ids)

            for fid, fmeta in jira_fields.items():
                if fid not in db_fields:
                    to_insert.append((
                        project_id, project_key, fid, fmeta["name"], fmeta["is_custom"], True
                    ))
                elif not db_fields[fid]:
                    to_reactivate.append(fid)

            # 4. Aplicar cambios
            if to_insert:
                execute_values(cur, """
                    INSERT INTO meta_project_field_association (
                        project_id, project_key, field_id, field_name_jira, is_custom, is_active
                    )
                    VALUES %s
                    ON CONFLICT (project_id, field_id) DO UPDATE SET
                        field_name_jira = EXCLUDED.field_name_jira,
                        is_custom = EXCLUDED.is_custom,
                        is_active = TRUE,
                        last_verified_at = CURRENT_TIMESTAMP;
                """, to_insert)

            if to_reactivate:
                cur.execute("""
                    UPDATE meta_project_field_association
                    SET is_active = TRUE, last_verified_at = CURRENT_TIMESTAMP
                    WHERE project_id = %s AND field_id = ANY(%s);
                """, (project_id, to_reactivate))

            if to_deactivate:
                cur.execute("""
                    UPDATE meta_project_field_association
                    SET is_active = FALSE, last_verified_at = CURRENT_TIMESTAMP
                    WHERE project_id = %s AND field_id = ANY(%s);
                """, (project_id, to_deactivate))

            # Actualizar last_verified_at para los que se mantuvieron activos
            cur.execute("""
                UPDATE meta_project_field_association
                SET last_verified_at = CURRENT_TIMESTAMP
                WHERE project_id = %s AND is_active = TRUE;
            """, (project_id,))

        logger.info(
            f"Proyecto {project_key}: Sincronización diferencial de campos completada. "
            f"Nuevos: {len(to_insert)}, Reactivados: {len(to_reactivate)}, Desactivados: {len(to_deactivate)}."
        )

    @staticmethod
    def get_fields_for_project_extraction(project_key: str, universal_fields: List[str]) -> List[str]:
        """
        Retorna la unión deduplicada de universal_fields + los custom fields activos
        asociados a las pantallas del proyecto en meta_project_field_association.
        """
        with dw_cursor() as cur:
            cur.execute("""
                SELECT field_id 
                FROM meta_project_field_association 
                WHERE project_key = %s AND is_active = TRUE;
            """, (project_key,))
            custom_ids = [row[0] for row in cur.fetchall()]

        combined = list(dict.fromkeys(universal_fields + custom_ids))
        return combined

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
    def refresh_project_sync_stats(project_key: str = None):
        """Refresca la tabla meta_project_sync_stats llamando al stored procedure en PostgreSQL."""
        with dw_cursor() as cur:
            cur.execute("CALL sp_refresh_project_sync_stats(%s);", (project_key,))
            logger.info(f"Estadísticas en meta_project_sync_stats refrescadas exitosamente (proyecto: {project_key or 'ALL'}).")

    @staticmethod
    def update_thread_status(
        worker_id: str,
        thread_name: str,
        current_project: Optional[str],
        status: str,
        total_batches: int = 0,
        total_issues: int = 0,
        last_issue_key: Optional[str] = None,
        queue_depth: int = 0
    ):
        """Actualiza el estado y latido en tiempo real de un worker en meta_etl_thread_monitor."""
        try:
            with dw_cursor() as cur:
                cur.execute("""
                    INSERT INTO meta_etl_thread_monitor (
                        worker_id, thread_name, current_project, status,
                        total_batches, total_issues, last_issue_key, queue_depth,
                        last_heartbeat, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        thread_name = EXCLUDED.thread_name,
                        current_project = EXCLUDED.current_project,
                        status = EXCLUDED.status,
                        total_batches = EXCLUDED.total_batches,
                        total_issues = EXCLUDED.total_issues,
                        last_issue_key = COALESCE(EXCLUDED.last_issue_key, meta_etl_thread_monitor.last_issue_key),
                        queue_depth = EXCLUDED.queue_depth,
                        last_heartbeat = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    worker_id, thread_name, current_project, status,
                    total_batches, total_issues, last_issue_key, queue_depth
                ))
        except Exception as e:
            logger.debug(f"Error actualizando thread monitor para {worker_id}: {e}")

    @staticmethod
    def reset_thread_monitor(active_workers: int = 3):
        """Inicializa los workers en IDLE o STANDBY según la cantidad requerida por el volumen a extraer."""
        try:
            with dw_cursor() as cur:
                cur.execute("""
                    INSERT INTO meta_etl_thread_monitor (
                        worker_id, thread_name, status, total_batches, total_issues, last_issue_key, queue_depth, last_heartbeat
                    )
                    VALUES ('DB-Consumer', 'DW-Consumer-Thread', 'IDLE', 0, 0, NULL, 0, CURRENT_TIMESTAMP)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        status = 'IDLE', current_project = NULL, queue_depth = 0, last_heartbeat = CURRENT_TIMESTAMP;
                """)
                for w in range(1, 4):
                    w_id = f"Worker-{w}"
                    w_status = 'IDLE' if w <= active_workers else 'STANDBY'
                    cur.execute("""
                        INSERT INTO meta_etl_thread_monitor (
                            worker_id, thread_name, status, total_batches, total_issues, last_issue_key, queue_depth, last_heartbeat
                        )
                        VALUES (%s, %s, %s, 0, 0, NULL, 0, CURRENT_TIMESTAMP)
                        ON CONFLICT (worker_id) DO UPDATE SET
                            status = %s, current_project = NULL, queue_depth = 0, last_heartbeat = CURRENT_TIMESTAMP;
                    """, (w_id, f"Jira-Fetch_{w-1}", w_status, w_status))
        except Exception:
            pass

    @staticmethod
    def get_thread_monitor_status() -> List[Dict[str, Any]]:
        """Obtiene el estado de todos los hilos para el dashboard en tiempo real."""
        with dw_cursor() as cur:
            cur.execute("""
                SELECT 
                    worker_id, thread_name, current_project, status,
                    total_batches, total_issues, last_issue_key, queue_depth,
                    last_heartbeat,
                    GREATEST(0, EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_heartbeat))::INT) AS seconds_since_heartbeat
                FROM meta_etl_thread_monitor
                ORDER BY worker_id ASC;
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


