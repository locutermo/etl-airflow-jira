"""
Consola de Administración - Jira Data Warehouse
Gobierno TI
"""

import os
import json
import logging
from datetime import datetime
import psycopg2
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Jira Data Warehouse | Gobierno TI",
    layout="wide",
    initial_sidebar_state="expanded"
)

DW_HOST = os.getenv("DW_HOST", "postgres-dw")
DW_PORT = int(os.getenv("DW_PORT", "5432"))
DW_NAME = os.getenv("DW_NAME", os.getenv("DW_DB", "jira_dw"))
DW_USER = os.getenv("DW_USER", "dw_admin")
DW_PASSWORD = os.getenv("DW_PASSWORD", "dw_pass_gobierno_2026")

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin_gobierno_2026")

def get_db_connection():
    return psycopg2.connect(
        host=DW_HOST,
        port=DW_PORT,
        dbname=DW_NAME,
        user=DW_USER,
        password=DW_PASSWORD,
        options="-c timezone=America/Lima"
    )

@st.cache_data(ttl=15, show_spinner=False)
def _run_query_cached(query: str, params: tuple = None) -> pd.DataFrame:
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Error de base de datos: {e}")
        return pd.DataFrame()

def run_query(query: str, params=None, ttl: int = 15) -> pd.DataFrame:
    hashable_params = tuple(params) if isinstance(params, (list, tuple)) else params
    if ttl > 0:
        return _run_query_cached(query, hashable_params)
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=hashable_params)
    except Exception as e:
        st.error(f"Error de base de datos: {e}")
        return pd.DataFrame()

def run_execute(query: str, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    st.cache_data.clear()

def trigger_airflow_dag(dag_id: str, conf: dict = None) -> bool:
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
    payload = {"conf": conf or {}}
    try:
        resp = requests.post(
            url,
            json=payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        st.error(f"Error al conectar con Airflow: {e}")
        return False

# -----------------------------------------------------------------------------
# Barra lateral
# -----------------------------------------------------------------------------
st.sidebar.title("Gobierno TI")
st.sidebar.caption("Jira Data Warehouse")

st.sidebar.markdown("---")
st.sidebar.markdown("**Servicios**")
st.sidebar.markdown("""
* [Airflow](http://localhost:8080)
* [PostgreSQL (Puerto 5433)](localhost:5433)
* [cAdvisor](http://localhost:8085)
""")

tab_daily, tab_dict, tab_model, tab_maint = st.tabs([
    "Panel General",
    "Homologación de Campos",
    "Modelo de Datos (ER)",
    "Mantenimiento"
])

# -----------------------------------------------------------------------------
# PESTAÑA 1: PANEL GENERAL
# -----------------------------------------------------------------------------
with tab_daily:
    st.subheader("Estado de Sincronización")

    df_watermark = run_query("""
        SELECT pipeline_name, last_successful_sync, last_run_at, status, records_processed
        FROM meta_etl_watermarks
        WHERE pipeline_name = 'etl_jira_incremental_sync';
    """)

    df_counts = run_query("""
        SELECT 
            COALESCE((SELECT COUNT(*) FROM dim_jira_project WHERE is_sync_enabled = TRUE), 0) AS total_projects,
            COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'fact_jira_issues_core'), 0)::BIGINT AS total_issues,
            COALESCE((SELECT SUM(total_deleted_issues) FROM meta_project_sync_stats WHERE is_sync_enabled = TRUE), 0)::BIGINT AS total_deleted,
            COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'fact_jira_changelog'), 0)::BIGINT AS total_changelog;
    """)

    col1, col2, col3, col4 = st.columns(4)

    status_str = "Operativo"
    last_sync_str = "Sin registros"
    w_status = "UNKNOWN"
    if not df_watermark.empty:
        w_status = df_watermark.iloc[0]["status"]
        if w_status == "SUCCESS":
            status_str = "Operativo"
        elif w_status == "RUNNING":
            status_str = "En ejecución"
        elif w_status == "RESET":
            status_str = "Listo"
        else:
            status_str = f"Error ({w_status})"

        last_dt = df_watermark.iloc[0]["last_successful_sync"]
        if last_dt is not None:
            dt_ts = pd.to_datetime(last_dt)
            if dt_ts.tzinfo is None:
                dt_ts = dt_ts.tz_localize('America/Lima')
            else:
                dt_ts = dt_ts.tz_convert('America/Lima')
            last_sync_str = dt_ts.strftime("%d/%m/%Y %H:%M")

    rec_proc = int(df_watermark.iloc[0]["records_processed"]) if not df_watermark.empty and pd.notnull(df_watermark.iloc[0]["records_processed"]) else 0
    if w_status == "RUNNING":
        col1.metric("Estado", status_str, f"{rec_proc:,} en lote actual")
    else:
        col1.metric("Estado", status_str, f"Corte: {last_sync_str}")

    tot_proj = df_counts.iloc[0]["total_projects"] if not df_counts.empty else 0
    tot_iss = df_counts.iloc[0]["total_issues"] if not df_counts.empty else 0
    tot_del = df_counts.iloc[0]["total_deleted"] if not df_counts.empty else 0
    tot_chg = df_counts.iloc[0]["total_changelog"] if not df_counts.empty else 0

    col2.metric("Proyectos Activos", f"{tot_proj:,}")
    col3.metric("Tickets en Almacén", f"{tot_iss:,}", f"{tot_del:,} eliminados")
    col4.metric("Historial de Cambios", f"{tot_chg:,} eventos")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # MONITOR DE HILOS DE EXTRACCIÓN CONCURRENTE (TIEMPO REAL)
    # -------------------------------------------------------------------------
    with st.expander("Monitor de Hilos de Extracción en Tiempo Real", expanded=True):
        df_threads = run_query("""
            SELECT 
                worker_id, thread_name, current_project, status,
                total_batches, total_issues, last_issue_key, queue_depth,
                GREATEST(0, EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_heartbeat))::INT) AS sec_ago
            FROM meta_etl_thread_monitor
            ORDER BY worker_id ASC;
        """, ttl=0)

        col_m_head1, col_m_head2 = st.columns([4, 1])
        with col_m_head1:
            st.caption("Estado en vivo de los hilos de Jira Cloud (Productores) y el escritor hacia PostgreSQL (Consumidor).")
        with col_m_head2:
            if st.button("Actualizar Hilos", key="btn_refresh_threads"):
                st.rerun()

        if not df_threads.empty:
            cols_w = st.columns(len(df_threads))
            for i, row in df_threads.iterrows():
                with cols_w[i]:
                    w_id = row["worker_id"]
                    w_status = row["status"]
                    w_proj = row["current_project"] or "-"
                    w_issues = int(row["total_issues"] or 0)
                    w_key = row["last_issue_key"] or "-"
                    sec_ago = int(row["sec_ago"] or 0)
                    q_depth = int(row["queue_depth"] or 0)

                    is_active = sec_ago < 60 and w_status in ("EXTRACTING", "WRITING_TO_DW")
                    
                    if w_id == "DB-Consumer":
                        status_badge = "[ACTIVO]" if is_active else "[EN ESPERA]"
                        st.markdown(f"**{w_id}** `{status_badge}`")
                        st.write(f"Estado: **{w_status}**")
                        st.write(f"Cola Buffer: **{q_depth} / 6 lotes**")
                        st.progress(min(1.0, max(0.0, q_depth / 6.0)))
                        st.caption(f"Persistidos: **{w_issues:,}** tickets (hace {sec_ago}s)")
                    else:
                        status_badge = "[EXTRAYENDO]" if is_active else "[INACTIVO]"
                        st.markdown(f"**{w_id}** `{status_badge}`")
                        st.write(f"Proyecto: **{w_proj}**")
                        st.write(f"Extraídos: **{w_issues:,}**")
                        st.caption(f"Último: `{w_key}` (hace {sec_ago}s)")
        else:
            st.info("Sin telemetría de hilos activa. Los hilos se activarán automáticamente durante la ejecución de la extracción.")

    st.markdown("---")

    # Tabla de Proyectos
    col_t_title, col_t_btn_map, col_t_btn_ref = st.columns([2.8, 1.4, 0.8])
    with col_t_title:
        st.markdown("### Proyectos")
    with col_t_btn_map:
        if st.button("Auto-Mapear Pantallas Masivo", help="Auto-homologa los campos de pantalla de todos los proyectos activos"):
            with st.spinner("Auto-mapeando pantallas de todos los proyectos activos..."):
                try:
                    run_execute("CALL sp_auto_generate_base_homologation();")
                    run_execute("CALL sp_refresh_project_sync_stats();")
                    st.cache_data.clear()
                    st.success("Mapeo de pantallas completado para todos los proyectos activos.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al auto-mapear: {e}")
    with col_t_btn_ref:
        if st.button("Actualizar"):
            st.cache_data.clear()
            st.rerun()

    col_f1, col_f2 = st.columns([2, 2])
    with col_f1:
        filtro_estado = st.radio("Mostrar:", ["Todos", "Activos", "Pausados"], horizontal=True)
    with col_f2:
        search_space = st.text_input("Filtrar por código o nombre:", "").strip().lower()

    df_stats = run_query("""
        SELECT 
            s.project_key AS "Código",
            s.project_name AS "Nombre",
            s.project_type AS "Tipo",
            CASE WHEN s.is_sync_enabled THEN 'Activo' ELSE 'Pausado' END AS "Estado",
            s.total_active_issues AS "Tickets",
            s.total_deleted_issues AS "Eliminados",
            COALESCE(scr.screen_fields, 0) AS "Campos Pantalla",
            s.mapped_custom_fields_count AS "Campos Homologados",
            COALESCE(to_char(s.last_issue_updated_in_jira, 'YYYY-MM-DD HH24:MI'), '-') AS "Última Actividad Jira",
            COALESCE(to_char(s.last_etl_sync_at, 'YYYY-MM-DD HH24:MI'), 'Pendiente') AS "Última Sincronización",
            s.is_sync_enabled
        FROM meta_project_sync_stats s
        LEFT JOIN (
            SELECT project_key, COUNT(DISTINCT field_id) AS screen_fields
            FROM meta_project_field_association
            WHERE is_active = TRUE
            GROUP BY project_key
        ) scr ON scr.project_key = s.project_key
        ORDER BY s.total_active_issues DESC;
    """)

    if not df_stats.empty:
        df_filtered = df_stats.copy()
        if filtro_estado == "Activos":
            df_filtered = df_filtered[df_filtered["is_sync_enabled"] == True]
        elif filtro_estado == "Pausados":
            df_filtered = df_filtered[df_filtered["is_sync_enabled"] == False]

        if search_space:
            df_filtered = df_filtered[
                df_filtered["Código"].str.lower().str.contains(search_space, na=False) |
                df_filtered["Nombre"].str.lower().str.contains(search_space, na=False)
            ]

        st.dataframe(df_filtered.drop(columns=["is_sync_enabled"]), use_container_width=True, hide_index=True)
    else:
        st.info("Sin estadísticas registradas. La tabla se actualiza automáticamente al sincronizar.")

    # Administración de proyectos
    with st.expander("Configuración de sincronización por proyecto", expanded=False):
        st.caption("Los proyectos pausados se omiten en la sincronización programada. Los datos históricos existentes se conservan.")

        tab_m_ind, tab_m_bulk = st.tabs(["Individual", "Por lote"])

        with tab_m_ind:
            df_all_p = run_query("""
                SELECT 
                    p.project_key, p.project_name, p.is_sync_enabled,
                    s.total_active_issues,
                    COALESCE(to_char(s.last_issue_updated_in_jira, 'YYYY-MM-DD HH24:MI'), '-') AS last_update
                FROM dim_jira_project p
                LEFT JOIN meta_project_sync_stats s ON p.project_id = s.project_id
                ORDER BY p.project_key ASC;
            """)

            if not df_all_p.empty:
                col_p1, col_p2 = st.columns([2, 2])
                with col_p1:
                    sel_p_key = st.selectbox(
                        "Proyecto:",
                        df_all_p["project_key"].tolist(),
                        format_func=lambda x: f"{x} - {df_all_p[df_all_p['project_key'] == x]['project_name'].values[0]}"
                    )
                    curr_row = df_all_p[df_all_p["project_key"] == sel_p_key].iloc[0]
                    curr_enabled = bool(curr_row["is_sync_enabled"])
                    curr_issues = curr_row["total_active_issues"] or 0
                    curr_date = curr_row["last_update"] or "Sin registros"

                with col_p2:
                    st.write("**Detalle:**")
                    estado_label = "Activo" if curr_enabled else "Pausado"
                    st.write(f"Estado: **{estado_label}** | Tickets: **{curr_issues:,}**")
                    st.caption(f"Última fecha registrada: `{curr_date}`")

                col_btn_a, col_btn_b = st.columns(2)
                with col_btn_a:
                    if curr_enabled:
                        if st.button(f"Pausar {sel_p_key}", use_container_width=True):
                            run_execute("UPDATE dim_jira_project SET is_sync_enabled = FALSE WHERE project_key = %s;", (sel_p_key,))
                            run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = FALSE WHERE project_key = %s;", (sel_p_key,))
                            st.success(f"{sel_p_key} pausado.")
                            st.rerun()
                    else:
                        if st.button(f"Activar {sel_p_key}", use_container_width=True):
                            run_execute("UPDATE dim_jira_project SET is_sync_enabled = TRUE WHERE project_key = %s;", (sel_p_key,))
                            run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = TRUE WHERE project_key = %s;", (sel_p_key,))
                            st.success(f"{sel_p_key} activado.")
                            st.rerun()

                with col_btn_b:
                    if st.button(f"Sincronizar {sel_p_key} ahora", use_container_width=True):
                        ok = trigger_airflow_dag(
                            "etl_jira_incremental_sync",
                            conf={"project_keys": sel_p_key, "max_records": 0}
                        )
                        if ok:
                            st.success(f"Sincronización iniciada para {sel_p_key}.")
                        else:
                            st.error("Error al iniciar sincronización.")

        with tab_m_bulk:
            df_bulk = run_query("SELECT project_key, project_name, is_sync_enabled FROM dim_jira_project ORDER BY project_key ASC;")
            if not df_bulk.empty:
                bulk_selected = st.multiselect(
                    "Seleccionar proyectos:",
                    df_bulk["project_key"].tolist(),
                    format_func=lambda x: f"{x} - {df_bulk[df_bulk['project_key'] == x]['project_name'].values[0]}"
                )
                col_b1, col_b2 = st.columns(2)
                with col_b1:
                    if st.button("Pausar seleccionados", disabled=not bulk_selected):
                        run_execute("UPDATE dim_jira_project SET is_sync_enabled = FALSE WHERE project_key = ANY(%s);", (bulk_selected,))
                        run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = FALSE WHERE project_key = ANY(%s);", (bulk_selected,))
                        st.success(f"{len(bulk_selected)} proyectos pausados.")
                        st.rerun()
                with col_b2:
                    if st.button("Activar seleccionados", disabled=not bulk_selected):
                        run_execute("UPDATE dim_jira_project SET is_sync_enabled = TRUE WHERE project_key = ANY(%s);", (bulk_selected,))
                        run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = TRUE WHERE project_key = ANY(%s);", (bulk_selected,))
                        st.success(f"{len(bulk_selected)} proyectos activados.")
                        st.rerun()

    st.markdown("---")

    # Inspector de campos por proyecto
    st.markdown("### Campos por Proyecto")
    df_projects = run_query("SELECT project_key, project_name FROM dim_jira_project ORDER BY project_key ASC;")

    if not df_projects.empty:
        col_sel, col_act1, col_act2, col_act3 = st.columns([2.2, 1.2, 1.3, 1.1])
        with col_sel:
            selected_proj = st.selectbox(
                "Proyecto a inspeccionar:",
                df_projects["project_key"].tolist(),
                format_func=lambda x: f"{x} - {df_projects[df_projects['project_key'] == x]['project_name'].values[0]}"
            )
        with col_act1:
            st.write("")
            if st.button(f"Auto-Mapear {selected_proj}", key=f"btn_auto_{selected_proj}", help=f"Genera reglas base únicamente para los campos de pantalla de {selected_proj} (excluyendo mensajes)"):
                with st.spinner(f"Detectando campos de pantalla para {selected_proj}..."):
                    try:
                        run_execute("CALL sp_auto_generate_base_homologation(%s);", (selected_proj,))
                        run_execute("CALL sp_refresh_project_sync_stats(%s);", (selected_proj,))
                        st.cache_data.clear()
                        st.success(f"Campos de pantalla homologados para {selected_proj}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error generando campos base para {selected_proj}: {e}")
        with col_act2:
            st.write("")
            if st.button("Auto-Mapear Todos", key="btn_auto_all_p_screen", help="Genera reglas base para los campos de pantalla de todos los proyectos activos"):
                with st.spinner("Auto-mapeando pantallas de todos los proyectos activos..."):
                    try:
                        run_execute("CALL sp_auto_generate_base_homologation();")
                        run_execute("CALL sp_refresh_project_sync_stats();")
                        st.cache_data.clear()
                        st.success("Mapeo completado para todos los proyectos activos.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
        with col_act3:
            st.write("")
            if st.button(f"Sincronizar Jira", key=f"btn_sync_{selected_proj}", help=f"Dispara extracción incremental desde Jira Cloud para {selected_proj}"):
                ok = trigger_airflow_dag("etl_jira_incremental_sync", conf={"project_keys": selected_proj, "max_records": 0})
                if ok:
                    st.success(f"Sincronización enviada para {selected_proj}.")
                else:
                    st.error("Error al conectar con Airflow.")

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown(f"**Campos mapeados en `{selected_proj}` (Activos en DW)**")
            
            df_tot = run_query(
                "SELECT COALESCE(total_active_issues, 0) FROM meta_project_sync_stats WHERE project_key = %s;",
                (selected_proj,)
            )
            total_proj_issues = int(df_tot.iloc[0, 0]) if not df_tot.empty else 0

            df_mapped = run_query("""
                WITH proj_eav AS (
                    SELECT canonical_name, COUNT(val_text) AS con_valor
                    FROM fact_issue_custom_values
                    WHERE project_key = %s
                    GROUP BY canonical_name
                )
                SELECT 
                    h.field_id AS "ID Campo",
                    c.field_name_jira AS "Nombre Jira",
                    h.canonical_name AS "Columna BD",
                    h.normalization_strategy AS "Tipo",
                    COALESCE(SUM(eav.con_valor), 0)::BIGINT AS "Con Valor"
                FROM meta_field_homologation h
                JOIN meta_jira_field_catalog c ON h.field_id = c.field_id
                LEFT JOIN proj_eav eav 
                    ON eav.canonical_name = h.canonical_name 
                    OR eav.canonical_name LIKE (h.canonical_name || '_%%')
                WHERE h.project_key = %s OR h.project_key = 'ALL'
                GROUP BY h.field_id, c.field_name_jira, h.canonical_name, h.normalization_strategy
                ORDER BY h.canonical_name ASC;
            """, (selected_proj, selected_proj))

            if not df_mapped.empty:
                if total_proj_issues > 0:
                    df_mapped["Cobertura"] = (df_mapped["Con Valor"] * 100.0 / total_proj_issues).round(1).astype(str) + " %"
                else:
                    df_mapped["Cobertura"] = "0.0 %"
                st.dataframe(df_mapped, use_container_width=True, hide_index=True)

                col_m_act1, col_m_act2 = st.columns(2)
                with col_m_act1:
                    if st.button(f"Regularizar Histórico ({len(df_mapped)})", key=f"btn_reg_hist_{selected_proj}", help="Procesa registros históricos almacenados en Staging para los campos mapeados"):
                        with st.spinner(f"Regularizando histórico de {selected_proj}..."):
                            try:
                                run_execute("CALL sp_regularize_project_custom_fields(%s);", (selected_proj,))
                                st.success(f"Histórico de {selected_proj} regularizado exitosamente.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                with col_m_act2:
                    if st.button(f"Limpiar Mapeo", key=f"btn_clean_{selected_proj}", help=f"Elimina las reglas de homologación y valores EAV de {selected_proj}"):
                        run_execute("DELETE FROM meta_field_homologation WHERE project_key = %s;", (selected_proj,))
                        run_execute("DELETE FROM fact_issue_custom_values WHERE project_key = %s;", (selected_proj,))
                        run_execute("CALL sp_rebuild_project_datamart_view(%s);", (selected_proj,))
                        run_execute("CALL sp_refresh_project_sync_stats(%s);", (selected_proj,))
                        st.success(f"Mapeo de {selected_proj} reiniciado.")
                        st.rerun()
            else:
                st.caption(f"No hay campos personalizados mapeados para `{selected_proj}`. Selecciona campos a la derecha o usa el botón superior.")

        with col_right:
            st.markdown(f"**Campos de pantalla disponibles (Sin Homologar)**")

            # 1. Buscar primero en el catálogo de asociación de pantallas de Jira
            df_available = run_query("""
                SELECT 
                    a.field_id,
                    a.field_name_jira,
                    COALESCE(c.schema_type, 'string') AS schema_type
                FROM meta_project_field_association a
                LEFT JOIN meta_jira_field_catalog c ON a.field_id = c.field_id
                WHERE a.project_key = %s 
                  AND a.is_active = TRUE 
                  AND a.is_custom = TRUE
                  AND a.field_id NOT IN (
                      SELECT field_id FROM meta_field_homologation WHERE project_key = %s OR project_key = 'ALL'
                  )
                  AND a.field_name_jira NOT LIKE '%%(Message)%%'
                  AND a.field_name_jira NOT LIKE '%%Message%%'
                ORDER BY a.field_name_jira ASC;
            """, (selected_proj, selected_proj))

            # 2. Si no hay datos de pantalla aún, fallback a muestra en staging
            if df_available.empty:
                df_available = run_query("""
                    WITH sample_raw AS (
                        SELECT raw_payload
                        FROM stg_jira_issues_raw
                        WHERE project_key = %s
                        LIMIT 50
                    ),
                    raw_keys AS (
                        SELECT DISTINCT jsonb_object_keys(raw_payload->'fields') AS field_id
                        FROM sample_raw
                    )
                    SELECT 
                        r.field_id,
                        COALESCE(c.field_name_jira, r.field_id) AS field_name_jira,
                        COALESCE(c.schema_type, 'string') AS schema_type
                    FROM raw_keys r
                    LEFT JOIN meta_jira_field_catalog c ON r.field_id = c.field_id
                    WHERE r.field_id LIKE 'customfield_%%'
                      AND r.field_id NOT IN (
                          SELECT field_id FROM meta_field_homologation WHERE project_key = %s OR project_key = 'ALL'
                      )
                      AND COALESCE(c.field_name_jira, '') NOT LIKE '%%(Message)%%'
                      AND COALESCE(c.field_name_jira, '') NOT LIKE '%%Message%%'
                    ORDER BY field_name_jira ASC;
                """, (selected_proj, selected_proj))

            if not df_available.empty:
                st.caption(f"Se detectaron **{len(df_available)}** campos configurados en Jira. Elige solo los que deseas homologar:")
                
                options_map = {
                    f"{row.field_name_jira} ({row.field_id})": (row.field_id, row.field_name_jira, row.schema_type)
                    for row in df_available.itertuples()
                }

                selected_fields = st.multiselect(
                    "Seleccionar campos para incorporar:",
                    list(options_map.keys()),
                    key=f"multi_fields_{selected_proj}",
                    help="Selecciona únicamente los campos de negocio que necesitas analizar."
                )

                if st.button("Incorporar Campos Seleccionados", key=f"btn_add_fields_{selected_proj}", disabled=not selected_fields):
                    for label in selected_fields:
                        fid, fname, stype = options_map[label]
                        run_execute("""
                            INSERT INTO meta_field_homologation (
                                project_key, request_type_name, field_id, canonical_name,
                                normalization_strategy, expose_in_datamart, track_in_transitions, updated_by
                            )
                            VALUES (
                                %s, 'ALL', %s, fn_clean_canonical_name(%s),
                                fn_infer_normalization_strategy(%s), TRUE, FALSE, 'user_manual'
                            )
                            ON CONFLICT (project_key, request_type_name, field_id) DO NOTHING;
                        """, (selected_proj, fid, fname, stype))
                    
                    run_execute("CALL sp_refresh_project_sync_stats(%s);", (selected_proj,))
                    st.success(f"{len(selected_fields)} campos incorporados a la homologación de {selected_proj}.")
                    st.rerun()

                with st.expander("Ver tabla completa de campos disponibles"):
                    st.dataframe(
                        df_available.rename(columns={"field_id": "ID Campo", "field_name_jira": "Nombre Jira", "schema_type": "Tipo"}),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.caption("Todos los campos de pantalla están mapeados o no hay campos detectados.")

# -----------------------------------------------------------------------------
# PESTAÑA 2: HOMOLOGACIÓN DE CAMPOS
# -----------------------------------------------------------------------------
with tab_dict:
    st.subheader("Homologación de Campos Personalizados")
    st.caption("Administra la asociación de campos de Jira al modelo analítico y la regularización del histórico.")

    # 1. Métricas resumidas en vivo
    df_hom_metrics = run_query("""
        SELECT 
            (SELECT COUNT(*) FROM meta_field_homologation) AS total_rules,
            (SELECT COUNT(DISTINCT project_key) FROM meta_field_homologation WHERE project_key != 'ALL') AS projects_with_rules,
            (SELECT COUNT(*) FROM meta_project_field_association WHERE is_active = TRUE) AS screen_associations,
            (SELECT COUNT(*) FROM meta_jira_field_catalog) AS catalog_fields;
    """)
    if not df_hom_metrics.empty:
        hm1, hm2, hm3, hm4 = st.columns(4)
        hm1.metric("Reglas Homologadas", f"{df_hom_metrics['total_rules'].iloc[0]:,}")
        hm2.metric("Proyectos Configurados", f"{df_hom_metrics['projects_with_rules'].iloc[0]:,}")
        hm3.metric("Campos en Pantallas Jira", f"{df_hom_metrics['screen_associations'].iloc[0]:,}")
        hm4.metric("Catálogo Global Jira", f"{df_hom_metrics['catalog_fields'].iloc[0]:,}")

    st.markdown("---")

    # 2. Acciones Masivas
    st.markdown("#### Acciones Masivas")
    col_act_m1, col_act_m2 = st.columns([1, 1])
    with col_act_m1:
        st.markdown("**Auto-Mapear Pantallas (Todos los Activos)**")
        st.caption("Genera reglas base automáticamente para los campos configurados en pantallas de Jira de todos los proyectos activos.")
        if st.button("Auto-Mapear Todos los Activos", key="btn_bulk_auto_hom"):
            with st.spinner("Ejecutando mapeo masivo de pantallas..."):
                try:
                    run_execute("CALL sp_auto_generate_base_homologation();")
                    run_execute("CALL sp_refresh_project_sync_stats();")
                    st.cache_data.clear()
                    st.success("Reglas base generadas exitosamente para todos los proyectos activos.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al auto-mapear: {e}")
    with col_act_m2:
        st.markdown("**Regularizar Histórico (Todos los Activos)**")
        st.caption("Procesa y regulariza secuencialmente los datos históricos almacenados en Staging para todos los proyectos activos.")
        if st.button("Regularizar Histórico de Todos los Activos", key="btn_bulk_reg_all"):
            with st.spinner("Procesando histórico de todos los proyectos activos..."):
                try:
                    df_act = run_query("SELECT project_key FROM dim_jira_project WHERE is_sync_enabled = TRUE ORDER BY project_key ASC;")
                    for p_key in df_act["project_key"]:
                        run_execute("CALL sp_regularize_project_custom_fields(%s);", (p_key,))
                    st.cache_data.clear()
                    st.success("Histórico regularizado exitosamente para todos los proyectos activos.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al regularizar: {e}")

    st.markdown("---")

    # 3. Sub-pestañas de Homologación
    tab_hom_rules, tab_hom_manual = st.tabs([
        "Reglas Activas por Proyecto",
        "Mapeo Manual Individual (Avanzado)"
    ])

    with tab_hom_rules:
        col_hr_f1, col_hr_f2 = st.columns([2, 2])
        with col_hr_f1:
            all_p_hom = ["Todos los Proyectos"] + (df_projects["project_key"].tolist() if not df_projects.empty else [])
            sel_p_hom = st.selectbox("Filtrar por Proyecto:", all_p_hom, key="sel_filter_p_hom")
        with col_hr_f2:
            search_rule = st.text_input("Buscar campo o columna:", "", key="inp_search_rule").strip().lower()

        query_rules = """
            SELECT 
                h.project_key AS "Proyecto",
                h.field_id AS "ID Campo",
                COALESCE(c.field_name_jira, h.field_id) AS "Nombre Jira",
                h.canonical_name AS "Columna DW",
                h.normalization_strategy AS "Estrategia",
                CASE WHEN h.expose_in_datamart THEN 'SI' ELSE 'NO' END AS "En Datamart",
                h.updated_by AS "Creado Por",
                to_char(h.updated_at, 'YYYY-MM-DD HH24:MI') AS "Actualizado"
            FROM meta_field_homologation h
            LEFT JOIN meta_jira_field_catalog c ON h.field_id = c.field_id
            WHERE 1=1
        """
        params_rules = []
        if sel_p_hom != "Todos los Proyectos":
            query_rules += " AND (h.project_key = %s OR h.project_key = 'ALL')"
            params_rules.append(sel_p_hom)
        query_rules += " ORDER BY h.project_key ASC, h.canonical_name ASC;"

        df_active_rules = run_query(query_rules, tuple(params_rules) if params_rules else None)

        if search_rule and not df_active_rules.empty:
            df_active_rules = df_active_rules[
                df_active_rules["Nombre Jira"].str.lower().str.contains(search_rule, na=False) |
                df_active_rules["Columna DW"].str.lower().str.contains(search_rule, na=False) |
                df_active_rules["ID Campo"].str.lower().str.contains(search_rule, na=False)
            ]

        st.caption(f"Mostrando **{len(df_active_rules):,}** reglas de homologación activas.")
        st.dataframe(df_active_rules, use_container_width=True, hide_index=True)

        if sel_p_hom != "Todos los Proyectos":
            col_hp1, col_hp2, col_hp3 = st.columns(3)
            with col_hp1:
                if st.button(f"Auto-Mapear Pantallas de {sel_p_hom}", key=f"btn_hom_p_{sel_p_hom}"):
                    with st.spinner(f"Auto-mapeando pantallas de {sel_p_hom}..."):
                        run_execute("CALL sp_auto_generate_base_homologation(%s);", (sel_p_hom,))
                        run_execute("CALL sp_refresh_project_sync_stats(%s);", (sel_p_hom,))
                        st.cache_data.clear()
                        st.success(f"Reglas generadas para {sel_p_hom}.")
                        st.rerun()
            with col_hp2:
                if st.button(f"Regularizar Histórico de {sel_p_hom}", key=f"btn_reg_p_{sel_p_hom}"):
                    with st.spinner(f"Regularizando histórico de {sel_p_hom}..."):
                        run_execute("CALL sp_regularize_project_custom_fields(%s);", (sel_p_hom,))
                        st.cache_data.clear()
                        st.success(f"Histórico regularizado para {sel_p_hom}.")
                        st.rerun()
            with col_hp3:
                if st.button(f"Limpiar Reglas de {sel_p_hom}", key=f"btn_clean_rules_{sel_p_hom}"):
                    run_execute("DELETE FROM meta_field_homologation WHERE project_key = %s;", (sel_p_hom,))
                    run_execute("DELETE FROM fact_issue_custom_values WHERE project_key = %s;", (sel_p_hom,))
                    run_execute("CALL sp_rebuild_project_datamart_view(%s);", (sel_p_hom,))
                    run_execute("CALL sp_refresh_project_sync_stats(%s);", (sel_p_hom,))
                    st.cache_data.clear()
                    st.success(f"Reglas de {sel_p_hom} eliminadas.")
                    st.rerun()

    with tab_hom_manual:
        st.markdown("#### Incorporación Manual de Campo (Uno a Uno)")
        st.caption("Utiliza este formulario si necesitas definir una regla personalizada con una estrategia específica o nombre analítico particular.")

        with st.form("form_backfill"):
            col_f1, col_f2 = st.columns(2)

            with col_f1:
                all_projects = ["ALL"] + (df_projects["project_key"].tolist() if not df_projects.empty else [])
                f_proj = st.selectbox("Proyecto:", all_projects, help="'ALL' aplica la regla a todos los proyectos")

                df_catalog = run_query("SELECT field_id, field_name_jira, schema_type FROM meta_jira_field_catalog ORDER BY field_name_jira ASC;")
                cat_options = [f"{row['field_id']} | {row['field_name_jira']} ({row['schema_type']})" for _, row in df_catalog.iterrows()] if not df_catalog.empty else ["customfield_10010 | Ejemplo"]
                f_cat_sel = st.selectbox("Campo de Jira:", cat_options)
                f_field_id = f_cat_sel.split(" | ")[0]

            with col_f2:
                f_canonical = st.text_input("Nombre de columna (sin espacios, ej. centro_costo):", value="").strip().lower()
                f_strategy = st.selectbox("Conversión:", [
                    "STRING",
                    "CASCADE_SPLIT",
                    "USER_DIM",
                    "NUMBER",
                    "DATE"
                ])

            col_c1, col_c2 = st.columns(2)
            with col_c1:
                f_expose = st.checkbox("Incluir en vista SQL (vw_mart_*)", value=True)
            with col_c2:
                f_track = st.checkbox("Calcular tiempos en transiciones", value=False)

            submit_backfill = st.form_submit_button("Guardar y procesar histórico")

        if submit_backfill:
            if not f_canonical:
                st.warning("Indica un nombre de columna válido.")
            else:
                if not f_canonical.startswith("cf_"):
                    f_canonical = f"cf_{f_canonical}"
                try:
                    run_execute("""
                        INSERT INTO meta_field_homologation (
                            project_key, request_type_name, field_id, canonical_name,
                            normalization_strategy, expose_in_datamart, track_in_transitions, updated_by
                        )
                        VALUES (%s, 'ALL', %s, %s, %s, %s, %s, 'admin')
                        ON CONFLICT (project_key, request_type_name, field_id) DO UPDATE SET
                            canonical_name = EXCLUDED.canonical_name,
                            normalization_strategy = EXCLUDED.normalization_strategy,
                            expose_in_datamart = EXCLUDED.expose_in_datamart,
                            track_in_transitions = EXCLUDED.track_in_transitions,
                            updated_at = CURRENT_TIMESTAMP;
                    """, (f_proj, f_field_id, f_canonical, f_strategy, f_expose, f_track))

                    with st.spinner("Procesando histórico..."):
                        run_execute("CALL sp_backfill_custom_field_from_raw(%s, %s, %s, %s);", (f_proj, f_field_id, f_canonical, f_strategy))

                    if f_proj != "ALL":
                        run_execute("CALL sp_rebuild_project_datamart_view(%s);", (f_proj,))

                    st.cache_data.clear()
                    st.success(f"Campo '{f_canonical}' guardado y procesado para '{f_proj}'.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al procesar: {e}")

# -----------------------------------------------------------------------------
# PESTAÑA 3: MODELO DE DATOS (ER)
# -----------------------------------------------------------------------------
with tab_model:
    st.subheader("Modelo de Datos y Arquitectura de Tablas")
    st.caption("Estructura relacional del Data Warehouse analítico (PostgreSQL 16).")

    # Métricas de volumen por tabla en vivo (lectura instantánea desde catálogo de PostgreSQL)
    df_counts = run_query("""
        SELECT 
            COALESCE(MAX(CASE WHEN relname = 'fact_jira_issues_core' THEN n_live_tup END), 0)::BIGINT AS "fact_jira_issues_core",
            COALESCE(MAX(CASE WHEN relname = 'fact_jira_changelog' THEN n_live_tup END), 0)::BIGINT AS "fact_jira_changelog",
            COALESCE(MAX(CASE WHEN relname = 'fact_jsm_requests' THEN n_live_tup END), 0)::BIGINT AS "fact_jsm_requests",
            COALESCE(MAX(CASE WHEN relname = 'fact_issue_custom_values' THEN n_live_tup END), 0)::BIGINT AS "fact_issue_custom_values",
            COALESCE(MAX(CASE WHEN relname = 'fact_issue_transitions' THEN n_live_tup END), 0)::BIGINT AS "fact_issue_transitions",
            COALESCE(MAX(CASE WHEN relname = 'dim_jira_project' THEN n_live_tup END), 0)::BIGINT AS "dim_jira_project",
            COALESCE(MAX(CASE WHEN relname = 'dim_jira_user' THEN n_live_tup END), 0)::BIGINT AS "dim_jira_user",
            COALESCE(MAX(CASE WHEN relname = 'meta_jira_field_catalog' THEN n_live_tup END), 0)::BIGINT AS "meta_jira_field_catalog"
        FROM pg_stat_user_tables
        WHERE relname IN (
            'fact_jira_issues_core', 'fact_jira_changelog', 'fact_jsm_requests',
            'fact_issue_custom_values', 'fact_issue_transitions', 'dim_jira_project',
            'dim_jira_user', 'meta_jira_field_catalog'
        );
    """)

    if not df_counts.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tickets (Core)", f"{df_counts['fact_jira_issues_core'].iloc[0]:,}")
        c2.metric("Historial de Cambios", f"{df_counts['fact_jira_changelog'].iloc[0]:,}")
        c3.metric("Tickets JSM", f"{df_counts['fact_jsm_requests'].iloc[0]:,}")
        c4.metric("Usuarios Dimensionados", f"{df_counts['dim_jira_user'].iloc[0]:,}")

    col_diag_header, col_diag_ctrl = st.columns([3, 1])
    with col_diag_header:
        st.markdown("#### Diagrama Entidad - Relación (ER)")
    with col_diag_ctrl:
        diag_height = st.slider("Altura del visor (px)", min_value=750, max_value=1600, value=1050, step=50)

    raw_mermaid_code = """erDiagram
    dim_jira_project ||--o{ fact_jira_issues_core : tiene
    dim_jira_project ||--|| meta_project_sync_stats : metricas
    dim_jira_user ||--o{ fact_jira_issues_core : asignado
    
    fact_jira_issues_core ||--o| fact_jsm_requests : extension_jsm
    fact_jira_issues_core ||--o{ fact_issue_custom_values : valores_eav
    fact_jira_issues_core ||--o{ fact_jira_changelog : historial
    fact_jira_issues_core ||--o{ fact_issue_transitions : transiciones

    meta_jira_field_catalog ||--o{ meta_field_homologation : catalogo
    meta_jira_field_catalog ||--o{ fact_issue_custom_values : tipo_campo
    meta_jira_field_catalog ||--o{ meta_project_field_association : pantallas
    dim_jira_project ||--o{ meta_project_field_association : configuracion
    
    stg_jira_issues_raw ||--o{ fact_jira_issues_core : transforma

    dim_jira_project {
        bigint project_id PK
        varchar project_key "unique"
        varchar project_name
        varchar project_type
        boolean is_sync_enabled
    }

    dim_jira_user {
        varchar account_id PK
        varchar display_name
        varchar email_address
        boolean is_active
        varchar account_type
    }

    fact_jira_issues_core {
        bigint issue_id PK
        varchar issue_key "unique"
        bigint project_id FK
        varchar summary
        varchar issue_type_name
        varchar status_name
        varchar status_category
        varchar reporter_account_id FK
        varchar assignee_account_id FK
        timestamp created_at
        timestamp updated_at
        timestamp resolved_at
        bigint lead_time_seconds
        boolean is_deleted
    }

    fact_jsm_requests {
        bigint issue_id PK
        varchar request_type_name
        varchar organization_name
        bigint sla_first_response_target_sec
        bigint sla_resolution_target_sec
        boolean sla_resolution_breached
        int csat_rating
    }

    fact_issue_custom_values {
        bigint issue_id PK
        varchar canonical_name "PK_part"
        varchar field_id FK
        text val_text
        numeric val_number
        timestamp val_timestamp
        varchar val_account_id
    }

    fact_jira_changelog {
        varchar changelog_item_key PK
        bigint issue_id FK
        varchar field_name
        text from_string
        text to_string
        timestamp changed_at
    }

    fact_issue_transitions {
        bigint transition_id PK
        bigint issue_id FK
        varchar field_name
        text field_value
        timestamp start_at
        timestamp end_at
        bigint duration_seconds
        boolean is_current_state
    }

    meta_jira_field_catalog {
        varchar field_id PK
        varchar field_name_jira
        varchar schema_type
        boolean is_custom
    }

    meta_project_field_association {
        bigint project_id PK
        varchar field_id PK
        varchar project_key
        varchar field_name_jira
        boolean is_custom
        boolean is_active
    }

    meta_field_homologation {
        int homologation_id PK
        varchar project_key
        varchar field_id FK
        varchar canonical_name
        varchar normalization_strategy
        boolean expose_in_datamart
    }

    stg_jira_issues_raw {
        bigint issue_id PK
        varchar project_key
        varchar issue_key
        timestamp updated_at
        jsonb raw_payload
    }"""

    json_mermaid_code = json.dumps(raw_mermaid_code.strip())

    mermaid_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{
                margin: 0;
                padding: 10px;
                background-color: transparent;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                color: #e0e0e0;
            }}
            .toolbar {{
                display: flex;
                justify-content: flex-end;
                gap: 8px;
                margin-bottom: 8px;
            }}
            .toolbar button {{
                background: #21262d;
                color: #c9d1d9;
                border: 1px solid #30363d;
                border-radius: 6px;
                padding: 6px 12px;
                font-size: 13px;
                cursor: pointer;
            }}
            .toolbar button:hover {{
                background: #30363d;
                color: #ffffff;
            }}
            .mermaid-wrapper {{
                overflow: auto;
                background: #0e1117;
                border: 1px solid #30363d;
                border-radius: 8px;
                padding: 24px;
                text-align: center;
                height: {diag_height - 65}px;
                box-sizing: border-box;
            }}
            #diagram-svg-target {{
                display: inline-block;
                transform-origin: top center;
                transition: transform 0.15s ease-in-out;
            }}
            #diagram-svg-target svg {{
                max-width: none !important;
                height: auto;
            }}
        </style>
        <script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.0/dist/mermaid.min.js"></script>
        <script>
            // Protección contra paths vacíos en motores Chromium (getPointAtLength)
            (function() {{
                function safeGetPointAtLength(origFn) {{
                    return function(val) {{
                        try {{
                            const res = origFn.call(this, val);
                            return res || {{ x: 0, y: 0 }};
                        }} catch (e) {{
                            return {{ x: 0, y: 0 }};
                        }}
                    }};
                }}

                if (window.SVGGeometryElement && SVGGeometryElement.prototype.getPointAtLength) {{
                    SVGGeometryElement.prototype.getPointAtLength = safeGetPointAtLength(SVGGeometryElement.prototype.getPointAtLength);
                }}
                if (window.SVGPathElement && SVGPathElement.prototype.getPointAtLength) {{
                    SVGPathElement.prototype.getPointAtLength = safeGetPointAtLength(SVGPathElement.prototype.getPointAtLength);
                }}
            }})();

            const mermaidCode = {json_mermaid_code};
            let currentZoom = 1.0;
            let renderAttempts = 0;
            let isRendered = false;

            function zoom(delta) {{
                currentZoom = Math.max(0.4, Math.min(2.5, currentZoom + delta));
                const el = document.getElementById('diagram-svg-target');
                if (el) {{
                    el.style.transform = `scale(${{currentZoom}})`;
                }}
            }}

            function resetZoom() {{
                currentZoom = 1.0;
                const el = document.getElementById('diagram-svg-target');
                if (el) {{
                    el.style.transform = 'scale(1)';
                }}
            }}

            async function renderDiagram() {{
                try {{
                    const target = document.getElementById('diagram-svg-target');
                    if (!target) return;
                    
                    // Si el contenedor está en un tab oculto (ancho 0), diferir render
                    if (target.clientWidth < 50 && renderAttempts < 10) {{
                        renderAttempts++;
                        setTimeout(renderDiagram, 200);
                        return;
                    }}

                    mermaid.initialize({{
                        startOnLoad: false,
                        theme: 'dark',
                        themeVariables: {{
                            darkMode: true,
                            background: '#0e1117',
                            primaryColor: '#1f6feb',
                            primaryTextColor: '#f0f6fc',
                            primaryBorderColor: '#388bfd',
                            lineColor: '#58a6ff',
                            secondaryColor: '#238636',
                            tertiaryColor: '#161b22',
                            fontSize: '13px'
                        }}
                    }});

                    const uniqueId = 'svg_er_' + Date.now();
                    const {{ svg }} = await mermaid.render(uniqueId, mermaidCode);
                    target.innerHTML = svg;
                    isRendered = true;
                }} catch (err) {{
                    console.error('Error renderizando diagrama:', err);
                    if (renderAttempts < 4) {{
                        renderAttempts++;
                        setTimeout(renderDiagram, 250);
                    }} else {{
                        const target = document.getElementById('diagram-svg-target');
                        if (target) {{
                            target.innerHTML = '<div style="color:#f85149; text-align:left; padding:20px;"><pre>' + err.message + '</pre></div>';
                        }}
                    }}
                }}
            }}

            // IntersectionObserver para renderizar solo cuando el tab es visible
            function initObserver() {{
                const target = document.getElementById('diagram-svg-target');
                if (!target) return;
                
                const observer = new IntersectionObserver((entries) => {{
                    entries.forEach(entry => {{
                        if (entry.isIntersecting && !isRendered) {{
                            renderDiagram();
                        }}
                    }});
                }}, {{ threshold: 0.05 }});
                
                observer.observe(target);
                
                // Fallback por temporizador
                setTimeout(() => {{
                    if (!isRendered && target.clientWidth > 50) {{
                        renderDiagram();
                    }}
                }}, 300);
            }}

            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', initObserver);
            }} else {{
                initObserver();
            }}
        </script>
    </head>
    <body>
        <div class="toolbar">
            <button onclick="renderDiagram()">Recargar</button>
            <button onclick="zoom(0.15)">Acercar (+)</button>
            <button onclick="zoom(-0.15)">Alejar (-)</button>
            <button onclick="resetZoom()">Restablecer</button>
        </div>
        <div class="mermaid-wrapper">
            <div id="diagram-svg-target">
                <p style="color:#8b949e; margin-top:40px;">Cargando modelo de datos...</p>
            </div>
        </div>
    </body>
    </html>
    """
    components.html(mermaid_html, height=diag_height, scrolling=False)

    st.markdown("---")
    st.markdown("#### Detalle de Capas y Tablas")

    tab_desc = [
        {"Capa": "Staging (Raw)", "Tabla": "stg_jira_issues_raw", "Tipo": "Lake JSONB", "Descripción": "Payloads íntegros crudos devueltos por la API v3. Permite Backfill Zero-API sin tocar Jira."},
        {"Capa": "Dimensional", "Tabla": "dim_jira_project", "Tipo": "Dimensión", "Descripción": "Espacios de trabajo, tipo (software, service desk, business) y estado de sincronización."},
        {"Capa": "Dimensional", "Tabla": "dim_jira_user", "Tipo": "Dimensión", "Descripción": "Usuarios unificados de Jira (asignados, reporteros, creadores, aprobadores)."},
        {"Capa": "Hechos", "Tabla": "fact_jira_issues_core", "Tipo": "Fact Principal", "Descripción": "Atributos universales de cada issue: estados, prioridades, fechas, tipo, lead time y borrado lógico."},
        {"Capa": "Hechos", "Tabla": "fact_jsm_requests", "Tipo": "Extensión 1:1", "Descripción": "Atributos exclusivos de Service Desk: Request Types, organizaciones, métricas de SLA y CSAT."},
        {"Capa": "Hechos", "Tabla": "fact_issue_custom_values", "Tipo": "EAV Tipado", "Descripción": "Almacenamiento columnar tipado (texto, número, fecha) para campos personalizados homologados."},
        {"Capa": "Hechos", "Tabla": "fact_jira_changelog", "Tipo": "Fact Eventos", "Descripción": "Historial de modificaciones por campo (status, assignee, priority, customfields)."},
        {"Capa": "Hechos", "Tabla": "fact_issue_transitions", "Tipo": "Fact Intervalos", "Descripción": "Tiempos de permanencia en segundos por estado o responsable, listos para Power BI."},
        {"Capa": "Metadatos", "Tabla": "meta_jira_field_catalog", "Tipo": "Catálogo", "Descripción": "Inventario de todos los custom fields existentes en la instancia de Atlassian Jira."},
        {"Capa": "Metadatos", "Tabla": "meta_field_homologation", "Tipo": "Configuración", "Descripción": "Reglas de mapeo a nombres canónicos y visibilidad en vistas mart para BI."},
        {"Capa": "Metadatos", "Tabla": "meta_etl_watermarks", "Tipo": "Control ETL", "Descripción": "Punto de control (watermark) del pipeline incremental con timestamps precisos GMT-5."},
        {"Capa": "Metadatos", "Tabla": "meta_project_sync_stats", "Tipo": "Estadísticas", "Descripción": "Conteo precalculado de tickets y estado por espacio para carga instantánea en UI."}
    ]
    st.dataframe(pd.DataFrame(tab_desc), use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# PESTAÑA 4: MANTENIMIENTO
# -----------------------------------------------------------------------------
with tab_maint:
    st.subheader("Mantenimiento")

    col_m1, col_m2 = st.columns(2)

    with col_m1:
        st.markdown("#### Renombrar código de proyecto")
        st.caption("Actualiza el código en todas las tablas si fue modificado en Jira.")
        with st.form("form_migrate_key"):
            old_p_key = st.text_input("Código anterior:", placeholder="OLD").strip().upper()
            new_p_key = st.text_input("Código nuevo:", placeholder="NEW").strip().upper()
            btn_migrate = st.form_submit_button("Actualizar código")

        if btn_migrate:
            if old_p_key and new_p_key:
                try:
                    run_execute("CALL sp_migrate_project_key(%s, %s);", (old_p_key, new_p_key))
                    st.success(f"Código actualizado: {old_p_key} -> {new_p_key}")
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Completa ambos campos.")

    with col_m2:
        st.markdown("#### Conciliación de tickets eliminados")
        st.caption("Consulta Jira para marcar tickets borrados en el almacén.")
        if st.button("Ejecutar conciliación"):
            ok = trigger_airflow_dag("etl_jira_reconciliation_deletes")
            if ok:
                st.success("Proceso de conciliación iniciado en Airflow.")
            else:
                st.error("Error al iniciar proceso.")

    st.markdown("---")
    st.markdown("#### Vistas analíticas (Power BI)")
    st.caption("Regenera las vistas SQL según las reglas de homologación activas.")
    if st.button("Regenerar vistas"):
        with st.spinner("Actualizando vistas..."):
            try:
                df_all_proj = run_query("SELECT project_key FROM dim_jira_project WHERE is_sync_enabled = TRUE;")
                for pk in df_all_proj["project_key"]:
                    run_execute("CALL sp_rebuild_project_datamart_view(%s);", (pk,))
                st.success("Vistas actualizadas correctamente.")
            except Exception as e:
                st.error(f"Error: {e}")
