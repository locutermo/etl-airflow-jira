"""
Portal UI Operativo & Daily Tracker - Gobierno TI (Integratel Perú)
Desarrollado en Streamlit para seguimiento en reuniones diarias (Dailys) y gestión Low-Code de Soporte L1/L2.
"""

import os
import json
import logging
from datetime import datetime
import psycopg2
import pandas as pd
import requests
import streamlit as st

# Configuración de página Streamlit
st.set_page_config(
    page_title="Gobierno TI | Jira Data Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Parámetros de conexión desde variables de entorno
DW_HOST = os.getenv("DW_HOST", "postgres-dw")
DW_PORT = int(os.getenv("DW_PORT", "5432"))
DW_NAME = os.getenv("DW_NAME", os.getenv("DW_DB", "jira_dw"))
DW_USER = os.getenv("DW_USER", "dw_admin")
DW_PASSWORD = os.getenv("DW_PASSWORD", "dw_pass_gobierno_2026")

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin_gobierno_2026")

def get_db_connection():
    """Conexión hacia el Data Warehouse analítico con timezone configurado en America/Lima."""
    return psycopg2.connect(
        host=DW_HOST,
        port=DW_PORT,
        dbname=DW_NAME,
        user=DW_USER,
        password=DW_PASSWORD,
        options="-c timezone=America/Lima"
    )

def run_query(query: str, params=None) -> pd.DataFrame:
    """Ejecuta una consulta SQL y retorna un DataFrame de pandas."""
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Error consultando base de datos: {e}")
        return pd.DataFrame()

def run_execute(query: str, params=None):
    """Ejecuta una instrucción DDL/DML o llamada a Stored Procedure."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()

def trigger_airflow_dag(dag_id: str, conf: dict = None) -> bool:
    """Dispara un DAG en Airflow mediante su API REST nativa."""
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
        st.error(f"Error comunicando con la API de Airflow: {e}")
        return False

# -----------------------------------------------------------------------------
# BARRA LATERAL (ESTADO GLOBAL Y FILTROS)
# -----------------------------------------------------------------------------
st.sidebar.image("https://integratelperu.atlassian.net/favicon.ico", width=36)
st.sidebar.title("Gobierno TI")
st.sidebar.caption("Plataforma Analítica Jira Cloud v2.0")

st.sidebar.markdown("---")
st.sidebar.subheader("Servicios del Ecosistema")
st.sidebar.markdown("""
* **Airflow Orquestador:** [Puerto 8080](http://localhost:8080)
* **PostgreSQL DW:** `localhost:5433`
* **cAdvisor Recursos:** [Puerto 8085](http://localhost:8085)
""")

# Pestañas principales
tab_daily, tab_dict, tab_maint = st.tabs([
    "📈 Tablero de Control Daily (Home)",
    "📚 Diccionario & Zero-API Backfill",
    "🛠️ Mantenimiento & Auditoría"
])

# -----------------------------------------------------------------------------
# PESTAÑA 1: TABLERO DE CONTROL DAILY & INVENTARIO DE ESPACIOS
# -----------------------------------------------------------------------------
with tab_daily:
    st.subheader("Resumen Ejecutivo de Sincronización (Daily Standup)")

    # 1. KPIs Superiores
    df_watermark = run_query("""
        SELECT pipeline_name, last_successful_sync, last_run_at, status, records_processed
        FROM meta_etl_watermarks
        WHERE pipeline_name = 'etl_jira_incremental_sync';
    """)

    df_counts = run_query("""
        SELECT 
            (SELECT COUNT(*) FROM dim_jira_project WHERE is_sync_enabled = TRUE) AS total_projects,
            (SELECT COUNT(*) FROM fact_jira_issues_core WHERE is_deleted = FALSE) AS total_issues,
            (SELECT COUNT(*) FROM fact_jira_issues_core WHERE is_deleted = TRUE) AS total_deleted,
            (SELECT COUNT(*) FROM fact_jira_changelog) AS total_changelog;
    """)

    col1, col2, col3, col4 = st.columns(4)

    status_str = "🟢 SALUDABLE"
    last_sync_str = "Sin datos"
    if not df_watermark.empty:
        w_status = df_watermark.iloc[0]["status"]
        if w_status == "SUCCESS":
            status_str = "🟢 SALUDABLE"
        elif w_status == "RUNNING":
            status_str = "🟡 EN EJECUCIÓN"
        elif w_status == "RESET":
            status_str = "⚪ REINICIADO (LISTO)"
        else:
            status_str = f"🔴 ERROR ({w_status})"
        last_dt = df_watermark.iloc[0]["last_successful_sync"]
        if last_dt is not None:
            dt_ts = pd.to_datetime(last_dt)
            if dt_ts.tzinfo is None:
                dt_ts = dt_ts.tz_localize('America/Lima')
            else:
                dt_ts = dt_ts.tz_convert('America/Lima')
            last_sync_str = dt_ts.strftime("%d/%m/%Y %H:%M (GMT-5)")

    rec_proc = int(df_watermark.iloc[0]["records_processed"]) if not df_watermark.empty and pd.notnull(df_watermark.iloc[0]["records_processed"]) else 0
    if w_status == "RUNNING":
        col1.metric("Estado del Pipeline", status_str, f"🔄 Lote actual: {rec_proc:,}")
    else:
        col1.metric("Estado del Pipeline", status_str, f"Último Sync: {last_sync_str}")

    tot_proj = df_counts.iloc[0]["total_projects"] if not df_counts.empty else 0
    tot_iss = df_counts.iloc[0]["total_issues"] if not df_counts.empty else 0
    tot_del = df_counts.iloc[0]["total_deleted"] if not df_counts.empty else 0
    tot_chg = df_counts.iloc[0]["total_changelog"] if not df_counts.empty else 0

    col2.metric("Espacios de Trabajo", f"{tot_proj:,}", "Proyectos activos")
    col3.metric("Tickets en Data Warehouse", f"{tot_iss:,}", f"{tot_del:,} eliminados detectados")
    col4.metric("Historial de Cambios", f"{tot_chg:,}", "Filas de changelog")

    st.markdown("---")

    # 2. Tabla Interactiva de Espacios de Trabajo
    col_t_title, col_t_btn = st.columns([4, 1])
    with col_t_title:
        st.markdown("### 🏢 Inventario y Estado por Espacio de Trabajo")
    with col_t_btn:
        st.write("")
        if st.button("🔄 Refrescar Espacios"):
            st.rerun()

    # Filtros de visualización
    col_f1, col_f2 = st.columns([2, 2])
    with col_f1:
        filtro_estado = st.radio("Filtrar por estado:", ["Todos", "✅ Solo Activos", "⏸️ Solo Pausados"], horizontal=True)
    with col_f2:
        search_space = st.text_input("🔍 Buscar espacio (prefijo o nombre):", "").strip().lower()

    df_stats = run_query("""
        SELECT 
            project_key AS "Prefijo (Key)",
            project_name AS "Nombre del Espacio",
            project_type AS "Tipo",
            CASE WHEN is_sync_enabled THEN '✅ Activo' ELSE '⏸️ Pausado' END AS "Estado Sync",
            total_active_issues AS "Tickets Activos",
            total_deleted_issues AS "Eliminados",
            mapped_custom_fields_count AS "Campos Homologados",
            to_char(last_issue_updated_in_jira, 'YYYY-MM-DD HH24:MI') AS "Última Actividad Jira",
            to_char(last_etl_sync_at, 'YYYY-MM-DD HH24:MI') AS "Último ETL DW",
            is_sync_enabled
        FROM meta_project_sync_stats
        ORDER BY total_active_issues DESC;
    """)

    if not df_stats.empty:
        # Aplicar filtros
        df_filtered = df_stats.copy()
        if filtro_estado == "✅ Solo Activos":
            df_filtered = df_filtered[df_filtered["is_sync_enabled"] == True]
        elif filtro_estado == "⏸️ Solo Pausados":
            df_filtered = df_filtered[df_filtered["is_sync_enabled"] == False]

        if search_space:
            df_filtered = df_filtered[
                df_filtered["Prefijo (Key)"].str.lower().str.contains(search_space, na=False) |
                df_filtered["Nombre del Espacio"].str.lower().str.contains(search_space, na=False)
            ]

        st.dataframe(df_filtered.drop(columns=["is_sync_enabled"]), use_container_width=True, hide_index=True)
    else:
        st.info("No se han sincronizado estadísticas aún. Ejecuta el DAG de sincronización inicial en Airflow.")

    # 3. Panel de Gestión: Activar / Pausar Sincronización de Espacios
    with st.expander("⚙️ Administrar Activación / Pausa de Espacios de Trabajo", expanded=False):
        st.markdown("""
        **Reglas del Motor:**
        * Al **pausar** un espacio, el pipeline incremental general lo ignora por completo. **Sus datos históricos se mantienen intactos** para consultas y Power BI.
        * Al **reactivar** un espacio, puedes reanudar la extracción inmediatamente **desde la última fecha registrada en el DW**, garantizando cero pérdida de datos.
        """)

        tab_m_ind, tab_m_bulk = st.tabs(["🎯 Gestión Individual", "📦 Gestión Masiva"])

        with tab_m_ind:
            df_all_p = run_query("""
                SELECT 
                    p.project_key, p.project_name, p.is_sync_enabled,
                    s.total_active_issues,
                    to_char(s.last_issue_updated_in_jira, 'YYYY-MM-DD HH24:MI') AS last_update
                FROM dim_jira_project p
                LEFT JOIN meta_project_sync_stats s ON p.project_id = s.project_id
                ORDER BY p.project_key ASC;
            """)

            if not df_all_p.empty:
                col_p1, col_p2 = st.columns([2, 2])
                with col_p1:
                    sel_p_key = st.selectbox(
                        "Selecciona el Espacio a configurar:",
                        df_all_p["project_key"].tolist(),
                        format_func=lambda x: f"{x} - {df_all_p[df_all_p['project_key'] == x]['project_name'].values[0]}"
                    )
                    curr_row = df_all_p[df_all_p["project_key"] == sel_p_key].iloc[0]
                    curr_enabled = bool(curr_row["is_sync_enabled"])
                    curr_issues = curr_row["total_active_issues"] or 0
                    curr_date = curr_row["last_update"] or "Sin fecha previa"

                with col_p2:
                    st.write("**Estado Actual:**")
                    if curr_enabled:
                        st.success(f"🟢 **ACTIVO** en pipeline incremental ({curr_issues:,} tickets en DW)")
                    else:
                        st.warning(f"⏸️ **PAUSADO** (Ignorado por ETL, {curr_issues:,} tickets históricos preservados)")
                    st.caption(f"Última fecha registrada en DW: `{curr_date}`")

                col_btn_a, col_btn_b = st.columns(2)
                with col_btn_a:
                    if curr_enabled:
                        if st.button(f"⏸️ Pausar Espacio '{sel_p_key}'", use_container_width=True):
                            run_execute("UPDATE dim_jira_project SET is_sync_enabled = FALSE WHERE project_key = %s;", (sel_p_key,))
                            run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = FALSE WHERE project_key = %s;", (sel_p_key,))
                            st.success(f"Espacio '{sel_p_key}' pausado. Ya no se extraerán tickets en los ciclos automáticos.")
                            st.rerun()
                    else:
                        if st.button(f"✅ Habilitar Espacio '{sel_p_key}'", use_container_width=True):
                            run_execute("UPDATE dim_jira_project SET is_sync_enabled = TRUE WHERE project_key = %s;", (sel_p_key,))
                            run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = TRUE WHERE project_key = %s;", (sel_p_key,))
                            st.success(f"Espacio '{sel_p_key}' activado para próximas sincronizaciones.")
                            st.rerun()

                with col_btn_b:
                    if not curr_enabled or curr_date != "Sin fecha previa":
                        if st.button(f"🚀 Sincronizar '{sel_p_key}' desde última fecha ({curr_date})", use_container_width=True):
                            ok = trigger_airflow_dag(
                                "etl_jira_incremental_sync",
                                conf={"project_keys": sel_p_key, "max_records": 0}
                            )
                            if ok:
                                st.success(f"Sincronización disparada en Airflow para '{sel_p_key}' desde `{curr_date}`.")
                            else:
                                st.error("Error al contactar Airflow API.")

        with tab_m_bulk:
            st.markdown("##### Pausar o Reactivar múltiples espacios a la vez")
            df_bulk = run_query("SELECT project_key, project_name, is_sync_enabled FROM dim_jira_project ORDER BY project_key ASC;")
            if not df_bulk.empty:
                bulk_selected = st.multiselect(
                    "Selecciona los espacios:",
                    df_bulk["project_key"].tolist(),
                    format_func=lambda x: f"{x} - {df_bulk[df_bulk['project_key'] == x]['project_name'].values[0]}"
                )
                col_b1, col_b2 = st.columns(2)
                with col_b1:
                    if st.button("⏸️ Pausar Seleccionados", disabled=not bulk_selected):
                        run_execute("UPDATE dim_jira_project SET is_sync_enabled = FALSE WHERE project_key = ANY(%s);", (bulk_selected,))
                        run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = FALSE WHERE project_key = ANY(%s);", (bulk_selected,))
                        st.success(f"{len(bulk_selected)} espacios han sido pausados.")
                        st.rerun()
                with col_b2:
                    if st.button("✅ Reactivar Seleccionados", disabled=not bulk_selected):
                        run_execute("UPDATE dim_jira_project SET is_sync_enabled = TRUE WHERE project_key = ANY(%s);", (bulk_selected,))
                        run_execute("UPDATE meta_project_sync_stats SET is_sync_enabled = TRUE WHERE project_key = ANY(%s);", (bulk_selected,))
                        st.success(f"{len(bulk_selected)} espacios han sido reactivados.")
                        st.rerun()

    st.markdown("---")

    # 4. Inspector Drill-Down de Campos por Espacio
    st.markdown("### 🔍 Inspector Detallado de Campos por Espacio")
    df_projects = run_query("SELECT project_key, project_name FROM dim_jira_project ORDER BY project_key ASC;")

    if not df_projects.empty:
        col_sel, col_act = st.columns([3, 1])
        with col_sel:
            selected_proj = st.selectbox(
                "Selecciona un Espacio de Trabajo para auditar:",
                df_projects["project_key"].tolist(),
                format_func=lambda x: f"{x} - {df_projects[df_projects['project_key'] == x]['project_name'].values[0]}"
            )
        with col_act:
            st.write("") # espaciador
            if st.button(f"🚀 Extraer {selected_proj} Ahora"):
                ok = trigger_airflow_dag("etl_jira_incremental_sync", conf={"project_keys": selected_proj, "max_records": 0})
                if ok:
                    st.success(f"Extracción iniciada en Airflow para '{selected_proj}'.")
                else:
                    st.error("Error contactando API de Airflow.")

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown(f"#### Campos Homologados en `{selected_proj}`")
            
            # Obtener total de issues del proyecto para el cálculo de cobertura
            df_tot = run_query(
                "SELECT COUNT(*) FROM fact_jira_issues_core WHERE project_key = %s AND is_deleted = FALSE;",
                (selected_proj,)
            )
            total_proj_issues = int(df_tot.iloc[0, 0]) if not df_tot.empty else 0

            df_mapped = run_query("""
                SELECT 
                    h.field_id AS "ID Campo",
                    c.field_name_jira AS "Nombre en Jira",
                    'cf_' || h.canonical_name AS "Columna en BD",
                    h.normalization_strategy AS "Estrategia",
                    COUNT(eav.val_text) AS "Tickets con Valor"
                FROM meta_field_homologation h
                JOIN meta_jira_field_catalog c ON h.field_id = c.field_id
                LEFT JOIN fact_issue_custom_values eav 
                    ON (eav.canonical_name = h.canonical_name OR eav.canonical_name LIKE concat(h.canonical_name, '_%%'))
                   AND eav.project_key = %s
                WHERE h.project_key = %s OR h.project_key = 'ALL'
                GROUP BY h.field_id, c.field_name_jira, h.canonical_name, h.normalization_strategy;
            """, (selected_proj, selected_proj))

            if not df_mapped.empty:
                if total_proj_issues > 0:
                    df_mapped["% Cobertura"] = (df_mapped["Tickets con Valor"] * 100.0 / total_proj_issues).round(1).astype(str) + " %"
                else:
                    df_mapped["% Cobertura"] = "0.0 %"
                st.dataframe(df_mapped, use_container_width=True, hide_index=True)
            else:
                st.caption(f"No hay campos personalizados homologados específicamente para `{selected_proj}`.")

        with col_right:
            st.markdown(f"#### Campos detectados en JSON crudo pero NO homologados")
            st.caption("Campos presentes en el Staging crudo que podrían agregarse a reportes.")

            df_unmapped = run_query("""
                WITH raw_keys AS (
                    SELECT DISTINCT jsonb_object_keys(raw_payload->'fields') AS field_id
                    FROM stg_jira_issues_raw
                    WHERE project_key = %s
                    LIMIT 200
                )
                SELECT 
                    r.field_id AS "ID Campo",
                    COALESCE(c.field_name_jira, r.field_id) AS "Nombre Detectado en Catálogo",
                    COALESCE(c.schema_type, 'Desconocido') AS "Tipo"
                FROM raw_keys r
                LEFT JOIN meta_jira_field_catalog c ON r.field_id = c.field_id
                WHERE r.field_id LIKE 'customfield_%%'
                  AND r.field_id NOT IN (
                      SELECT field_id FROM meta_field_homologation WHERE project_key = %s OR project_key = 'ALL'
                  )
                ORDER BY r.field_id ASC;
            """, (selected_proj, selected_proj))

            if not df_unmapped.empty:
                st.dataframe(df_unmapped, use_container_width=True, hide_index=True)
            else:
                st.success("¡Todos los campos personalizados con datos en este proyecto ya están homologados!")

# -----------------------------------------------------------------------------
# PESTAÑA 2: DICCIONARIO & ZERO-API BACKFILL
# -----------------------------------------------------------------------------
with tab_dict:
    st.subheader("Diccionario de Homologación Semántica y Regularización Histórica")
    st.markdown("""
    Esta herramienta permite agregar un nuevo campo al Data Warehouse para cualquier proyecto.
    Gracias al diseño **ELT Zero-API Backfill**, el campo se puebla retroactivamente para los **500k tickets en segundos**
    directamente desde el JSON crudo en base de datos, sin consumir cuota de peticiones en Jira Cloud.
    """)

    with st.form("form_backfill"):
        col_f1, col_f2 = st.columns(2)

        with col_f1:
            all_projects = ["ALL"] + (df_projects["project_key"].tolist() if not df_projects.empty else [])
            f_proj = st.selectbox("Espacio de Trabajo:", all_projects, help="'ALL' aplica la regla a todos los proyectos")

            df_catalog = run_query("SELECT field_id, field_name_jira, schema_type FROM meta_jira_field_catalog ORDER BY field_name_jira ASC;")
            cat_options = [f"{row['field_id']} | {row['field_name_jira']} ({row['schema_type']})" for _, row in df_catalog.iterrows()] if not df_catalog.empty else ["customfield_10010 | Ejemplo"]
            f_cat_sel = st.selectbox("Campo de Jira a Extraer:", cat_options)
            f_field_id = f_cat_sel.split(" | ")[0]

        with col_f2:
            f_canonical = st.text_input("Nombre Canónico (sin espacios ni acentos, ej. area_solicitante):", value="").strip().lower()
            f_strategy = st.selectbox("Estrategia de Normalización:", [
                "STRING",
                "CASCADE_SPLIT",
                "USER_DIM",
                "NUMBER",
                "DATE"
            ], help="CASCADE_SPLIT crea columnas _parent y _child. USER_DIM extrae a dim_jira_user.")

        col_c1, col_c2 = st.columns(2)
        with col_c1:
            f_expose = st.checkbox("Exponer en Vista de Power BI (vw_mart_*)", value=True)
        with col_c2:
            f_track = st.checkbox("Calcular Tiempos de Permanencia en Transiciones", value=False)

        submit_backfill = st.form_submit_button("🚀 Guardar y Regularizar Históricos Ahora (Zero-API)")

    if submit_backfill:
        if not f_canonical:
            st.warning("Debes indicar un nombre canónico válido (ej. centro_costo).")
        else:
            try:
                # 1. Guardar en meta_field_homologation
                run_execute("""
                    INSERT INTO meta_field_homologation (
                        project_key, request_type_name, field_id, canonical_name,
                        normalization_strategy, expose_in_datamart, track_in_transitions, updated_by
                    )
                    VALUES (%s, 'ALL', %s, %s, %s, %s, %s, 'ui_admin')
                    ON CONFLICT (project_key, request_type_name, field_id) DO UPDATE SET
                        canonical_name = EXCLUDED.canonical_name,
                        normalization_strategy = EXCLUDED.normalization_strategy,
                        expose_in_datamart = EXCLUDED.expose_in_datamart,
                        track_in_transitions = EXCLUDED.track_in_transitions,
                        updated_at = CURRENT_TIMESTAMP;
                """, (f_proj, f_field_id, f_canonical, f_strategy, f_expose, f_track))

                # 2. Ejecutar Stored Procedure de Backfill
                with st.spinner("Poblando valores históricos desde el Staging JSON crudo..."):
                    run_execute("CALL sp_backfill_custom_field_from_raw(%s, %s, %s, %s);", (f_proj, f_field_id, f_canonical, f_strategy))

                # 3. Reconstruir vista para Power BI si aplica a un proyecto específico
                if f_proj != "ALL":
                    run_execute("CALL sp_rebuild_project_datamart_view(%s);", (f_proj,))

                st.success(f"✅ Campo '{f_canonical}' regularizado exitosamente para el espacio '{f_proj}'. ¡Ya disponible para Power BI!")
                st.balloons()
            except Exception as e:
                st.error(f"Error durante el backfill: {e}")

# -----------------------------------------------------------------------------
# PESTAÑA 3: MANTENIMIENTO & AUDITORÍA
# -----------------------------------------------------------------------------
with tab_maint:
    st.subheader("Mantenimiento de Integridad Referencial y Auditoría")

    col_m1, col_m2 = st.columns(2)

    with col_m1:
        st.markdown("#### 🔄 Migración de Prefijo de Proyecto (Key Rename)")
        st.caption("Actualiza en cascada todas las tablas cuando en Jira cambian el prefijo (ej. de OLD a NEW).")
        with st.form("form_migrate_key"):
            old_p_key = st.text_input("Prefijo Antiguo (en BD):", placeholder="OLD").strip().upper()
            new_p_key = st.text_input("Nuevo Prefijo (en Jira):", placeholder="NEW").strip().upper()
            btn_migrate = st.form_submit_button("Ejecutar Migración de Prefijo")

        if btn_migrate:
            if old_p_key and new_p_key:
                try:
                    run_execute("CALL sp_migrate_project_key(%s, %s);", (old_p_key, new_p_key))
                    st.success(f"Migración completada: {old_p_key} -> {new_p_key}")
                except Exception as e:
                    st.error(f"Fallo en migración: {e}")
            else:
                st.warning("Ingresa ambos prefijos.")

    with col_m2:
        st.markdown("#### 🗑️ Auditoría de Eliminados (Jira Expressions API)")
        st.caption("Ejecuta una conciliación ultraligera para detectar tickets borrados físicamente en Atlassian.")
        if st.button("🚀 Disparar Barrido de Eliminados Ahora (Airflow DAG)"):
            ok = trigger_airflow_dag("etl_jira_reconciliation_deletes")
            if ok:
                st.success("DAG 'etl_jira_reconciliation_deletes' disparado exitosamente en Airflow.")
            else:
                st.error("No se pudo iniciar el DAG en Airflow. Verifica que el webserver esté activo.")

    st.markdown("---")
    st.markdown("#### ⚙️ Reconstrucción Masiva de Vistas Data Mart (Power BI)")
    if st.button("Reconstruir todas las vistas vw_mart_*"):
        with st.spinner("Reconstruyendo vistas dinámicas..."):
            try:
                df_all_proj = run_query("SELECT project_key FROM dim_jira_project WHERE is_sync_enabled = TRUE;")
                for pk in df_all_proj["project_key"]:
                    run_execute("CALL sp_rebuild_project_datamart_view(%s);", (pk,))
                st.success("Todas las vistas para Power BI han sido actualizadas con las columnas más recientes.")
            except Exception as e:
                st.error(f"Error reconstruyendo vistas: {e}")

