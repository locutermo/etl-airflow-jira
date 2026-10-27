-- =============================================================================
-- PROYECTO: PLATAFORMA DE INGENIERÍA DE DATOS JIRA CLOUD - GOBIERNO TI
-- ARCHIVO: sql/02_stored_procedures.sql
-- DESCRIPCIÓN: Procedimientos almacenados críticos para el motor analítico
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. CÁLCULO DE TIEMPOS DE PERMANENCIA EN EL HISTORIAL
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_calculate_changelog_intervals(p_since TIMESTAMP WITH TIME ZONE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Limpiar transiciones previas únicamente de los issues actualizados en el lote incremental
    DELETE FROM fact_issue_transitions
    WHERE issue_id IN (
        SELECT issue_id FROM fact_jira_issues_core WHERE updated_at >= p_since
    );

    -- 2. Reconstruir línea de tiempo completa (Evento Inicial en Creación + Eventos de Changelog)
    WITH modified_issues AS (
        SELECT issue_id, issue_key, project_key, created_at, status_name, priority_name
        FROM fact_jira_issues_core
        WHERE updated_at >= p_since
    ),
    tracked_fields AS (
        SELECT 'status' AS field_name
        UNION SELECT 'assignee'
        UNION SELECT 'priority'
        UNION SELECT canonical_name FROM meta_field_homologation WHERE track_in_transitions = TRUE
    ),
    initial_states AS (
        -- Sintetiza el Evento 0 (valor que tenía el campo cuando se creó el ticket)
        SELECT DISTINCT ON (c.issue_id, c.field_name)
            c.issue_id,
            i.issue_key,
            i.project_key,
            c.field_name,
            COALESCE(c.from_string, 'Sin Valor Inicial') AS field_value,
            i.created_at AS start_at,
            0 AS event_order
        FROM fact_jira_changelog c
        INNER JOIN modified_issues i ON c.issue_id = i.issue_id
        INNER JOIN tracked_fields tf ON c.field_name = tf.field_name
        ORDER BY c.issue_id, c.field_name, c.changed_at ASC
    ),
    subsequent_changes AS (
        -- Eventos 1..N registrados en el changelog
        SELECT
            c.issue_id,
            i.issue_key,
            i.project_key,
            c.field_name,
            COALESCE(c.to_string, 'Sin Valor') AS field_value,
            c.changed_at AS start_at,
            ROW_NUMBER() OVER (PARTITION BY c.issue_id, c.field_name ORDER BY c.changed_at ASC, c.changelog_id ASC) AS event_order
        FROM fact_jira_changelog c
        INNER JOIN modified_issues i ON c.issue_id = i.issue_id
        INNER JOIN tracked_fields tf ON c.field_name = tf.field_name
    ),
    unioned_timeline AS (
        SELECT * FROM initial_states
        UNION ALL
        SELECT * FROM subsequent_changes
    ),
    paired_intervals AS (
        SELECT
            issue_id,
            issue_key,
            project_key,
            field_name,
            field_value,
            start_at,
            LEAD(start_at) OVER (PARTITION BY issue_id, field_name ORDER BY start_at ASC, event_order ASC) AS end_at,
            ROW_NUMBER() OVER (PARTITION BY issue_id, field_name ORDER BY start_at ASC, event_order ASC) AS seq_num
        FROM unioned_timeline
    )
    INSERT INTO fact_issue_transitions (
        issue_id, issue_key, project_key, field_name, field_value,
        start_at, end_at, duration_seconds, is_current_state, transition_sequence
    )
    SELECT
        issue_id,
        issue_key,
        project_key,
        field_name,
        field_value,
        start_at,
        end_at,
        GREATEST(0, EXTRACT(EPOCH FROM (COALESCE(end_at, CURRENT_TIMESTAMP) - start_at))::BIGINT) AS duration_seconds,
        (end_at IS NULL) AS is_current_state,
        seq_num
    FROM paired_intervals;
END;
$$;

-- -----------------------------------------------------------------------------
-- 2. REGULARIZACIÓN HISTÓRICA INSTANTÁNEA SIN API (ZERO-API BACKFILL)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_backfill_custom_field_from_raw(
    p_project_key VARCHAR(30),
    p_field_id VARCHAR(80),
    p_canonical_name VARCHAR(100),
    p_strategy VARCHAR(50) -- 'STRING', 'NUMBER', 'DATE', 'CASCADE_SPLIT', 'USER_DIM'
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Caso 1: Campo en Cascada (Padre e Hijo)
    IF p_strategy = 'CASCADE_SPLIT' THEN
        -- Insertar Padre
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name || '_parent',
            s.raw_payload->'fields'->p_field_id->>'value'
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id IS NOT NULL
          AND jsonb_typeof(s.raw_payload->'fields'->p_field_id) = 'object'
          AND s.raw_payload->'fields'->p_field_id->>'value' IS NOT NULL
        ON CONFLICT (issue_id, canonical_name) DO UPDATE SET val_text = EXCLUDED.val_text;

        -- Insertar Hijo
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name || '_child',
            s.raw_payload->'fields'->p_field_id->'child'->>'value'
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id->'child' IS NOT NULL
          AND s.raw_payload->'fields'->p_field_id->'child'->>'value' IS NOT NULL
        ON CONFLICT (issue_id, canonical_name) DO UPDATE SET val_text = EXCLUDED.val_text;

    -- Caso 2: Campo Tipo Usuario (Extrae Usuario a Dimensión + Asocia Valor)
    ELSIF p_strategy = 'USER_DIM' THEN
        INSERT INTO dim_jira_user (account_id, display_name, email_address, is_active)
        SELECT DISTINCT ON (s.raw_payload->'fields'->p_field_id->>'accountId')
            s.raw_payload->'fields'->p_field_id->>'accountId',
            COALESCE(s.raw_payload->'fields'->p_field_id->>'displayName', 'Usuario Desconocido'),
            s.raw_payload->'fields'->p_field_id->>'emailAddress',
            COALESCE((s.raw_payload->'fields'->p_field_id->>'active')::BOOLEAN, TRUE)
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id->>'accountId' IS NOT NULL
        ON CONFLICT (account_id) DO UPDATE 
        SET display_name = EXCLUDED.display_name,
            email_address = COALESCE(EXCLUDED.email_address, dim_jira_user.email_address);

        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text, val_account_id)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name,
            s.raw_payload->'fields'->p_field_id->>'displayName',
            s.raw_payload->'fields'->p_field_id->>'accountId'
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id->>'accountId' IS NOT NULL
        ON CONFLICT (issue_id, canonical_name) DO UPDATE 
        SET val_text = EXCLUDED.val_text, val_account_id = EXCLUDED.val_account_id;

    -- Caso 3: Numérico
    ELSIF p_strategy = 'NUMBER' THEN
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text, val_number)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name,
            s.raw_payload->'fields'->>p_field_id,
            CASE 
                WHEN s.raw_payload->'fields'->>p_field_id ~ '^-?[0-9]+(\.[0-9]+)?$' 
                THEN (s.raw_payload->'fields'->>p_field_id)::NUMERIC 
                ELSE NULL 
            END
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id IS NOT NULL
          AND jsonb_typeof(s.raw_payload->'fields'->p_field_id) != 'null'
        ON CONFLICT (issue_id, canonical_name) DO UPDATE 
        SET val_text = EXCLUDED.val_text, val_number = EXCLUDED.val_number;

    -- Caso 4: Fecha / Timestamp
    ELSIF p_strategy = 'DATE' THEN
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text, val_timestamp)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name,
            s.raw_payload->'fields'->>p_field_id,
            CASE 
                WHEN s.raw_payload->'fields'->>p_field_id IS NOT NULL 
                THEN (s.raw_payload->'fields'->>p_field_id)::TIMESTAMP WITH TIME ZONE 
                ELSE NULL 
            END
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id IS NOT NULL
          AND jsonb_typeof(s.raw_payload->'fields'->p_field_id) != 'null'
        ON CONFLICT (issue_id, canonical_name) DO UPDATE 
        SET val_text = EXCLUDED.val_text, val_timestamp = EXCLUDED.val_timestamp;

    -- Caso 5: Texto Simple u Opción Simple / Array de Opciones
    ELSE
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name,
            CASE 
                WHEN jsonb_typeof(s.raw_payload->'fields'->p_field_id) = 'object' 
                THEN COALESCE(s.raw_payload->'fields'->p_field_id->>'value', s.raw_payload->'fields'->p_field_id->>'name')
                WHEN jsonb_typeof(s.raw_payload->'fields'->p_field_id) = 'array'
                THEN (
                    SELECT string_agg(COALESCE(elem->>'value', elem->>'name', elem::text), ', ')
                    FROM jsonb_array_elements(s.raw_payload->'fields'->p_field_id) elem
                )
                ELSE s.raw_payload->'fields'->>p_field_id
            END
        FROM stg_jira_issues_raw s
        WHERE (p_project_key = 'ALL' OR s.project_key = p_project_key)
          AND s.raw_payload->'fields'->p_field_id IS NOT NULL
          AND jsonb_typeof(s.raw_payload->'fields'->p_field_id) != 'null'
        ON CONFLICT (issue_id, canonical_name) DO UPDATE SET val_text = EXCLUDED.val_text;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 3. MIGRACIÓN DE PREFIJO DE ESPACIO DE TRABAJO
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_migrate_project_key(
    p_old_key VARCHAR(30),
    p_new_key VARCHAR(30)
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Actualizar dimensión de proyectos
    UPDATE dim_jira_project
    SET previous_project_key = p_old_key,
        project_key = p_new_key,
        last_synced_at = CURRENT_TIMESTAMP
    WHERE project_key = p_old_key;

    -- 2. Actualizar tabla Core reemplazando el prefijo en issue_key, parent_issue_key y epic_issue_key
    UPDATE fact_jira_issues_core
    SET project_key = p_new_key,
        issue_key = REGEXP_REPLACE(issue_key, '^' || p_old_key || '-', p_new_key || '-'),
        parent_issue_key = CASE 
            WHEN parent_issue_key LIKE p_old_key || '-%' 
            THEN REGEXP_REPLACE(parent_issue_key, '^' || p_old_key || '-', p_new_key || '-')
            ELSE parent_issue_key END,
        epic_issue_key = CASE 
            WHEN epic_issue_key LIKE p_old_key || '-%' 
            THEN REGEXP_REPLACE(epic_issue_key, '^' || p_old_key || '-', p_new_key || '-')
            ELSE epic_issue_key END,
        dw_updated_at = CURRENT_TIMESTAMP
    WHERE project_key = p_old_key;

    -- 3. Actualizar tablas relacionadas
    UPDATE stg_jira_issues_raw
    SET project_key = p_new_key,
        issue_key = REGEXP_REPLACE(issue_key, '^' || p_old_key || '-', p_new_key || '-')
    WHERE project_key = p_old_key;

    UPDATE fact_jsm_requests SET project_key = p_new_key WHERE project_key = p_old_key;
    UPDATE fact_issue_custom_values SET project_key = p_new_key WHERE project_key = p_old_key;
    UPDATE fact_jira_changelog SET project_key = p_new_key WHERE project_key = p_old_key;
    UPDATE fact_issue_transitions 
    SET project_key = p_new_key,
        issue_key = REGEXP_REPLACE(issue_key, '^' || p_old_key || '-', p_new_key || '-')
    WHERE project_key = p_old_key;

    UPDATE meta_field_homologation SET project_key = p_new_key WHERE project_key = p_old_key;
    UPDATE meta_project_sync_stats SET project_key = p_new_key WHERE project_key = p_old_key;
END;
$$;

-- -----------------------------------------------------------------------------
-- 4. GENERADOR DINÁMICO DE VISTAS PARA POWER BI
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_rebuild_project_datamart_view(p_project_key VARCHAR(30))
LANGUAGE plpgsql
AS $$
DECLARE
    v_cols_sql TEXT := '';
    v_view_name TEXT := 'vw_mart_' || LOWER(p_project_key);
    rec RECORD;
BEGIN
    -- Construir dinámicamente las expresiones MAX(CASE...) solo para los campos activos de este proyecto
    FOR rec IN (
        SELECT DISTINCT 
            CASE 
                WHEN normalization_strategy = 'CASCADE_SPLIT' THEN canonical_name || '_parent'
                ELSE canonical_name 
            END AS col_name
        FROM meta_field_homologation
        WHERE (project_key = p_project_key OR project_key = 'ALL')
          AND expose_in_datamart = TRUE
        UNION
        SELECT DISTINCT canonical_name || '_child' AS col_name
        FROM meta_field_homologation
        WHERE (project_key = p_project_key OR project_key = 'ALL')
          AND expose_in_datamart = TRUE
          AND normalization_strategy = 'CASCADE_SPLIT'
    ) LOOP
        v_cols_sql := v_cols_sql || format(
            ', MAX(CASE WHEN eav.canonical_name = %L THEN eav.val_text END) AS %I',
            rec.col_name, 'cf_' || rec.col_name
        );
    END LOOP;

    -- Crear o reemplazar la vista optimizada para Power BI
    EXECUTE format('
        CREATE OR REPLACE VIEW %I AS
        SELECT 
            c.issue_id,
            c.issue_key,
            c.project_key,
            c.summary,
            c.issue_type_name,
            c.hierarchy_level,
            c.parent_issue_key,
            c.epic_issue_key,
            c.status_name,
            c.status_category,
            c.priority_name,
            c.resolution_name,
            ur.display_name AS reporter_name,
            ur.email_address AS reporter_email,
            ua.display_name AS assignee_name,
            ua.email_address AS assignee_email,
            c.created_at,
            c.updated_at,
            c.resolved_at,
            c.lead_time_seconds,
            jsm.request_type_name,
            jsm.organization_name,
            jsm.sla_first_response_breached,
            jsm.sla_resolution_breached
            %s
        FROM fact_jira_issues_core c
        LEFT JOIN dim_jira_user ur ON c.reporter_account_id = ur.account_id
        LEFT JOIN dim_jira_user ua ON c.assignee_account_id = ua.account_id
        LEFT JOIN fact_jsm_requests jsm ON c.issue_id = jsm.issue_id
        LEFT JOIN fact_issue_custom_values eav ON c.issue_id = eav.issue_id
        WHERE c.project_key = %L
          AND c.is_deleted = FALSE
        GROUP BY 
            c.issue_id, ur.display_name, ur.email_address, 
            ua.display_name, ua.email_address,
            jsm.request_type_name, jsm.organization_name,
            jsm.sla_first_response_breached, jsm.sla_resolution_breached;
    ', v_view_name, v_cols_sql, p_project_key);
END;
$$;

