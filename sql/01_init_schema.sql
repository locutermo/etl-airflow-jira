-- =============================================================================
-- PROYECTO: PLATAFORMA DE INGENIERÍA DE DATOS JIRA CLOUD - GOBIERNO TI
-- ARCHIVO: sql/01_init_schema.sql
-- DESCRIPCIÓN: DDL de tablas maestras, staging, hechos, dimensiones y metadatos
-- =============================================================================

-- 1. CAPA DE CONTROL Y METADATOS OPERATIVOS
-- -----------------------------------------------------------------------------

-- Control de ejecución incremental (Watermarks)
CREATE TABLE IF NOT EXISTS meta_etl_watermarks (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_successful_sync TIMESTAMP WITH TIME ZONE NOT NULL,
    last_run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    records_processed INT DEFAULT 0,
    status VARCHAR(30) DEFAULT 'SUCCESS', -- SUCCESS, RUNNING, FAILED
    error_message TEXT
);

-- Catálogo Maestro de Espacios de Trabajo (Proyectos)
CREATE TABLE IF NOT EXISTS dim_jira_project (
    project_id BIGINT PRIMARY KEY,               -- ID inmutable en Jira (ej. 10024)
    project_key VARCHAR(30) NOT NULL UNIQUE,     -- Prefijo actual (ej. 'TI', 'MESA')
    previous_project_key VARCHAR(30),            -- Detecta si cambió el prefijo en Jira
    project_name VARCHAR(255) NOT NULL,
    project_type VARCHAR(50) NOT NULL,           -- 'software', 'service_desk', 'business'
    project_category VARCHAR(150),
    lead_account_id VARCHAR(128),
    is_sync_enabled BOOLEAN DEFAULT TRUE,        -- Controlable desde la UI
    created_in_dw_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_project_key ON dim_jira_project(project_key);

-- Estadísticas pre-calculadas por proyecto para carga instantánea en las Dailys (UI)
CREATE TABLE IF NOT EXISTS meta_project_sync_stats (
    project_id BIGINT PRIMARY KEY REFERENCES dim_jira_project(project_id) ON DELETE CASCADE,
    project_key VARCHAR(30) NOT NULL,
    project_name VARCHAR(255) NOT NULL,
    project_type VARCHAR(50) NOT NULL,
    is_sync_enabled BOOLEAN DEFAULT TRUE,
    total_active_issues INT DEFAULT 0,
    total_deleted_issues INT DEFAULT 0,
    mapped_custom_fields_count INT DEFAULT 0,
    unmapped_custom_fields_count INT DEFAULT 0,
    last_issue_updated_in_jira TIMESTAMP WITH TIME ZONE,
    last_etl_sync_at TIMESTAMP WITH TIME ZONE,
    last_sync_status VARCHAR(20) DEFAULT 'SUCCESS'
);

-- Catálogo de todos los campos existentes en la instancia de Jira
CREATE TABLE IF NOT EXISTS meta_jira_field_catalog (
    field_id VARCHAR(80) PRIMARY KEY,            -- Ej: 'customfield_10010'
    field_name_jira VARCHAR(255) NOT NULL,       -- Nombre en pantalla de Jira
    schema_type VARCHAR(100),                    -- 'option', 'option-with-child', 'user', 'array', 'number', 'string', 'datetime'
    schema_custom VARCHAR(255),                  -- Tipo interno del plugin de Atlassian
    is_custom BOOLEAN DEFAULT TRUE,
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Catálogo de Asociación: Campos configurados por cada Espacio de Trabajo
CREATE TABLE IF NOT EXISTS meta_project_field_association (
    project_id BIGINT NOT NULL REFERENCES dim_jira_project(project_id) ON DELETE CASCADE,
    project_key VARCHAR(30) NOT NULL,
    field_id VARCHAR(80) NOT NULL REFERENCES meta_jira_field_catalog(field_id),
    field_name_jira VARCHAR(255) NOT NULL,
    is_custom BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,             -- FALSE si fue removido en Jira
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_verified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, field_id)
);

CREATE INDEX IF NOT EXISTS idx_proj_field_active 
ON meta_project_field_association(project_key, field_id) 
WHERE is_active = TRUE;

-- Diccionario de Homologación Semántica y Mapeo por Proyecto / Request Type (Gestionado en UI)
CREATE TABLE IF NOT EXISTS meta_field_homologation (
    homologation_id SERIAL PRIMARY KEY,
    project_key VARCHAR(30) NOT NULL DEFAULT 'ALL',        -- 'ALL' o prefijo específico ('MESA')
    request_type_name VARCHAR(150) NOT NULL DEFAULT 'ALL', -- 'ALL' o tipo de solicitud JSM
    field_id VARCHAR(80) NOT NULL REFERENCES meta_jira_field_catalog(field_id),
    canonical_name VARCHAR(100) NOT NULL,                  -- Nombre estandarizado sin espacios (ej. 'area_solicitante')
    normalization_strategy VARCHAR(50) NOT NULL,           -- 'STRING', 'NUMBER', 'DATE', 'CASCADE_SPLIT', 'USER_DIM', 'MULTI_CSV', 'ADF_TEXT'
    expose_in_datamart BOOLEAN DEFAULT TRUE,               -- Si es TRUE, se incluye en la vista de Power BI
    track_in_transitions BOOLEAN DEFAULT FALSE,            -- Si es TRUE, se calcula tiempo de permanencia en el SP
    updated_by VARCHAR(100) DEFAULT 'system',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_key, request_type_name, field_id)
);

-- -----------------------------------------------------------------------------
-- 2. CAPA DE STAGING (ALMACÉN CRUDO PARA BACKFILL ZERO-API)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_jira_issues_raw (
    issue_id BIGINT PRIMARY KEY,
    project_id BIGINT NOT NULL,
    project_key VARCHAR(30) NOT NULL,
    issue_key VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    raw_payload JSONB NOT NULL,                  -- JSON íntegro devuelto por la API v3 de Jira
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_raw_project_key ON stg_jira_issues_raw(project_key);
CREATE INDEX IF NOT EXISTS idx_stg_raw_updated_at ON stg_jira_issues_raw(updated_at);
CREATE INDEX IF NOT EXISTS idx_stg_raw_jsonb_gin ON stg_jira_issues_raw USING GIN (raw_payload);

-- -----------------------------------------------------------------------------
-- 3. CAPA DIMENSIONAL Y DE HECHOS (CORE, JSM, EAV, CHANGELOG)
-- -----------------------------------------------------------------------------

-- Dimensión Unificada de Usuarios (Reporters, Assignees, Approvers, User Pickers)
CREATE TABLE IF NOT EXISTS dim_jira_user (
    account_id VARCHAR(128) PRIMARY KEY,
    display_name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    account_type VARCHAR(50),                    -- 'atlassian', 'customer', 'app'
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Hechos Principal (Campos Universales de todos los proyectos)
CREATE TABLE IF NOT EXISTS fact_jira_issues_core (
    issue_id BIGINT PRIMARY KEY,                 -- PK Inmutable
    issue_key VARCHAR(50) NOT NULL UNIQUE,       -- Mutable ante renombrado o movimiento
    project_id BIGINT NOT NULL REFERENCES dim_jira_project(project_id),
    project_key VARCHAR(30) NOT NULL,
    summary TEXT NOT NULL,
    description_plain TEXT,                      -- Extraído limpio desde formato ADF
    issue_type_id VARCHAR(50),
    issue_type_name VARCHAR(100) NOT NULL,
    is_subtask BOOLEAN DEFAULT FALSE,
    
    -- Atributos de Jerarquía Desnormalizados (Sub-task -> Standard -> Epic/Feature)
    hierarchy_level INT NOT NULL DEFAULT 0,      -- -1: Subtarea, 0: Estándar (Historia/Tarea/Incidente), 1: Épica/Feature
    parent_issue_id BIGINT,
    parent_issue_key VARCHAR(50),
    epic_issue_id BIGINT,
    epic_issue_key VARCHAR(50),
    epic_summary TEXT,
    
    -- Estados y Prioridades
    status_id VARCHAR(50),
    status_name VARCHAR(100) NOT NULL,
    status_category VARCHAR(50) NOT NULL,        -- 'To Do', 'In Progress', 'Done'
    priority_name VARCHAR(50),
    resolution_name VARCHAR(100),
    
    -- Responsables y Usuarios Core
    reporter_account_id VARCHAR(128) REFERENCES dim_jira_user(account_id),
    assignee_account_id VARCHAR(128) REFERENCES dim_jira_user(account_id),
    creator_account_id VARCHAR(128) REFERENCES dim_jira_user(account_id),
    
    -- Métricas Ágiles Estándar (Si aplican)
    sprint_name VARCHAR(255),
    story_points NUMERIC(10,2),
    labels_csv TEXT,
    components_csv TEXT,
    
    -- Fechas Clave
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    resolved_at TIMESTAMP WITH TIME ZONE,
    due_date DATE,
    
    -- Tiempos de Ciclo Globales Pre-calculados
    lead_time_seconds BIGINT,                    -- resolved_at - created_at en segundos
    
    -- Control de Borrado Lógico (Reconciliación)
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_detected_at TIMESTAMP WITH TIME ZONE,
    dw_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_core_project ON fact_jira_issues_core(project_key) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_fact_core_project_id ON fact_jira_issues_core(project_id);
CREATE INDEX IF NOT EXISTS idx_fact_core_updated ON fact_jira_issues_core(updated_at);
CREATE INDEX IF NOT EXISTS idx_fact_core_parent ON fact_jira_issues_core(parent_issue_key);
CREATE INDEX IF NOT EXISTS idx_fact_core_epic ON fact_jira_issues_core(epic_issue_key);
CREATE INDEX IF NOT EXISTS idx_fact_core_status ON fact_jira_issues_core(status_name);

-- Tabla de Extensión Exclusiva para Jira Service Management (JSM)
CREATE TABLE IF NOT EXISTS fact_jsm_requests (
    issue_id BIGINT PRIMARY KEY REFERENCES fact_jira_issues_core(issue_id) ON DELETE CASCADE,
    project_key VARCHAR(30) NOT NULL,
    service_desk_id VARCHAR(50),
    request_type_id VARCHAR(50),
    request_type_name VARCHAR(255),              -- Tipo de Solicitud del Portal JSM
    request_type_group VARCHAR(255),
    organization_id VARCHAR(50),
    organization_name VARCHAR(255),
    
    -- SLAs Nativos de JSM
    sla_first_response_target_sec BIGINT,
    sla_first_response_elapsed_sec BIGINT,
    sla_first_response_breached BOOLEAN,
    sla_resolution_target_sec BIGINT,
    sla_resolution_elapsed_sec BIGINT,
    sla_resolution_breached BOOLEAN,
    
    -- Encuesta de Satisfacción
    csat_rating INT,                             -- Escala 1 a 5
    csat_comment TEXT,
    dw_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_jsm_project_req ON fact_jsm_requests(project_key, request_type_name);

-- Tabla EAV Tipada para Campos Personalizados Heterogéneos (Cero Columnas NULL)
CREATE TABLE IF NOT EXISTS fact_issue_custom_values (
    issue_id BIGINT NOT NULL REFERENCES fact_jira_issues_core(issue_id) ON DELETE CASCADE,
    project_key VARCHAR(30) NOT NULL,
    field_id VARCHAR(80) NOT NULL,               -- Ej: 'customfield_10050' o 'customfield_10050_child'
    canonical_name VARCHAR(100) NOT NULL,        -- Ej: 'cf_area_solicitante_parent', 'cf_area_solicitante_child'
    val_text TEXT,                               -- Valor normalizado en texto (incluye Display Name de Users)
    val_number NUMERIC(18,4),                    -- Valor numérico si corresponde
    val_timestamp TIMESTAMP WITH TIME ZONE,      -- Valor fecha si corresponde
    val_account_id VARCHAR(128),                 -- Si es tipo USER, guarda el accountId para joins
    PRIMARY KEY (issue_id, canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_eav_project_canonical ON fact_issue_custom_values(project_key, canonical_name);
CREATE INDEX IF NOT EXISTS idx_eav_field_id ON fact_issue_custom_values(field_id);

-- Tabla de Historial Crudo de Cambios (1M+ Registros)
CREATE TABLE IF NOT EXISTS fact_jira_changelog (
    changelog_item_key VARCHAR(120) PRIMARY KEY, -- Compuesto: changelog_id + '_' + field_name
    changelog_id BIGINT NOT NULL,
    issue_id BIGINT NOT NULL REFERENCES fact_jira_issues_core(issue_id) ON DELETE CASCADE,
    project_key VARCHAR(30) NOT NULL,
    author_account_id VARCHAR(128),
    field_name VARCHAR(100) NOT NULL,            -- 'status', 'assignee', 'priority', o nombre de customfield
    field_id VARCHAR(80),
    from_string TEXT,
    from_value_id TEXT,
    to_string TEXT,
    to_value_id TEXT,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_changelog_issue_field ON fact_jira_changelog(issue_id, field_name, changed_at);
CREATE INDEX IF NOT EXISTS idx_changelog_changed_at ON fact_jira_changelog(changed_at);

-- Tabla Pre-Calculada de Transiciones e Intervalos en Segundos (Optimizada para Power BI)
CREATE TABLE IF NOT EXISTS fact_issue_transitions (
    transition_id BIGSERIAL PRIMARY KEY,
    issue_id BIGINT NOT NULL REFERENCES fact_jira_issues_core(issue_id) ON DELETE CASCADE,
    issue_key VARCHAR(50) NOT NULL,
    project_key VARCHAR(30) NOT NULL,
    field_name VARCHAR(100) NOT NULL,            -- Ej: 'status', 'assignee', 'priority'
    field_value TEXT NOT NULL,                   -- Valor en el que permaneció el ticket
    start_at TIMESTAMP WITH TIME ZONE NOT NULL,  -- Inicio exacto del estado/valor
    end_at TIMESTAMP WITH TIME ZONE,             -- Fin exacto del estado/valor (NULL si continúa en ese estado)
    duration_seconds BIGINT NOT NULL,            -- Segundos exactos transcurridos (end_at - start_at)
    is_current_state BOOLEAN DEFAULT FALSE,      -- TRUE si es el valor vigente del ticket
    transition_sequence INT NOT NULL             -- Orden cronológico (1, 2, 3...)
);

CREATE INDEX IF NOT EXISTS idx_transitions_bi_fast ON fact_issue_transitions(project_key, field_name, field_value);
CREATE INDEX IF NOT EXISTS idx_transitions_issue_id ON fact_issue_transitions(issue_id);

-- Inicializar Watermark por defecto si no existe (Zona Horaria America/Lima GMT-5)
INSERT INTO meta_etl_watermarks (pipeline_name, last_successful_sync, status)
VALUES ('etl_jira_incremental_sync', '2020-01-01 00:00:00-05', 'SUCCESS')
ON CONFLICT (pipeline_name) DO NOTHING;

