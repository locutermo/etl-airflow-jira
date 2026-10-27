# Documento de Especificación Técnica e Ingeniería de Datos ( Blueprint Arquitectónico )
**Proyecto:** Plataforma de Orquestación ETL, Data Warehouse Analítico y Portal Operativo para Atlassian Jira Cloud  
**Organización / Área:** Gobierno TI - Integratel Perú  
**Instancia Origen:** `https://integratelperu.atlassian.net`  
**Volumetría Base:** +500,000 Issues | +1,000,000 Registros de Historial (Changelog) | +100 Espacios de Trabajo (Jira Software & Jira Service Management)  
**Propósito del Documento:** Guía maestra de implementación end-to-end para el Agente de Ingeniería de Software / Datos. Contiene todas las definiciones de arquitectura, esquemas DDL de base de datos, lógica de extracción incremental, algoritmos de transformación, procedimientos almacenados, diseño de interfaz gráfica (UI) para soporte y configuración de despliegue en contenedores Docker.

---

## 1. Resumen Ejecutivo y Objetivos del Sistema

El presente proyecto define la arquitectura e implementación de un ecosistema de datos 100% contenerizado y basado en tecnologías Open Source para extraer, procesar, almacenar y exponer la información operativa e histórica de la instancia corporativa de Jira Cloud (`integratelperu.atlassian.net`).

### 1.1. Objetivos Principales
1. **Desacoplamiento y Rendimiento en Power BI:** Eliminar las consultas lentas y cálculos pesados en DAX pre-calculando métricas, duraciones de transición de estados y jerarquías directamente en un Data Warehouse en **PostgreSQL 16**.
2. **Manejo de Heterogeneidad (+100 Proyectos):** Soportar más de 100 espacios de trabajo no estandarizados (mezcla de Jira Software, Jira Work Management y Jira Service Management con múltiples *Request Types*) sin generar tablas con cientos de columnas vacías (*sparse tables*).
3. **Extracción Incremental Eficiente:** Evitar la re-extracción innecesaria de tickets históricos no modificados mediante control de *Watermarks* y uso de la API de **Jira Expressions** para conciliación ultraligera de borrados.
4. **Regularización Histórica sin Costo de API (*Zero-API Backfill*):** Almacenar el payload JSON crudo en una capa de *Staging* para que, cuando se requiera agregar un campo preexistente a un reporte meses después, se pueble retroactivamente para los 500k registros en segundos vía SQL interno sin re-consultar la API de Atlassian.
5. **Operación Low-Code (Soporte L1/L2) y Visibilidad en Dailys:** Proveer una interfaz web ultraligera (accesible en localhost/servidor y publicada de forma segura hacia internet mediante **Ngrok** para 5 miembros del equipo) que permita monitorear el estado en las reuniones diarias (*dailys*), homologar campos, migrar prefijos de proyectos y regularizar datos sin tocar código Python ni consola Linux.

---

## 2. Arquitectura Global y Pila Tecnológica (Tech Stack)

### 2.1. Diagrama de Arquitectura de Alto Nivel

```text
+---------------------------------------------------------------------------------------------------+
|                                   ATLASSIAN JIRA CLOUD                                            |
|                         (https://integratelperu.atlassian.net)                                    |
|        [ REST API v3: Search JQL / Projects / Fields ]   [ Jira Expressions API: 10k IDs ]        |
+------------------------------------+------------------------------------+-------------------------+
                                     |                                    |
                                     | Extracción Incremental             | Auditoría Ultraligera
                                     v (Lotes JSON + Changelog)           v (Detección de Eliminados)
+---------------------------------------------------------------------------------------------------+
|                           SERVIDOR LINUX GOBIERNO TI (DOCKER HOST)                                |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  | 1. CAPA DE ORQUESTACIÓN: Apache Airflow 2.8+ (LocalExecutor)                                |  |
|  |    - DAG 1: etl_jira_incremental_sync (Horario / Cada 15-30 min)                            |  |
|  |    - DAG 2: etl_jira_reconciliation_deletes (Diario Nocturno o Bajo Demanda)                |  |
|  |    - DAG 3: admin_backfill_and_maintenance (Trigger manual desde UI con Params)             |  |
|  |    - Callbacks de Alerta: Notificación inmediata a Webhook (Teams / WhatsApp) ante fallos   |  |
|  +----------------------------------------------+----------------------------------------------+  |
|                                                 |                                                 |
|                                                 v                                                 |
|  +---------------------------------------------------------------------------------------------+  |
|  | 2. CAPA DE ALMACENAMIENTO: PostgreSQL 16 (Data Warehouse Analítico - Puerto 5433)           |  |
|  |                                                                                             |  |
|  |  [A. Capa Staging (ELT)]         [B. Capa Core & Extensiones]     [C. Capa Semántica / BI]  |  |
|  |  - stg_jira_issues_raw (JSONB)   - dim_jira_project               - meta_field_homologation |  |
|  |  - etl_watermarks                - dim_jira_user                  - meta_project_sync_stats |  |
|  |                                  - fact_jira_issues_core          - Vistas Dinámicas Pivot  |  |
|  |                                  - fact_jsm_requests                (vw_mart_*)             |  |
|  |                                  - fact_issue_custom_values (EAV)                           |  |
|  |                                  - fact_jira_changelog                                      |  |
|  |                                  - fact_issue_transitions (Pre-calculada con LEAD/LAG)      |  |
|  +----------------------------------------------+----------------------------------------------+  |
|                                                 ^                                                 |
|                                                 | Lectura / Escritura de Metadatos y Disparo API  |
|                                                 v                                                 |
|  +---------------------------------------------------------------------------------------------+  |
|  | 3. CAPA DE GESTIÓN Y MONITOREO:                                                             |  |
|  |    - Portal UI Operativo & Daily Tracker (Streamlit / Python Slim - Puerto 8501, ~50MB RAM) |  |
|  |    - Túnel Seguro Ngrok (Exposición HTTPS con Basic Auth para 5 usuarios en Dailys)         |  |
|  |    - cAdvisor (Monitoreo de CPU/RAM de contenedores Docker - Puerto 8085)                   |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
                                                 |
                                                 | Conexión Nativa PostgreSQL (Vistas Data Mart)
                                                 v
                                  +------------------------------+
                                  |     MICROSOFT POWER BI       |
                                  |  (Dashboards Corporativos)   |
                                  +------------------------------+
```

### 2.2. Componentes del Stack y Presupuesto de Memoria RAM (Docker)

| Servicio Docker | Imagen Base | Función Principal | Consumo RAM Estimado |
| :--- | :--- | :--- | :--- |
| `postgres-dw` | `postgres:16-alpine` | Motor de Data Warehouse (500k issues, 1M changelogs, JSONB). | 1.0 GB - 2.0 GB (Tuning analítico) |
| `postgres-airflow` | `postgres:15-alpine` | Base de datos interna de metadatos de Airflow. | 60 MB - 120 MB |
| `airflow-webserver` | Custom `apache/airflow:2.8.1-python3.10` | Interfaz web de Airflow y API REST interna. | 350 MB - 500 MB |
| `airflow-scheduler` | Custom `apache/airflow:2.8.1-python3.10` | Planificador y ejecutor de tareas (`LocalExecutor`). | 350 MB - 600 MB |
| `etl-admin-ui` | `python:3.11-slim` (Streamlit) | Portal web para Soporte L1/L2 y seguimiento en Dailys. | 45 MB - 80 MB |
| `ngrok-tunnel` | `ngrok/ngrok:latest` | Túnel HTTPS con autenticación básica para 5 usuarios externos. | 20 MB - 35 MB |
| `cadvisor` | `gcr.io/cadvisor/cadvisor:latest` | Telemetría y monitoreo de recursos de contenedores. | 40 MB - 70 MB |
| **TOTAL** | | **Requerimiento en Servidor:** Mínimo 4 GB RAM (Recomendado 8 GB RAM) | **~1.9 GB - 3.4 GB en carga** |

---

## 3. Diseño Detallado de la Base de Datos (PostgreSQL 16 DDL)

### 3.1. Principios de Modelado e Inmutabilidad
1. **Claves Primarias Inmutables:** Jamás utilizar `issue_key` (ej. `TI-123`) ni `project_key` (ej. `TI`) como llaves primarias físicas, ya que cambian cuando un ticket se mueve de proyecto o cuando un administrador renombra el prefijo de un espacio. Toda clave primaria y foránea de integridad referencial se basa en **`issue_id` (BIGINT)** y **`project_id` (BIGINT)**.
2. **Nomenclatura Estandarizada:** Todas las tablas y columnas usan `snake_case` en minúsculas.
   * Dimensiones: prefijo `dim_`
   * Tablas de hechos: prefijo `fact_`
   * Tablas de staging: prefijo `stg_`
   * Tablas de configuración/metadatos: prefijo `meta_`
   * Campos personalizados expuestos en vistas: prefijo `cf_<nombre_canonico>`

### 3.2. Script DDL Completo (`sql/01_init_schema.sql`)

```sql
-- =============================================================================
-- 1. CAPA DE CONTROL Y METADATOS OPERATIVOS
-- =============================================================================

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

CREATE INDEX idx_dim_project_key ON dim_jira_project(project_key);

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

-- Diccionario de Homologación Semántica y Mapeo por Proyecto / Request Type (Gestionado en UI)
CREATE TABLE IF NOT EXISTS meta_field_homologation (
    homologation_id SERIAL PRIMARY KEY,
    project_key VARCHAR(30) NOT NULL DEFAULT 'ALL',       -- 'ALL' o prefijo específico ('MESA')
    request_type_name VARCHAR(150) NOT NULL DEFAULT 'ALL',-- 'ALL' o tipo de solicitud JSM
    field_id VARCHAR(80) NOT NULL REFERENCES meta_jira_field_catalog(field_id),
    canonical_name VARCHAR(100) NOT NULL,                 -- Nombre estandarizado sin espacios (ej. 'area_solicitante')
    normalization_strategy VARCHAR(50) NOT NULL,          -- 'STRING', 'NUMBER', 'DATE', 'CASCADE_SPLIT', 'USER_DIM', 'MULTI_CSV', 'ADF_TEXT'
    expose_in_datamart BOOLEAN DEFAULT TRUE,              -- Si es TRUE, se incluye en la vista de Power BI
    track_in_transitions BOOLEAN DEFAULT FALSE,           -- Si es TRUE, se calcula tiempo de permanencia en el SP
    updated_by VARCHAR(100) DEFAULT 'system',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_key, request_type_name, field_id)
);

-- =============================================================================
-- 2. CAPA DE STAGING (ALMACÉN CRUDO PARA BACKFILL ZERO-API)
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg_jira_issues_raw (
    issue_id BIGINT PRIMARY KEY,
    project_id BIGINT NOT NULL,
    project_key VARCHAR(30) NOT NULL,
    issue_key VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    raw_payload JSONB NOT NULL,                  -- JSON íntegro devuelto por la API v3 de Jira
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stg_raw_project_key ON stg_jira_issues_raw(project_key);
CREATE INDEX idx_stg_raw_updated_at ON stg_jira_issues_raw(updated_at);
CREATE INDEX idx_stg_raw_jsonb_gin ON stg_jira_issues_raw USING GIN (raw_payload);

-- =============================================================================
-- 3. CAPA DIMENSIONAL Y DE HECHOS (CORE, JSM, EAV, CHANGELOG)
-- =============================================================================

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

CREATE INDEX idx_fact_core_project ON fact_jira_issues_core(project_key) WHERE is_deleted = FALSE;
CREATE INDEX idx_fact_core_updated ON fact_jira_issues_core(updated_at);
CREATE INDEX idx_fact_core_parent ON fact_jira_issues_core(parent_issue_key);
CREATE INDEX idx_fact_core_epic ON fact_jira_issues_core(epic_issue_key);
CREATE INDEX idx_fact_core_status ON fact_jira_issues_core(status_name);

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

CREATE INDEX idx_fact_jsm_project_req ON fact_jsm_requests(project_key, request_type_name);

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

CREATE INDEX idx_eav_project_canonical ON fact_issue_custom_values(project_key, canonical_name);
CREATE INDEX idx_eav_field_id ON fact_issue_custom_values(field_id);

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

CREATE INDEX idx_changelog_issue_field ON fact_jira_changelog(issue_id, field_name, changed_at);
CREATE INDEX idx_changelog_changed_at ON fact_jira_changelog(changed_at);

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

CREATE INDEX idx_transitions_bi_fast ON fact_issue_transitions(project_key, field_name, field_value);
CREATE INDEX idx_transitions_issue_id ON fact_issue_transitions(issue_id);
```

---

## 4. Reglas de Transformación por Tipo de Campo (Normalización Python -> SQL)

El módulo de transformación en Python (`plugins/jira_transformer.py`) debe procesar el JSON de `stg_jira_issues_raw` aplicando la siguiente matriz de reglas según el tipo de campo en Jira:

| Tipo de Campo en Jira | Estructura JSON en API v3 | Regla de Transformación y Almacenamiento |
| :--- | :--- | :--- |
| **Cascading Select** (`option-with-child`) | `{"value": "Padre", "child": {"value": "Hijo"}}` | Se descompone en **dos registros/columnas independientes**:<br>1. `cf_<nombre>_parent` = `"Padre"`<br>2. `cf_<nombre>_child` = `"Hijo"` |
| **User Picker** (`user` / `array of user`) | `{"accountId": "7120...", "displayName": "Juan Perez", "emailAddress": "jp@...", "active": true}` | 1. Se hace `UPSERT` del objeto usuario en la tabla `dim_jira_user` (`account_id`, `display_name`, `email_address`, `is_active`).<br>2. En `fact_issue_custom_values` se guarda `val_text = displayName` y `val_account_id = accountId`. |
| **Single Select / Radio** (`option`) | `{"id": "101", "value": "Opción A"}` | Se extrae `value` y se almacena limpio en `val_text`. |
| **Multi-Select / Checkboxes / Labels** (`array`) | `[{"value": "Red"}, {"value": "Server"}]` | Se ordenan alfabéticamente los valores y se concatenan separados por coma (`"Red, Server"`) en `val_text`. |
| **Atlassian Document Format (ADF)** (`doc`) | `{"type": "doc", "version": 1, "content": [...]}` | Función recursiva que recorre todos los nodos `"type": "text"` y `"type": "hardBreak"`, concatenando el texto plano limpio sin etiquetas ni sintaxis JSON. |
| **Jerarquía de Issues** (`parent` / `subtasks`) | `fields.issuetype.subtask` y `fields.parent` | - Si `subtask == True`: `hierarchy_level = -1`, `parent_issue_key = parent.key`.<br>- Si `subtask == False` y `issuetype.name not in ('Epic', 'Initiative')`: `hierarchy_level = 0`, `epic_issue_key = parent.key` (si tiene padre).<br>- Si `issuetype.name in ('Epic', 'Feature')`: `hierarchy_level = 1`. |
| **Customer Request Type (JSM)** | `customfield_10010.requestType.name` | Se detecta automáticamente el campo con `schema.custom == 'com.atlassian.servicedesk:vp-origin'` y se extrae `requestType.name`, `requestType.id` y `serviceDeskId` hacia `fact_jsm_requests`. |
| **SLAs de JSM** | `customfield_10015.completedCycles` y `ongoingCycle` | Se evalúa el último ciclo completado (o el ciclo en curso si aún no cierra). Se extrae `elapsedTime.millis / 1000`, `goalDuration.millis / 1000` y el booleano `breached`. |

---

## 5. Procedimientos Almacenados Críticos (Motor Analítico en BD)

El archivo `sql/02_stored_procedures.sql` debe implementar los siguientes 4 procedimientos almacenados en PostgreSQL:

### 5.1. Cálculo de Tiempos de Permanencia en el Historial (`sp_calculate_changelog_intervals`)
Calcula la fecha de inicio (`start_at`), fecha de fin (`end_at`) y duración en segundos (`duration_seconds`) para cada cambio de estado, asignado, prioridad o campo personalizado marcado con `track_in_transitions = TRUE`.

```sql
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
```

### 5.2. Regularización Histórica Instantánea sin API (`sp_backfill_custom_field_from_raw`)
Cuando se agrega un campo nuevo al diccionario de un proyecto y se requiere poblar los 500,000 tickets históricos que no han sufrido modificaciones recientes en Jira, este procedimiento extrae el valor directamente de la columna `raw_payload` de `stg_jira_issues_raw`:

```sql
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

    -- Caso 3: Texto Simple u Opción Simple
    ELSE
        INSERT INTO fact_issue_custom_values (issue_id, project_key, field_id, canonical_name, val_text)
        SELECT 
            s.issue_id, s.project_key, p_field_id, p_canonical_name,
            CASE 
                WHEN jsonb_typeof(s.raw_payload->'fields'->p_field_id) = 'object' 
                THEN COALESCE(s.raw_payload->'fields'->p_field_id->>'value', s.raw_payload->'fields'->p_field_id->>'name')
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
```

### 5.3. Migración de Prefijo de Espacio de Trabajo (`sp_migrate_project_key`)
Resuelve el problema donde un administrador renombra el prefijo de un proyecto en Jira (ej. de `OLD` a `NEW`), acción que en Jira no actualiza el timestamp `updated` de los tickets.

```sql
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
```

### 5.4. Generador Dinámico de Vistas para Power BI (`sp_rebuild_project_datamart_view`)
Construye automáticamente una vista SQL plana por espacio de trabajo (o dominio) pivotando únicamente los campos personalizados que pertenecen a ese proyecto, garantizando que Power BI consulte una tabla limpia y sin columnas vacías de otros proyectos:

```sql
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
```

---

## 6. Diseño de Flujos de Extracción (Airflow DAGs)

### 6.1. DAG 1: `etl_jira_incremental_sync` (Frecuencia: Cada 30 minutos)
1. **`sync_metadata_and_projects`**:
   * Consulta `/rest/api/3/project/search` y `/rest/api/3/field`.
   * Inserta proyectos nuevos en `dim_jira_project` (detectando si algún `project_id` existente cambió su `project_key`, en cuyo caso dispara automáticamente `CALL sp_migrate_project_key(old, new)`).
   * Actualiza `meta_jira_field_catalog`.
2. **`extract_incremental_issues`**:
   * Lee `last_successful_sync` de `meta_etl_watermarks` y resta un margen de seguridad (*buffer*) de 15 minutos para evitar pérdida de eventos por latencia de indexación en Jira:
     `JQL: updated >= "YYYY-MM-DD HH:mm" ORDER BY updated ASC`
   * Pagina sobre `/rest/api/3/search/jql` con `expand=changelog`, `fields=*all`, `maxResults=100` y `nextPageToken`.
   * Hace `UPSERT` en bloque en `stg_jira_issues_raw`.
3. **`transform_and_load_dw`**:
   * Procesa los registros modificados poblando `dim_jira_user`, `fact_jira_issues_core`, `fact_jsm_requests`, `fact_issue_custom_values` (según reglas de `meta_field_homologation`) y `fact_jira_changelog`.
4. **`calculate_intervals_and_stats`**:
   * Ejecuta `CALL sp_calculate_changelog_intervals(watermark)`.
   * Refresca la tabla `meta_project_sync_stats` para que el tablero de las *dailys* muestre conteos al instante.
   * Actualiza `meta_etl_watermarks` con `status = 'SUCCESS'`.

### 6.2. DAG 2: `etl_jira_reconciliation_deletes` (Auditoría Ultraligera con Jira Expressions API)
Resuelve la eliminación física de tickets en Jira Cloud extrayendo hasta **10,000 IDs por petición HTTP** mediante el endpoint de expresiones de Jira:

* **Endpoint:** `POST https://integratelperu.atlassian.net/rest/api/3/expression/eval`
* **Payload de Petición:**
  ```json
  {
    "expression": "issues.map(i => i.id)",
    "context": {
      "issues": {
        "jql": {
          "query": "project = 'TI' ORDER BY id ASC",
          "maxResults": 10000,
          "nextPageToken": "<token_cursor>"
        }
      }
    }
  }
  ```
* **Lógica de Cruce en PostgreSQL:**
  Se recibe el arreglo de enteros `active_ids` (ej. `[10001, 10002, ...]`) y se ejecuta un cruce directo con `UNNEST`:
  ```sql
  UPDATE fact_jira_issues_core f
  SET is_deleted = TRUE,
      deleted_detected_at = CURRENT_TIMESTAMP
  WHERE f.project_key = %(project_key)s
    AND f.is_deleted = FALSE
    AND NOT EXISTS (
        SELECT 1 FROM UNNEST(%(active_ids)s::BIGINT[]) AS active_id
        WHERE active_id = f.issue_id
    );
  ```

### 6.3. Sistema de Alertas ante Fallos (`plugins/alerts.py`)
Todos los DAGs incluyen en sus `default_args` el callback `'on_failure_callback': notify_webhook_failure`. Cuando cualquier tarea falla (después de agotar sus reintentos configurados), envía una tarjeta JSON con el nombre del DAG, tarea fallida, hora en zona horaria `America/Lima`, extracto de la excepción y el enlace directo al log hacia el Webhook de Microsoft Teams / WhatsApp del equipo de Gobierno TI.

---

## 7. Especificación de la Interfaz Gráfica Operativa (UI Low-Code & Daily Tracker)

Se construye una aplicación en **Streamlit (`ui/app.py`)** conectada a `postgres-dw` y a la API REST de Airflow. Está diseñada para ser usada en las reuniones diarias (*dailys*) por las 5 personas del equipo y para que Soporte (L1/L2) realice configuraciones sin intervenir en el código fuente.

### 7.1. Pestaña 1: Tablero de Control Daily & Inventario de Espacios (Home)
* **Tarjetas KPI Superiores:**
  * Estado Global de Sincronización (`🟢 SALUDABLE` / `🔴 ERROR EN ÚLTIMA CORRIDA`).
  * Fecha/Hora de última actualización exitosa (`Hace X minutos`).
  * Total Espacios Activos (`Ej. 104 Espacios`).
  * Total Issues Activos en BD (`512,430`) | Eliminados Detectados (`412`) | Filas en Changelog (`1,180,210`).
* **Tabla Interactiva de Espacios de Trabajo:**
  Lee directamente de `meta_project_sync_stats` mostrando:
  * `Prefijo (Key)` | `Nombre del Espacio` | `Tipo (JSM / Software)` | `Estado Sync (Activo/Pausado)` | `Cantidad de Registros` | `Última Modificación en Jira` | `Último Sync ETL` | `Cantidad de Campos Extraídos`.
* **Inspector Detallado por Espacio (Drill-Down):**
  Al seleccionar un espacio de trabajo (ej. `MESA`), despliega:
  1. **Listado de Campos Extraídos Actualmente:** Muestra el `field_id`, nombre en Jira, nombre de columna en BD (`cf_...`), tipo de transformación y el porcentaje de tickets que tienen ese campo lleno.
  2. **Campos Detectados en el JSON Crudo pero No Mapeados:** Escanea una muestra de `stg_jira_issues_raw` de ese proyecto y lista qué otros `customfield_XXXXX` tienen datos en Jira pero aún no están agregados al reporte, ofreciendo un botón `[+ Mapear e Incluir en este Proyecto]`.

### 7.2. Pestaña 2: Diccionario de Campos y Regularización Histórica (Backfill)
* Permite a Soporte L1/L2 agregar un nuevo campo a cualquier proyecto:
  * Selecciona el Espacio de Trabajo (`TODOS` o un `project_key` específico).
  * Selecciona el campo del catálogo (`customfield_XXXXX - Nombre en Jira`).
  * Define el nombre estandarizado (`canonical_name`, ej. `centro_costo`).
  * Selecciona la estrategia (`STRING`, `CASCADE_SPLIT`, `USER_DIM`, `NUMBER`, `DATE`).
  * Botón **"Guardar y Regularizar Históricos Ahora"**:
    1. Inserta la regla en `meta_field_homologation`.
    2. Ejecuta `CALL sp_backfill_custom_field_from_raw(...)` poblando los registros antiguos desde el JSONB local en segundos.
    3. Ejecuta `CALL sp_rebuild_project_datamart_view(...)` para que la columna aparezca inmediatamente en la vista de Power BI.

### 7.3. Pestaña 3: Mantenimiento de Integridad (Prefijos, Eliminados y Tiempos)
* **Sección A - Migrador Manual de Prefijo de Espacio:**
  * Selector `Prefijo Antiguo en BD` -> Input `Nuevo Prefijo en Jira` -> Botón **"Migrar Proyecto Ahora"** (invoca `sp_migrate_project_key`).
* **Sección B - Auditoría de Tickets Eliminados (Jira Expressions):**
  * Selector de proyecto (o `TODOS`) -> Botón **"Ejecutar Barrido de Eliminados"** (dispara vía API REST el DAG `etl_jira_reconciliation_deletes` y muestra cuántos tickets borrados se detectaron).
* **Sección C - Configuración de Campos en Tabla de Transiciones (Tiempos):**
  * Permite marcar con un check qué campos adicionales deben calcular `start_at`, `end_at` y `duration_seconds` en `fact_issue_transitions`.

---

## 8. Acceso Local y Exposición Segura a Internet con Ngrok (Para las Dailys)

Para cumplir con el requerimiento de que el portal pueda verse tanto localmente en el servidor como desde internet por las 5 personas del equipo durante las *dailys*:

1. **Acceso en Servidor / Red Local:** Disponible permanentemente en `http://localhost:8501` o `http://<IP_PRIVADA_SERVIDOR>:8501`.
2. **Acceso desde Internet vía Ngrok Dockerizado:**
   * Se incluye el contenedor oficial `ngrok/ngrok:latest` dentro del `docker-compose.yml`.
   * Se configura con un **dominio estático gratuito de Ngrok** (`--domain=${NGROK_STATIC_DOMAIN}`) para que la URL nunca cambie al reiniciar el servidor.
   * **Seguridad Obligatoria:** Se activa el parámetro `--basic-auth="${NGROK_USER}:${NGROK_PASSWORD}"` directamente en el borde de Ngrok. De esta forma, ninguna persona externa en internet puede visualizar el dashboard sin ingresar la credencial del equipo de Gobierno TI.

---

## 9. Estructura del Repositorio Git y Archivos de Configuración

El agente implementador debe crear exactamente la siguiente estructura de directorios:

```text
airflow-jira-gobierno-ti/
├── .env.example                # Plantilla de variables de entorno y secretos
├── .gitignore                  # Exclusión de .env, logs/ y volúmenes locales
├── README.md                   # Manual de despliegue y operación del servidor
├── docker-compose.yml          # Orquestación completa de los 7 contenedores
├── Dockerfile.airflow          # Imagen custom de Airflow con librerías ETL
├── sql/
│   ├── 01_init_schema.sql      # Creación de todas las tablas e índices
│   └── 02_stored_procedures.sql# Los 4 procedimientos almacenados críticos
├── dags/
│   ├── etl_jira_incremental.py # DAG principal de extracción incremental
│   ├── etl_jira_deletes.py     # DAG de reconciliación con Jira Expressions API
│   └── admin_maintenance.py    # DAG de soporte invocable con parámetros
├── plugins/
│   ├── jira_client.py          # Cliente HTTP con manejo de Rate Limit (429) y Backoff
│   ├── jira_transformer.py     # Parser de Cascadas, Users, ADF, JSM SLAs y Jerarquías
│   └── alerts.py               # Notificador de fallas hacia Webhook (Teams/WhatsApp)
└── ui/
    ├── Dockerfile.ui           # Imagen ultraligera python:3.11-slim para Streamlit
    ├── requirements.txt        # streamlit, psycopg2-binary, pandas, requests
    └── app.py                  # Código completo de las 3 pestañas del Portal UI
```

### 9.1. Archivo `docker-compose.yml` Definitivo para Producción

```yaml
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile.airflow
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@postgres-airflow/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DEFAULT_TIMEZONE: 'America/Lima'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    JIRA_BASE_URL: ${JIRA_BASE_URL}
    JIRA_USER_EMAIL: ${JIRA_USER_EMAIL}
    JIRA_API_TOKEN: ${JIRA_API_TOKEN}
    DW_CONN_URI: postgresql://${DW_USER}:${DW_PASSWORD}@postgres-dw:5432/${DW_DB}
    ALERT_WEBHOOK_URL: ${ALERT_WEBHOOK_URL}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-5000}:0"
  depends_on:
    postgres-airflow:
      condition: service_healthy
    postgres-dw:
      condition: service_healthy

services:
  # 1. Base de Datos Interna de Airflow
  postgres-airflow:
    image: postgres:15-alpine
    container_name: gobti-airflow-metadb
    environment:
      POSTGRES_USER: ${AIRFLOW_DB_USER}
      POSTGRES_PASSWORD: ${AIRFLOW_DB_PASSWORD}
      POSTGRES_DB: airflow
    volumes:
      - airflow_metadb_vol:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${AIRFLOW_DB_USER}"]
      interval: 10s
      retries: 5
    restart: always

  # 2. Data Warehouse Analítico de Jira (PostgreSQL 16)
  postgres-dw:
    image: postgres:16-alpine
    container_name: gobti-jira-dw
    environment:
      POSTGRES_USER: ${DW_USER}
      POSTGRES_PASSWORD: ${DW_PASSWORD}
      POSTGRES_DB: ${DW_DB}
      TZ: 'America/Lima'
    ports:
      - "5433:5432" # Expuesto para Power BI Gateway / Desktop
    volumes:
      - jira_dw_vol:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    command: >
      postgres -c shared_buffers=1GB
               -c effective_cache_size=3GB
               -c work_mem=64MB
               -c maintenance_work_mem=256MB
               -c max_parallel_workers_per_gather=2
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${DW_USER}", "-d", "${DW_DB}"]
      interval: 10s
      retries: 5
    restart: always

  # 3. Inicializador de Airflow
  airflow-init:
    <<: *airflow-common
    container_name: gobti-airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID:-5000}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_CREATE_USER: 'true'
      _AIRFLOW_USR_USER: ${AIRFLOW_ADMIN_USER:-admin}
      _AIRFLOW_PW_USER: ${AIRFLOW_ADMIN_PASSWORD:-admin}
      _AIRFLOW_ROLE_USER: 'Admin'
    user: "0:0"
    volumes:
      - ./:/sources

  # 4. Airflow Webserver
  airflow-webserver:
    <<: *airflow-common
    container_name: gobti-airflow-webserver
    command: webserver
    ports:
      - "8080:8080"
    restart: always

  # 5. Airflow Scheduler
  airflow-scheduler:
    <<: *airflow-common
    container_name: gobti-airflow-scheduler
    command: scheduler
    restart: always

  # 6. Portal UI Operativo & Daily Tracker (Streamlit Ultraligero)
  etl-admin-ui:
    build:
      context: ./ui
      dockerfile: Dockerfile.ui
    container_name: gobti-jira-ui
    environment:
      DW_HOST: postgres-dw
      DW_PORT: 5432
      DW_NAME: ${DW_DB}
      DW_USER: ${DW_USER}
      DW_PASSWORD: ${DW_PASSWORD}
      AIRFLOW_API_URL: http://airflow-webserver:8080/api/v1
      AIRFLOW_USER: ${AIRFLOW_ADMIN_USER:-admin}
      AIRFLOW_PASSWORD: ${AIRFLOW_ADMIN_PASSWORD:-admin}
    ports:
      - "8501:8501"
    depends_on:
      postgres-dw:
        condition: service_healthy
    restart: always

  # 7. Túnel Ngrok para Seguimiento en Dailys (5 Personas desde Internet)
  ngrok-tunnel:
    image: ngrok/ngrok:latest
    container_name: gobti-ngrok-tunnel
    command:
      - "http"
      - "etl-admin-ui:8501"
      - "--basic-auth=${NGROK_USER}:${NGROK_PASSWORD}"
      - "--domain=${NGROK_STATIC_DOMAIN}"
    environment:
      NGROK_AUTHTOKEN: ${NGROK_AUTHTOKEN}
    ports:
      - "4040:4040"
    depends_on:
      - etl-admin-ui
    restart: always

  # 8. Monitoreo de Recursos de Contenedores
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:latest
    container_name: gobti-cadvisor
    ports:
      - "8085:8080"
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
    restart: always

volumes:
  airflow_metadb_vol:
  jira_dw_vol:
```

---

## 10. Seguridad, Resiliencia y Plan de Implementación para el Agente

### 10.1. Lineamientos de Seguridad y Manejo de Errores
1. **Control de Rate Limiting (HTTP 429):** El cliente HTTP en `plugins/jira_client.py` debe inspeccionar la cabecera `Retry-After` de Atlassian ante respuestas `429 Too Many Requests` o errores `5xx`, aplicando *Exponential Backoff* con jitter antes de reintentar.
2. **Idempotencia Estricta:** Todo proceso de escritura en PostgreSQL debe utilizar sentencias `INSERT ... ON CONFLICT DO UPDATE` o transacciones atómicas para garantizar que re-ejecutar un lote jamás duplique filas.
3. **Aislamiento de Secretos:** Ningún API Token de Jira, contraseña de PostgreSQL ni Authtoken de Ngrok debe estar escrito en el código fuente; todos se inyectan exclusivamente desde el archivo `.env`.

### 10.2. Orden Secuencial de Construcción para el Agente Experto
1. **Fase 1 (Infraestructura y Base de Datos):** Generar la estructura de carpetas, los `Dockerfile`, el `docker-compose.yml` y los scripts `01_init_schema.sql` y `02_stored_procedures.sql`. Levantar y validar que las tablas y procedimientos existan en `postgres-dw`.
2. **Fase 2 (Conector Jira y Transformador):** Programar `plugins/jira_client.py` (con soporte para REST API v3 Search y Jira Expressions API) y `plugins/jira_transformer.py` (con soporte para Cascadas, Usuarios, ADF, Jerarquías y SLAs de JSM).
3. **Fase 3 (DAGs de Airflow y Alertas):** Implementar los 3 DAGs (`etl_jira_incremental.py`, `etl_jira_deletes.py`, `admin_maintenance.py`) junto con `plugins/alerts.py`.
4. **Fase 4 (Portal UI y Túnel Ngrok):** Desarrollar `ui/app.py` en Streamlit implementando las 3 pestañas operativas (Tablero Daily de Espacios/Campos, Homologación + Backfill Local y Mantenimiento de Prefijos/Eliminados).
5. **Fase 5 (Pruebas de Integración):** Ejecutar una sincronización inicial de prueba, validar la generación de vistas `vw_mart_<proyecto>` y verificar el cálculo de segundos en `fact_issue_transitions`.
