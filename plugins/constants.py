"""
Constantes Globales y Configuración Estándar del Proyecto Gobierno TI.
Centraliza identificadores de campos de Jira, esquemas de CMDB, zonas horarias y límites de ejecución.
"""

import os
import pytz

# ==============================================================================
# 1. ZONA HORARIA CORPORATIVA
# ==============================================================================
TIMEZONE_NAME = "America/Lima"
LIMA_TZ = pytz.timezone(TIMEZONE_NAME)

# ==============================================================================
# 2. CAMPOS UNIVERSALES BASE DE JIRA CLOUD
# ==============================================================================
# Lista quirúrgica de campos estándar que se extraen en todos los proyectos
UNIVERSAL_BASE_FIELDS = [
    "summary",
    "description",
    "issuetype",
    "status",
    "priority",
    "resolution",
    "resolutiondate",
    "created",
    "updated",
    "duedate",
    "reporter",
    "assignee",
    "creator",
    "parent",
    "project",
    "labels",
    "components",
    "timetracking",
    "timeoriginalestimate",
    "timespent",
    "customfield_10010",  # Customer Request Type (JSM)
    "customfield_10002"   # Organizations (JSM)
]

# ==============================================================================
# 3. JIRA SERVICE MANAGEMENT (SISTEMA & SLAS)
# ==============================================================================
JSM_REQUEST_TYPE_FIELD_ID = os.getenv("JSM_REQUEST_TYPE_FIELD_ID", "customfield_10010")
JSM_ORGANIZATIONS_FIELD_ID = os.getenv("JSM_ORGANIZATIONS_FIELD_ID", "customfield_10002")
JSM_SYSTEM_FIELD_TYPES = (
    "sd-customerrequesttype",
    "sd-servicelevelagreement"
)
JSM_SYSTEM_CUSTOM_PREFIX = "com.atlassian.servicedesk"

# ==============================================================================
# 4. JIRA ASSETS / INSIGHT CMDB
# ==============================================================================
JIRA_ASSETS_WORKSPACE_ID = os.getenv("JIRA_ASSETS_WORKSPACE_ID", "ae0390c7-daf0-4efd-8181-99c3b55f1d1c")
JIRA_ASSETS_CMDB_SCHEMA_ID = int(os.getenv("JIRA_ASSETS_CMDB_SCHEMA_ID", "8"))
JIRA_ASSETS_API_BASE_URL = "https://api.atlassian.com/jsm/assets/workspace"
CMDB_OBJECT_CUSTOM_TYPE = "com.atlassian.jira.plugins.cmdb:cmdb-object-cftype"

# ==============================================================================
# 5. ATLASSIAN ADMIN & DIRECTORY API (DIRECTORIO DE IDENTIDADES)
# ==============================================================================
ATLASSIAN_ORG_ID = os.getenv("ATLASSIAN_ORG_ID", "")
ATLASSIAN_ORG_TOKEN = os.getenv("ATLASSIAN_ORG_TOKEN", "")
ATLASSIAN_ADMIN_API_BASE_URL = "https://api.atlassian.com/admin/v1"

# ==============================================================================
# 6. PARÁMETROS OPERATIVOS Y LÍMITES DE MEMORIA (SERVIDOR 8 GB)
# ==============================================================================
DEFAULT_BATCH_SIZE = 100
ASSETS_PAGE_SIZE = 100
QUEUE_MAX_CAPACITY = 6
DEFAULT_FETCH_WORKERS = 3
HTTP_MAX_RETRIES = 5
HTTP_BASE_BACKOFF_SEC = 2.0
HTTP_REQUEST_TIMEOUT = 60
