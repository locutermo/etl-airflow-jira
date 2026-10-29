"""
Cliente HTTP para Atlassian Jira Cloud REST API v3 y Jira Expressions API.
Diseñado para alta volumetría (+500k issues) con manejo robusto de Rate Limiting (HTTP 429) y Backoff Exponencial.
"""

import os
import time
import random
import logging
from typing import Dict, List, Optional, Any, Generator
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

from constants import (
    UNIVERSAL_BASE_FIELDS,
    JIRA_ASSETS_WORKSPACE_ID,
    JIRA_ASSETS_API_BASE_URL,
    ASSETS_PAGE_SIZE,
    ATLASSIAN_ORG_ID,
    ATLASSIAN_ORG_TOKEN,
    ATLASSIAN_ADMIN_API_BASE_URL,
    HTTP_MAX_RETRIES,
    HTTP_BASE_BACKOFF_SEC,
    HTTP_REQUEST_TIMEOUT
)

class JiraClient:
    """Cliente para interactuar de forma resiliente con Jira Cloud REST API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        user_email: Optional[str] = None,
        api_token: Optional[str] = None,
        max_retries: int = HTTP_MAX_RETRIES,
        base_backoff_sec: float = HTTP_BASE_BACKOFF_SEC,
        request_timeout: int = HTTP_REQUEST_TIMEOUT
    ):
        self.base_url = (base_url or os.getenv("JIRA_BASE_URL", "https://integratelperu.atlassian.net")).rstrip('/')
        self.user_email = user_email or os.getenv("JIRA_USER_EMAIL", "")
        self.api_token = api_token or os.getenv("JIRA_API_TOKEN", "")
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self.request_timeout = request_timeout

        if not self.user_email or not self.api_token:
            logger.warning("Credenciales de Jira no configuradas completamente (JIRA_USER_EMAIL o JIRA_API_TOKEN vacíos).")

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.user_email, self.api_token)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "GobiernoTI-Airflow-ETL/2.0"
        })

    def _request(
        self,
        method: str,
        endpoint: str,
        raise_for_status: bool = True,
        **kwargs
    ) -> requests.Response:
        """
        Ejecuta una petición HTTP con manejo inteligente de cabecera Retry-After y Backoff exponencial con jitter.
        """
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
        kwargs.setdefault("timeout", self.request_timeout)

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(method, url, **kwargs)

                # HTTP 429: Too Many Requests (Rate limit de Atlassian)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_time = int(retry_after) + random.uniform(0.5, 2.0)
                    else:
                        sleep_time = (self.base_backoff_sec ** attempt) + random.uniform(0.5, 2.0)
                    
                    logger.warning(
                        f"[HTTP 429] Rate Limit alcanzado en Jira Cloud ({url}). "
                        f"Reintentando en {sleep_time:.2f}s (Intento {attempt}/{self.max_retries})."
                    )
                    time.sleep(sleep_time)
                    continue

                # Errores 5xx del servidor Jira
                if 500 <= response.status_code < 600:
                    sleep_time = (self.base_backoff_sec ** attempt) + random.uniform(0.5, 2.0)
                    logger.warning(
                        f"[HTTP {response.status_code}] Error de servidor Jira ({url}). "
                        f"Reintentando en {sleep_time:.2f}s (Intento {attempt}/{self.max_retries})."
                    )
                    time.sleep(sleep_time)
                    continue

                if raise_for_status:
                    response.raise_for_status()
                return response

            except (requests.ConnectionError, requests.Timeout) as e:
                sleep_time = (self.base_backoff_sec ** attempt) + random.uniform(0.5, 2.0)
                logger.warning(
                    f"[Conexión/Timeout] Error conectando con Jira: {e}. "
                    f"Reintentando en {sleep_time:.2f}s (Intento {attempt}/{self.max_retries})."
                )
                time.sleep(sleep_time)

        raise RuntimeError(f"Se excedió el número máximo de reintentos ({self.max_retries}) para {method} {url}")

    def get_projects(self) -> List[Dict[str, Any]]:
        """
        Consulta todos los proyectos accesibles mediante /rest/api/3/project/search.
        Maneja paginación automática para obtener la totalidad (+100 proyectos).
        """
        projects = []
        start_at = 0
        max_results = 50

        while True:
            params = {
                "startAt": start_at,
                "maxResults": max_results,
                "expand": "description,lead,insight"
            }
            resp = self._request("GET", "/rest/api/3/project/search", params=params)
            data = resp.json()
            values = data.get("values", [])
            projects.extend(values)

            if data.get("isLast", True) or len(values) < max_results:
                break
            start_at += max_results

        logger.info(f"Se obtuvieron {len(projects)} proyectos desde Jira Cloud.")
        return projects

    def get_fields(self) -> List[Dict[str, Any]]:
        """
        Obtiene el catálogo completo de campos (estándar y customfields) de Jira Cloud.
        """
        resp = self._request("GET", "/rest/api/3/field")
        fields = resp.json()
        logger.info(f"Se catalogaron {len(fields)} campos en Jira Cloud.")
        return fields

    def get_project_fields(self, project_key: str) -> Dict[str, Dict[str, Any]]:
        """
        Consulta todos los campos configurados en las pantallas (Create/View/Edit) para un proyecto.
        Estrategia 1: /rest/api/3/issue/createmeta/{project_key}/issuetypes (rápido para proyectos con permisos de creación).
        Estrategia 2 (Fallback): Jerarquía de esquemas de pantallas (para proyectos de solo lectura o sin permiso de creación).
        Retorna: {field_id: {"name": str, "is_custom": bool}}
        """
        project_fields: Dict[str, Dict[str, Any]] = {}

        # 1. Intentar createmeta
        try:
            resp_types = self._request("GET", f"/rest/api/3/issue/createmeta/{project_key}/issuetypes", raise_for_status=False)
            if resp_types.status_code == 200:
                data_types = resp_types.json()
                if isinstance(data_types, dict):
                    issue_types = data_types.get("issueTypes") or data_types.get("values", [])
                elif isinstance(data_types, list):
                    issue_types = data_types
                else:
                    issue_types = []

                for itype in issue_types:
                    itype_id = itype.get("id")
                    if not itype_id:
                        continue

                    start_at = 0
                    max_results = 100

                    while True:
                        try:
                            params = {"startAt": start_at, "maxResults": max_results}
                            resp_fields = self._request(
                                "GET",
                                f"/rest/api/3/issue/createmeta/{project_key}/issuetypes/{itype_id}",
                                params=params,
                                raise_for_status=False
                            )
                            if resp_fields.status_code != 200:
                                break

                            data_fields = resp_fields.json()
                            if isinstance(data_fields, dict):
                                fields_list = data_fields.get("fields") or data_fields.get("values", [])
                            elif isinstance(data_fields, list):
                                fields_list = data_fields
                            else:
                                fields_list = []

                            for f in fields_list:
                                fid = f.get("fieldId") or f.get("id") or f.get("key")
                                fname = f.get("name", fid)
                                if fid and fid not in project_fields:
                                    is_custom = bool(fid.startswith("customfield_") or f.get("custom", False))
                                    project_fields[fid] = {
                                        "name": fname,
                                        "is_custom": is_custom
                                    }

                            total = data_fields.get("total", len(fields_list)) if isinstance(data_fields, dict) else len(fields_list)
                            if start_at + len(fields_list) >= total or len(fields_list) == 0:
                                break
                            start_at += len(fields_list)
                        except Exception:
                            break
            elif resp_types.status_code in (403, 404):
                logger.info(f"Proyecto '{project_key}': createmeta no habilitado (HTTP {resp_types.status_code}). Aplicando fallback de esquemas de pantalla...")
            else:
                logger.info(f"Proyecto '{project_key}': createmeta devolvió HTTP {resp_types.status_code}. Aplicando fallback de esquemas de pantalla...")
        except Exception as e:
            logger.info(f"Proyecto '{project_key}': createmeta no disponible ({e}). Aplicando fallback de esquemas de pantalla...")

        # 2. Fallback: Jerarquía formal de esquemas de pantallas
        if not project_fields:
            try:
                p_resp = self._request("GET", f"/rest/api/3/project/{project_key}", raise_for_status=False)
                if p_resp.status_code == 200:
                    p_id = p_resp.json().get("id")
                    if p_id:
                        scheme_resp = self._request("GET", f"/rest/api/3/issuetypescreenscheme/project?projectId={p_id}", raise_for_status=False)
                        if scheme_resp.status_code == 200:
                            vals = scheme_resp.json().get("values", [])
                            if vals:
                                scheme_id = vals[0].get("issueTypeScreenScheme", {}).get("id")
                                if scheme_id:
                                    m_resp = self._request("GET", f"/rest/api/3/issuetypescreenscheme/mapping?issueTypeScreenSchemeId={scheme_id}", raise_for_status=False)
                                    if m_resp.status_code == 200:
                                        mappings = m_resp.json().get("values", [])
                                        screen_scheme_ids = list(set(m.get("screenSchemeId") for m in mappings if m.get("screenSchemeId")))
                                        
                                        screen_ids = set()
                                        for sc_id in screen_scheme_ids:
                                            sc_resp = self._request("GET", f"/rest/api/3/screenscheme?id={sc_id}", raise_for_status=False)
                                            if sc_resp.status_code == 200:
                                                sc = sc_resp.json()
                                                for v in sc.get("values", []):
                                                    for s_type, s_id in v.get("screens", {}).items():
                                                        screen_ids.add(s_id)

                                        for s_id in screen_ids:
                                            tabs_resp = self._request("GET", f"/rest/api/3/screens/{s_id}/tabs", raise_for_status=False)
                                            if tabs_resp.status_code == 200:
                                                tabs = tabs_resp.json()
                                                for tab in tabs:
                                                    t_id = tab.get("id")
                                                    tf_resp = self._request("GET", f"/rest/api/3/screens/{s_id}/tabs/{t_id}/fields", raise_for_status=False)
                                                    if tf_resp.status_code == 200:
                                                        t_fields = tf_resp.json()
                                                        for f in t_fields:
                                                            fid = f.get("id")
                                                            fname = f.get("name", fid)
                                                            if fid and fid not in project_fields:
                                                                is_custom = bool(fid.startswith("customfield_") or f.get("custom", False))
                                                                project_fields[fid] = {
                                                                    "name": fname,
                                                                    "is_custom": is_custom
                                                                }
            except Exception as e_fb:
                logger.warning(f"Fallback de pantallas para '{project_key}' finalizó con advertencia: {e_fb}")

        logger.info(f"Proyecto '{project_key}': {len(project_fields)} campos detectados en pantallas de Jira.")
        return project_fields

    def search_issues_incremental(
        self,
        jql: str,
        fields: Optional[List[str]] = None,
        expand: str = "changelog",
        batch_size: int = 100
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Paginador por cursor / nextPageToken de issues con changelog completo.
        Devuelve lotes de issues en formato raw (JSON devuelto por Jira).
        Si no se especifican 'fields', usa UNIVERSAL_BASE_FIELDS (nunca '*all').
        """
        next_page_token = None
        has_more = True
        total_extracted = 0

        target_fields = fields if fields is not None else UNIVERSAL_BASE_FIELDS

        logger.info(f"Iniciando extracción incremental con JQL: {jql} ({len(target_fields)} campos solicitados)")

        while has_more:
            payload: Dict[str, Any] = {
                "jql": jql,
                "maxResults": batch_size,
                "expand": expand,
                "fields": target_fields
            }
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            resp = self._request("POST", "/rest/api/3/search/jql", json=payload)
            data = resp.json()
            issues = data.get("issues", [])
            next_page_token = data.get("nextPageToken")
            has_more = bool(next_page_token) and len(issues) > 0

            if issues:
                total_extracted += len(issues)
                logger.info(f"Lote extraído: {len(issues)} issues (Total acumulado: {total_extracted})")
                yield issues
            else:
                break

    def get_approximate_issue_count(self, jql: str) -> int:
        """
        Consulta a la API moderna de Jira Cloud el conteo aproximado de issues
        que cumplen con la condición JQL en < 200 ms.
        POST /rest/api/3/search/approximate-count
        """
        try:
            resp = self._request(
                "POST",
                "/rest/api/3/search/approximate-count",
                json={"jql": jql},
                raise_for_status=False
            )
            if resp.status_code == 200:
                return int(resp.json().get("count", 0))
            logger.warning(f"Error consultando conteo aproximado ({resp.status_code}): {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"Excepción obteniendo conteo aproximado de Jira: {e}")
        return 0

    def get_active_issue_ids_via_expression(self, project_key: str) -> List[int]:
        """
        Auditoría ultraligera: extrae hasta 10,000 IDs por request HTTP mediante Jira Expressions API.
        Permite detectar tickets eliminados físicamente en Jira sin re-extraer payloads.
        """
        all_ids: List[int] = []
        next_page_token: Optional[str] = None
        endpoint = "/rest/api/3/expression/eval"

        logger.info(f"Iniciando barrido de IDs para proyecto '{project_key}' vía Jira Expressions API.")

        while True:
            jql_query: Dict[str, Any] = {
                "query": f"project = '{project_key}' ORDER BY id ASC",
                "maxResults": 10000
            }
            if next_page_token:
                jql_query["nextPageToken"] = next_page_token

            body = {
                "expression": "issues.map(i => i.id)",
                "context": {
                    "issues": {
                        "jql": jql_query
                    }
                }
            }

            resp = self._request("POST", endpoint, json=body)
            result = resp.json()
            ids_batch = result.get("value", [])

            if isinstance(ids_batch, list):
                all_ids.extend([int(x) for x in ids_batch if str(x).isdigit()])

            # Revisar cursor de metadatos de contexto
            meta = result.get("meta", {}).get("issues", {}).get("jql", {})
            next_page_token = meta.get("nextPageToken")

            if not next_page_token or len(ids_batch) == 0:
                break

        logger.info(f"Total IDs activos encontrados en Jira para '{project_key}': {len(all_ids)}")
        return all_ids

    # =========================================================================
    # JIRA ASSETS / INSIGHT CMDB APIS
    # =========================================================================
    def get_assets_workspace_id(self) -> str:
        """Obtiene el ID del workspace de Assets (JSM Insight)."""
        if JIRA_ASSETS_WORKSPACE_ID:
            return JIRA_ASSETS_WORKSPACE_ID
        resp = self._request("GET", "/rest/servicedeskapi/insight/workspace")
        values = resp.json().get("values", [])
        if values and "workspaceId" in values[0]:
            return values[0]["workspaceId"]
        raise RuntimeError("No se encontró ningún workspaceId de Assets configurado en Jira Cloud.")

    def get_assets_schemas(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista todos los esquemas de objetos de la CMDB."""
        ws_id = workspace_id or self.get_assets_workspace_id()
        url = f"{JIRA_ASSETS_API_BASE_URL}/{ws_id}/v1/objectschema/list"
        resp = self._request("GET", url)
        return resp.json().get("values", [])

    def get_assets_object_types(self, schema_id: int, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista los tipos de objeto (Object Types) pertenecientes a un esquema."""
        ws_id = workspace_id or self.get_assets_workspace_id()
        url = f"{JIRA_ASSETS_API_BASE_URL}/{ws_id}/v1/objectschema/{schema_id}/objecttypes"
        resp = self._request("GET", url)
        return resp.json()

    def search_assets_objects_aql(
        self,
        schema_id: int,
        aql: Optional[str] = None,
        workspace_id: Optional[str] = None,
        batch_size: int = ASSETS_PAGE_SIZE
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Extrae paginadamente los objetos de la CMDB con sus atributos mediante AQL.
        """
        ws_id = workspace_id or self.get_assets_workspace_id()
        url = f"{JIRA_ASSETS_API_BASE_URL}/{ws_id}/v1/object/aql"
        ql_query = aql or f"objectSchemaId = {schema_id}"
        start_at = 0

        while True:
            params = {
                "startAt": start_at,
                "maxResults": batch_size,
                "includeAttributes": "true"
            }
            body = {"qlQuery": ql_query}
            resp = self._request("POST", url, params=params, json=body)
            data = resp.json()
            values = data.get("values", [])

            if not values:
                break

            yield values

            start_at += len(values)
            if data.get("isLast", False) or len(values) < batch_size:
                break

    # =========================================================================
    # ATLASSIAN ADMIN & DIRECTORY API (DIRECTORIO DE CORREOS CORPORATIVOS)
    # =========================================================================
    def get_organization_users(
        self,
        org_id: Optional[str] = None,
        org_token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Consulta el directorio corporativo de identidades de Atlassian Admin API.
        Permite extraer el correo corporativo real de todos los usuarios sin restricciones de perfil.
        Endpoint: https://api.atlassian.com/admin/v1/orgs/{orgId}/users
        """
        oid = org_id or ATLASSIAN_ORG_ID
        otoken = org_token or ATLASSIAN_ORG_TOKEN
        if not oid or not otoken:
            logger.warning("ATLASSIAN_ORG_ID o ATLASSIAN_ORG_TOKEN no configurados. Omite consulta de Admin Org API.")
            return []

        url = f"{ATLASSIAN_ADMIN_API_BASE_URL}/orgs/{oid}/users"
        headers = {
            "Authorization": f"Bearer {otoken}",
            "Accept": "application/json"
        }
        all_users = []
        cursor = None

        while True:
            params = {}
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(url, headers=headers, params=params, timeout=self.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            users = data.get("data", [])
            all_users.extend(users)

            links = data.get("links", {})
            cursor = links.get("next")
            if not cursor or not users:
                break

        logger.info(f"Directorio de Organización Atlassian extraído: {len(all_users)} identidades.")
        return all_users



