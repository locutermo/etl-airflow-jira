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

class JiraClient:
    """Cliente para interactuar de forma resiliente con Jira Cloud REST API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        user_email: Optional[str] = None,
        api_token: Optional[str] = None,
        max_retries: int = 5,
        base_backoff_sec: float = 2.0,
        request_timeout: int = 60
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

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Ejecuta una petición HTTP con manejo inteligente de cabecera Retry-After y Backoff exponencial con jitter.
        """
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
        """
        next_page_token = None
        has_more = True
        total_extracted = 0

        logger.info(f"Iniciando extracción incremental con JQL: {jql}")

        while has_more:
            payload: Dict[str, Any] = {
                "jql": jql,
                "maxResults": batch_size,
                "expand": expand,
                "fields": fields if fields else ["*all"]
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

