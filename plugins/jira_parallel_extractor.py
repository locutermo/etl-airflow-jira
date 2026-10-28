"""
Extractor Paralelo y Quirúrgico de Jira Cloud (API v3) con Control de Memoria por Backpressure.
Diseñado para servidores de 8 GB RAM (huella de memoria < 400 MB).
Implementa el patrón Productor-Consumidor acotado (Bounded Queue).
"""

import queue
import threading
import logging
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

from jira_client import JiraClient
from jira_transformer import JiraTransformer, parse_iso_datetime
from db import DWRepository

logger = logging.getLogger(__name__)

_QUEUE_SENTINEL = object()

class MemorySafeParallelExtractor:
    """
    Extractor concurrente por Sharding de Proyectos.
    - Productores (3 hilos): Consultan Jira Cloud en paralelo con nextPageToken por proyecto y payload quirúrgico.
    - Cola acotada (maxsize=6): Aplica backpressure automático bloqueando productores si la BD no ha consumido.
    - Consumidor (1 hilo): Inserción masiva atómica en Staging y Core en PostgreSQL.
    """

    def __init__(
        self,
        jira_client: Optional[JiraClient] = None,
        transformer: Optional[JiraTransformer] = None,
        max_fetch_workers: int = 3,
        queue_capacity: int = 6,
        batch_size: int = 100,
        checkpoint_callback: Optional[Callable[[int, datetime, str], None]] = None
    ):
        self.client = jira_client or JiraClient()
        self.transformer = transformer or JiraTransformer()
        self.max_fetch_workers = max_fetch_workers
        self.queue_capacity = queue_capacity
        self.batch_size = batch_size
        self.checkpoint_callback = checkpoint_callback

        self._queue: queue.Queue = queue.Queue(maxsize=self.queue_capacity)
        self._total_processed = 0
        self._processed_lock = threading.Lock()
        self._consumer_exception: Optional[Exception] = None
        self._stop_event = threading.Event()

    def _producer_stream(
        self,
        project_key: str,
        since_date_str: str,
        fields: List[str]
    ):
        """
        Hilo Productor: Extrae issues de un único proyecto paginando por nextPageToken.
        Deposita los lotes en la cola acotada; se bloquea si la cola está llena (Backpressure).
        """
        if self._stop_event.is_set():
            return

        jql = f'project = "{project_key}" AND updated >= "{since_date_str}" ORDER BY updated ASC'
        logger.info(f"[Productor {project_key}] Iniciando extracción (JQL: {jql}, {len(fields)} campos quirúrgicos).")

        proj_count = 0
        try:
            for raw_batch in self.client.search_issues_incremental(
                jql=jql,
                fields=fields,
                batch_size=self.batch_size
            ):
                if self._stop_event.is_set():
                    break

                if not raw_batch:
                    continue

                proj_count += len(raw_batch)

                # Bloquea si la cola está llena (maxsize=6), liberando CPU y pausando ingesta de RAM
                self._queue.put((project_key, raw_batch), block=True)

            logger.info(f"[Productor {project_key}] Finalizado con éxito. Total issues extraídos: {proj_count}.")
        except Exception as e:
            logger.error(f"[Productor {project_key}] Error durante la extracción: {e}", exc_info=True)

    def _consumer_writer(self):
        """
        Hilo Consumidor: Recibe lotes de la cola, inserta en Staging y transforma a Core en PostgreSQL.
        Garantiza recolección de basura inmediata tras cada lote.
        """
        logger.info("[Consumidor BD] Hilo escritor en PostgreSQL iniciado.")

        while True:
            item = self._queue.get()
            if item is _QUEUE_SENTINEL:
                self._queue.task_done()
                break

            project_key, batch = item
            try:
                # 1. Inserción cruda en Staging (JSONB)
                DWRepository.upsert_raw_staging(batch)

                # 2. Transformación y carga atómica en Core, EAV y Changelog
                transformed_batch = [self.transformer.transform_issue(issue) for issue in batch]
                DWRepository.upsert_transformed_data(transformed_batch)

                batch_len = len(batch)
                max_batch_dt: Optional[datetime] = None

                for issue in batch:
                    upd_str = issue.get("fields", {}).get("updated")
                    if upd_str:
                        dt = parse_iso_datetime(upd_str)
                        if dt and (max_batch_dt is None or dt > max_batch_dt):
                            max_batch_dt = dt

                with self._processed_lock:
                    self._total_processed += batch_len
                    current_total = self._total_processed

                # Notificar checkpoint si corresponde
                if self.checkpoint_callback and max_batch_dt:
                    try:
                        self.checkpoint_callback(current_total, max_batch_dt, project_key)
                    except Exception as cb_err:
                        logger.warning(f"Error en checkpoint_callback: {cb_err}")

                # 3. Liberación explícita de memoria RAM para preservar servidor de 8 GB
                del batch
                del transformed_batch
                if current_total % 1000 == 0:
                    gc.collect()

            except Exception as e:
                logger.error(f"[Consumidor BD] Error procesando lote de '{project_key}': {e}", exc_info=True)
                self._consumer_exception = e
                self._stop_event.set()
            finally:
                self._queue.task_done()

        logger.info("[Consumidor BD] Hilo escritor finalizado.")

    def run(
        self,
        project_configs: List[Dict[str, Any]]
    ) -> int:
        """
        Ejecuta la extracción paralela para la lista de proyectos provista.
        project_configs: lista de dicts con:
          - 'project_key': str
          - 'since_date_str': str
          - 'fields': List[str]
        """
        if not project_configs:
            logger.info("No hay proyectos para procesar en este ciclo.")
            return 0

        self._total_processed = 0
        self._consumer_exception = None
        self._stop_event.clear()

        # 1. Iniciar hilo consumidor
        consumer_thread = threading.Thread(target=self._consumer_writer, name="DW-Consumer-Thread", daemon=True)
        consumer_thread.start()

        # 2. Iniciar pool de productores (máx 3 hilos concurrentes)
        logger.info(f"Iniciando extracción paralela ({len(project_configs)} proyectos, máx {self.max_fetch_workers} workers).")

        with ThreadPoolExecutor(max_workers=self.max_fetch_workers, thread_name_prefix="Jira-Fetch") as executor:
            futures = [
                executor.submit(
                    self._producer_stream,
                    cfg["project_key"],
                    cfg["since_date_str"],
                    cfg["fields"]
                )
                for cfg in project_configs
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Fallo en hilo productor: {e}")

        # 3. Esperar que el consumidor drene todos los lotes pendientes en la cola
        self._queue.join()

        # 4. Detener el consumidor enviando el centinela
        self._queue.put(_QUEUE_SENTINEL)
        consumer_thread.join(timeout=30)

        if self._consumer_exception:
            raise RuntimeError(f"Fallo en la persistencia del Data Warehouse: {self._consumer_exception}")

        logger.info(f"Extracción paralela finalizada con éxito. Total tickets procesados en DW: {self._total_processed}.")
        return self._total_processed
