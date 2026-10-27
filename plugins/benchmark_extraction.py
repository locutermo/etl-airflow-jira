"""
Benchmark de Rendimiento y Medición de Tiempos para Extracción Masiva Jira Cloud -> DW
Mide: Latencia HTTP por lote, tasa de throughput (issues/sec), tiempo de inserción en DB
y proyecta el tiempo total para +500,000 issues.
"""

import sys
import os
import time
import logging
from datetime import datetime

sys.path.insert(0, "/opt/airflow/plugins")
sys.path.insert(0, os.path.abspath("plugins"))

from jira_client import JiraClient
from jira_transformer import JiraTransformer, parse_iso_datetime
from db import DWRepository, get_dw_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("benchmark")

def run_benchmark(sample_batches=5, batch_size=100):
    logger.info(f"=== INICIANDO BENCHMARK REAL: {sample_batches} lotes de {batch_size} issues ({sample_batches*batch_size} total) ===")
    
    client = JiraClient()
    rules = DWRepository.get_homologation_rules()
    transformer = JiraTransformer(homologation_rules=rules)
    
    # JQL desde fecha antigua para simular carga histórica
    jql = 'updated >= "2020-01-01" ORDER BY updated ASC'
    
    http_times = []
    db_staging_times = []
    transform_times = []
    db_core_times = []
    batch_count = 0
    total_issues = 0
    
    t_start_global = time.time()
    
    for batch in client.search_issues_incremental(jql=jql, batch_size=batch_size):
        batch_count += 1
        total_issues += len(batch)
        
        # 1. Medir Inserción Staging
        t0 = time.time()
        DWRepository.upsert_raw_staging(batch)
        t_stage = time.time() - t0
        db_staging_times.append(t_stage)
        
        # 2. Medir Transformación
        t0 = time.time()
        transformed = [transformer.transform_issue(issue) for issue in batch]
        t_trans = time.time() - t0
        transform_times.append(t_trans)
        
        # 3. Medir Inserción Core
        t0 = time.time()
        DWRepository.upsert_transformed_data(transformed)
        t_core = time.time() - t0
        db_core_times.append(t_core)
        
        logger.info(
            f"Lote {batch_count}/{sample_batches} completado: "
            f"Staging={t_stage:.2f}s | Transform={t_trans:.2f}s | Core={t_core:.2f}s"
        )
        
        if batch_count >= sample_batches:
            break
            
    t_total_global = time.time() - t_start_global
    
    # Cálculos y proyecciones
    avg_sec_per_batch = t_total_global / batch_count
    issues_per_sec = total_issues / t_total_global
    
    target_500k = 500000
    est_total_sec = target_500k / issues_per_sec
    est_total_hours = est_total_sec / 3600
    est_total_min = est_total_sec / 60
    
    print("\n" + "="*80)
    print("           RESULTADOS DEL BENCHMARK DE EXTRACCIÓN REAL JIRA CLOUD")
    print("="*80)
    print(f"Tickets analizados en muestra:      {total_issues}")
    print(f"Tiempo total de la muestra:         {t_total_global:.2f} segundos")
    print(f"Rendimiento medio:                  {issues_per_sec:.2f} tickets / segundo")
    print(f"Tiempo promedio por lote (100 iss): {avg_sec_per_batch:.2f} segundos")
    print(f" - Escritura en Staging (JSONB):    {sum(db_staging_times)/len(db_staging_times):.3f}s / lote")
    print(f" - Transformación Python:           {sum(transform_times)/len(transform_times):.3f}s / lote")
    print(f" - Carga Dimensional y Core:        {sum(db_core_times)/len(db_core_times):.3f}s / lote")
    print("-" * 80)
    print(f"PROYECCIÓN PARA 500,000 TICKETS:")
    print(f" - Tiempo estimado total:           {est_total_hours:.2f} horas ({est_total_min:.1f} minutos)")
    print(f" - Peticiones HTTP totales:         5,000 requests de 100 issues")
    print("="*80 + "\n")

if __name__ == "__main__":
    run_benchmark(sample_batches=5, batch_size=100)

