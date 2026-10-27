"""
Sistema de Alertas y Notificaciones para Airflow DAGs.
Envía alertas hacia Webhooks corporativos (Microsoft Teams / WhatsApp / Slack)
con hora en zona horaria America/Lima y enlace al log ante cualquier fallo de tarea.
"""

import os
import logging
import requests
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

LIMA_TZ = pytz.timezone("America/Lima")

def notify_webhook_failure(context):
    """
    Callback on_failure_callback para Airflow.
    Captura el contexto de error y envía una tarjeta detallada al Webhook corporativo.
    """
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url or webhook_url.startswith("https://placeholder"):
        logger.warning("ALERT_WEBHOOK_URL no configurado o es un placeholder. Se omite notificación externa.")
        return

    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "Desconocido"
    task_id = ti.task_id if ti else "Desconocido"
    execution_date = context.get("execution_date")
    exception = context.get("exception", "Error no especificado")
    log_url = ti.log_url if ti else ""

    # Fecha actual en Lima
    now_lima = datetime.now(LIMA_TZ).strftime("%Y-%m-%d %H:%M:%S (%Z)")

    # Tarjeta compatible con Microsoft Teams (MessageCard) y Webhooks JSON genéricos
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "D9381E", # Rojo Alerta
        "summary": f"Fallo en Pipeline ETL Jira: {dag_id}.{task_id}",
        "sections": [
            {
                "activityTitle": "🚨 **ALERTA CRÍTICA: Fallo en Pipeline de Datos Jira (Gobierno TI)**",
                "activitySubtitle": f"Ejecución: {now_lima}",
                "facts": [
                    {"name": "DAG:", "value": str(dag_id)},
                    {"name": "Tarea:", "value": str(task_id)},
                    {"name": "Fecha Ejecución:", "value": str(execution_date)},
                    {"name": "Error:", "value": f"```{str(exception)[:400]}```"}
                ],
                "markdown": True
            }
        ]
    }

    if log_url:
        payload["potentialAction"] = [
            {
                "@type": "OpenUri",
                "name": "🔍 Ver Logs en Airflow",
                "targets": [{"os": "default", "uri": log_url}]
            }
        ]

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            logger.info("Notificación de fallo enviada exitosamente al Webhook.")
        else:
            logger.warning(f"Respuesta inesperada del Webhook ({resp.status_code}): {resp.text}")
    except Exception as e:
        logger.error(f"Error enviando notificación al Webhook: {e}")

