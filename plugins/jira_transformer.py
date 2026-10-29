"""
Módulo de Normalización y Transformación de datos de Jira Cloud para PostgreSQL Data Warehouse.
Implementa las reglas analíticas de Gobierno TI: Cascadas, ADF a texto plano, Usuarios, JSM SLAs y Jerarquías.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import pytz

logger = logging.getLogger(__name__)

LIMA_TZ = pytz.timezone("America/Lima")

def parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Convierte cadenas ISO 8601 de Jira a objetos datetime normalizados en America/Lima (GMT-5)."""
    if not dt_str:
        return None
    try:
        # Manejar formato con zona horaria sin dos puntos (ej. -0500)
        clean_str = dt_str.replace("Z", "+00:00")
        if len(clean_str) > 23 and (clean_str[-5] in ('+', '-')) and clean_str[-3] != ':':
            clean_str = clean_str[:-2] + ':' + clean_str[-2:]
        dt = datetime.fromisoformat(clean_str)
        if dt.tzinfo is None:
            dt = LIMA_TZ.localize(dt)
        else:
            dt = dt.astimezone(LIMA_TZ)
        return dt
    except Exception:
        try:
            dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")
            return LIMA_TZ.localize(dt)
        except Exception:
            return None

def extract_adf_text(node: Any) -> str:
    """
    Recorre recursivamente un árbol Atlassian Document Format (ADF)
    extrayendo texto limpio sin etiquetas JSON ni formato residual.
    """
    if not node:
        return ""
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""

    node_type = node.get("type", "")
    text_accum = []

    if node_type == "text":
        return node.get("text", "")
    elif node_type == "hardBreak":
        return "\n"

    content = node.get("content", [])
    if isinstance(content, list):
        for child in content:
            child_text = extract_adf_text(child)
            if child_text:
                text_accum.append(child_text)

    joined = "".join(text_accum) if node_type in ("paragraph", "heading") else " ".join(text_accum)
    return joined.strip()

class JiraTransformer:
    """Transformador de payloads de Jira a estructuras de tablas analíticas."""

    def __init__(self, field_catalog: Optional[Dict[str, Dict[str, Any]]] = None, homologation_rules: Optional[List[Dict[str, Any]]] = None):
        """
        :param field_catalog: Dict mapeando field_id -> metadatos (schema_type, schema_custom)
        :param homologation_rules: Lista de reglas de meta_field_homologation
        """
        self.field_catalog = field_catalog or {}
        self.homologation_rules = homologation_rules or []

    def transform_issue(self, raw_issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforma un issue en bruto devuelto por la API v3 de Jira en registros listos para:
        - dim_jira_user
        - fact_jira_issues_core
        - fact_jsm_requests (si aplica)
        - fact_issue_custom_values (EAV)
        - fact_jira_changelog
        """
        issue_id = int(raw_issue["id"])
        issue_key = raw_issue["key"]
        fields = raw_issue.get("fields", {})
        project_data = fields.get("project", {})
        project_id = int(project_data.get("id", 0))
        project_key = project_data.get("key", issue_key.split("-")[0])

        # 1. Extraer Usuarios Clave (dim_jira_user)
        users_to_upsert: List[Dict[str, Any]] = []

        def register_user(u_dict: Optional[Dict[str, Any]]) -> Optional[str]:
            if not u_dict or not isinstance(u_dict, dict):
                return None
            account_id = u_dict.get("accountId")
            if not account_id:
                return None
            users_to_upsert.append({
                "account_id": account_id,
                "display_name": u_dict.get("displayName", "Usuario Desconocido"),
                "email_address": u_dict.get("emailAddress"),
                "is_active": u_dict.get("active", True),
                "account_type": u_dict.get("accountType", "atlassian")
            })
            return account_id

        reporter_id = register_user(fields.get("reporter"))
        assignee_id = register_user(fields.get("assignee"))
        creator_id = register_user(fields.get("creator"))

        # 2. Fechas y Tiempos de Ciclo
        created_at = parse_iso_datetime(fields.get("created"))
        updated_at = parse_iso_datetime(fields.get("updated"))
        resolution_date = parse_iso_datetime(fields.get("resolutiondate"))
        due_date_str = fields.get("duedate")
        due_date = datetime.strptime(due_date_str, "%Y-%m-%d").date() if due_date_str else None

        lead_time_sec = None
        if created_at and resolution_date:
            lead_time_sec = max(0, int((resolution_date - created_at).total_seconds()))

        # 3. Jerarquía de Issues
        issuetype = fields.get("issuetype", {})
        is_subtask = issuetype.get("subtask", False)
        type_name = issuetype.get("name", "Tarea")
        parent_obj = fields.get("parent")

        hierarchy_level = 0
        parent_issue_id = None
        parent_issue_key = None
        epic_issue_id = None
        epic_issue_key = None
        epic_summary = None

        if is_subtask:
            hierarchy_level = -1
            if parent_obj:
                parent_issue_id = int(parent_obj.get("id", 0)) if str(parent_obj.get("id", "")).isdigit() else None
                parent_issue_key = parent_obj.get("key")
        elif type_name in ("Epic", "Épica", "Feature", "Iniciativa"):
            hierarchy_level = 1
        else:
            hierarchy_level = 0
            if parent_obj:
                epic_issue_id = int(parent_obj.get("id", 0)) if str(parent_obj.get("id", "")).isdigit() else None
                epic_issue_key = parent_obj.get("key")
                epic_fields = parent_obj.get("fields", {})
                epic_summary = epic_fields.get("summary")

        # 4. Estados y Prioridades
        status_obj = fields.get("status", {})
        status_category_obj = status_obj.get("statusCategory", {})
        status_name = status_obj.get("name", "Abierto")
        status_category = status_category_obj.get("name", "To Do")

        priority_obj = fields.get("priority") or {}
        priority_name = priority_obj.get("name") if isinstance(priority_obj, dict) else None

        resolution_obj = fields.get("resolution") or {}
        resolution_name = resolution_obj.get("name") if isinstance(resolution_obj, dict) else None

        # 5. Descripción limpia (ADF)
        raw_description = fields.get("description")
        description_plain = extract_adf_text(raw_description) if raw_description else None

        # 6. Agilidad: Labels y Componentes CSV
        labels = fields.get("labels", [])
        labels_csv = ", ".join(sorted(labels)) if isinstance(labels, list) and labels else None

        components = fields.get("components", [])
        components_csv = ", ".join(sorted([c.get("name", "") for c in components if isinstance(c, dict) and c.get("name")])) if components else None

        # Objeto fact_jira_issues_core
        core_record = {
            "issue_id": issue_id,
            "issue_key": issue_key,
            "project_id": project_id,
            "project_key": project_key,
            "summary": fields.get("summary", ""),
            "description_plain": description_plain,
            "issue_type_id": str(issuetype.get("id")),
            "issue_type_name": type_name,
            "is_subtask": is_subtask,
            "hierarchy_level": hierarchy_level,
            "parent_issue_id": parent_issue_id,
            "parent_issue_key": parent_issue_key,
            "epic_issue_id": epic_issue_id,
            "epic_issue_key": epic_issue_key,
            "epic_summary": epic_summary,
            "status_id": str(status_obj.get("id")),
            "status_name": status_name,
            "status_category": status_category,
            "priority_name": priority_name,
            "resolution_name": resolution_name,
            "reporter_account_id": reporter_id,
            "assignee_account_id": assignee_id,
            "creator_account_id": creator_id,
            "sprint_name": None,
            "story_points": None,
            "labels_csv": labels_csv,
            "components_csv": components_csv,
            "created_at": created_at,
            "updated_at": updated_at,
            "resolved_at": resolution_date,
            "due_date": due_date,
            "lead_time_seconds": lead_time_sec,
            "is_deleted": False
        }

        # 7. Detección y Extracción JSM (fact_jsm_requests)
        jsm_record: Optional[Dict[str, Any]] = None
        for f_id, f_val in fields.items():
            if not f_val or not isinstance(f_val, dict):
                continue
            # Buscar Request Type JSM
            if "requestType" in f_val and isinstance(f_val.get("requestType"), dict):
                req_type = f_val.get("requestType", {})
                jsm_record = {
                    "issue_id": issue_id,
                    "project_key": project_key,
                    "service_desk_id": str(f_val.get("serviceDeskId", "")),
                    "request_type_id": str(req_type.get("id", "")),
                    "request_type_name": req_type.get("name"),
                    "request_type_group": req_type.get("groupName"),
                    "organization_id": None,
                    "organization_name": None,
                    "sla_first_response_target_sec": None,
                    "sla_first_response_elapsed_sec": None,
                    "sla_first_response_breached": None,
                    "sla_resolution_target_sec": None,
                    "sla_resolution_elapsed_sec": None,
                    "sla_resolution_breached": None,
                    "csat_rating": None,
                    "csat_comment": None
                }
                break
            elif f_id == "customfield_10010" and "name" in f_val:
                jsm_record = {
                    "issue_id": issue_id,
                    "project_key": project_key,
                    "service_desk_id": str(f_val.get("serviceDeskId", "")),
                    "request_type_id": str(f_val.get("id", "")),
                    "request_type_name": f_val.get("name"),
                    "request_type_group": f_val.get("groupName"),
                    "organization_id": None,
                    "organization_name": None,
                    "sla_first_response_target_sec": None,
                    "sla_first_response_elapsed_sec": None,
                    "sla_first_response_breached": None,
                    "sla_resolution_target_sec": None,
                    "sla_resolution_elapsed_sec": None,
                    "sla_resolution_breached": None,
                    "csat_rating": None,
                    "csat_comment": None
                }
                break

        # Extracción de Organizaciones JSM
        if jsm_record:
            orgs = fields.get("customfield_10002")
            if isinstance(orgs, list) and orgs:
                org_names = [o.get("name") for o in orgs if isinstance(o, dict) and o.get("name")]
                org_ids = [str(o.get("id")) for o in orgs if isinstance(o, dict) and o.get("id")]
                if org_names:
                    jsm_record["organization_name"] = ", ".join(org_names)
                if org_ids:
                    jsm_record["organization_id"] = org_ids[0]

        # Extracción de SLAs de JSM
        for f_id, f_val in fields.items():
            if not isinstance(f_val, dict) or "ongoingCycle" not in f_val and "completedCycles" not in f_val:
                continue
            sla_name = f_id.lower()
            cycles = f_val.get("completedCycles", [])
            last_cycle = cycles[-1] if cycles else f_val.get("ongoingCycle", {})
            if last_cycle:
                elapsed_ms = last_cycle.get("elapsedTime", {}).get("millis", 0)
                goal_ms = last_cycle.get("goalDuration", {}).get("millis", 0)
                breached = last_cycle.get("breached", False)

                if jsm_record:
                    if "response" in sla_name or "primera" in sla_name or "first" in sla_name:
                        jsm_record["sla_first_response_target_sec"] = int(goal_ms / 1000) if goal_ms else None
                        jsm_record["sla_first_response_elapsed_sec"] = int(elapsed_ms / 1000) if elapsed_ms else None
                        jsm_record["sla_first_response_breached"] = breached
                    else:
                        jsm_record["sla_resolution_target_sec"] = int(goal_ms / 1000) if goal_ms else None
                        jsm_record["sla_resolution_elapsed_sec"] = int(elapsed_ms / 1000) if elapsed_ms else None
                        jsm_record["sla_resolution_breached"] = breached

        # 8. Campos Personalizados Dinámicos (EAV: fact_issue_custom_values)
        custom_values: List[Dict[str, Any]] = []

        # Mapear según reglas de homologación cargadas en memoria
        for rule in self.homologation_rules:
            rule_proj = rule.get("project_key", "ALL")
            if rule_proj != "ALL" and rule_proj != project_key:
                continue

            field_id = rule.get("field_id")
            canonical_name = rule.get("canonical_name")
            strategy = rule.get("normalization_strategy", "STRING")

            if field_id not in fields or fields[field_id] is None:
                continue

            raw_val = fields[field_id]

            if strategy == "CASCADE_SPLIT" and isinstance(raw_val, dict):
                parent_val = raw_val.get("value")
                child_val = raw_val.get("child", {}).get("value") if isinstance(raw_val.get("child"), dict) else None
                if parent_val:
                    custom_values.append({
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "field_id": field_id,
                        "canonical_name": f"{canonical_name}_parent",
                        "val_text": str(parent_val),
                        "val_number": None,
                        "val_timestamp": None,
                        "val_account_id": None
                    })
                if child_val:
                    custom_values.append({
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "field_id": field_id,
                        "canonical_name": f"{canonical_name}_child",
                        "val_text": str(child_val),
                        "val_number": None,
                        "val_timestamp": None,
                        "val_account_id": None
                    })

            elif strategy == "USER_DIM":
                if isinstance(raw_val, dict) and "accountId" in raw_val:
                    u_acc_id = register_user(raw_val)
                    custom_values.append({
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "field_id": field_id,
                        "canonical_name": canonical_name,
                        "val_text": raw_val.get("displayName", "Usuario Desconocido"),
                        "val_number": None,
                        "val_timestamp": None,
                        "val_account_id": u_acc_id
                    })

            elif strategy == "NUMBER":
                try:
                    num_val = float(raw_val)
                    custom_values.append({
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "field_id": field_id,
                        "canonical_name": canonical_name,
                        "val_text": str(raw_val),
                        "val_number": num_val,
                        "val_timestamp": None,
                        "val_account_id": None
                    })
                except (ValueError, TypeError):
                    pass

            elif strategy == "DATE":
                dt_val = parse_iso_datetime(str(raw_val))
                custom_values.append({
                    "issue_id": issue_id,
                    "project_key": project_key,
                    "field_id": field_id,
                    "canonical_name": canonical_name,
                    "val_text": str(raw_val),
                    "val_number": None,
                    "val_timestamp": dt_val,
                    "val_account_id": None
                })

            else:
                # String / Multi-select / Option estándar
                text_val = None
                if isinstance(raw_val, dict):
                    text_val = raw_val.get("value") or raw_val.get("name") or str(raw_val)
                elif isinstance(raw_val, list):
                    items = []
                    for it in raw_val:
                        if isinstance(it, dict):
                            items.append(it.get("value") or it.get("name") or str(it))
                        else:
                            items.append(str(it))
                    text_val = ", ".join(sorted(items))
                else:
                    text_val = str(raw_val)

                if text_val is not None:
                    custom_values.append({
                        "issue_id": issue_id,
                        "project_key": project_key,
                        "field_id": field_id,
                        "canonical_name": canonical_name,
                        "val_text": text_val,
                        "val_number": None,
                        "val_timestamp": None,
                        "val_account_id": None
                    })

        # 9. Historial Crudo de Cambios (fact_jira_changelog)
        changelog_entries: List[Dict[str, Any]] = []
        raw_changelog = raw_issue.get("changelog", {})
        histories = raw_changelog.get("histories", [])

        for hist in histories:
            hist_id = int(hist.get("id", 0))
            author = hist.get("author")
            author_acc_id = register_user(author)
            created_dt = parse_iso_datetime(hist.get("created"))

            items = hist.get("items", [])
            seen_fields_in_hist: Dict[str, int] = {}
            for item in items:
                f_name = item.get("field", "").lower()
                f_id = item.get("fieldId")
                count = seen_fields_in_hist.get(f_name, 0)
                seen_fields_in_hist[f_name] = count + 1
                if count == 0:
                    item_key = f"{hist_id}_{f_name}"[:120]
                else:
                    item_key = f"{hist_id}_{f_name}_{count}"[:120]

                changelog_entries.append({
                    "changelog_item_key": item_key,
                    "changelog_id": hist_id,
                    "issue_id": issue_id,
                    "project_key": project_key,
                    "author_account_id": author_acc_id,
                    "field_name": f_name,
                    "field_id": f_id,
                    "from_string": item.get("fromString"),
                    "from_value_id": item.get("from"),
                    "to_string": item.get("toString"),
                    "to_value_id": item.get("to"),
                    "changed_at": created_dt
                })

        return {
            "users": users_to_upsert,
            "core": core_record,
            "jsm": jsm_record,
            "custom_values": custom_values,
            "changelog": changelog_entries
        }

