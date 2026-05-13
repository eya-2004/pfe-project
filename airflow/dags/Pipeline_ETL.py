

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional
import re as _re
import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

SOURCE_TO_TARGET: dict[str, str] = {
    "bscs_billing_account":        "corrected_billing_account",
    "bscs_billing_account_assign": "corrected_billing_account_assign",
    "bscs_charge":                 "corrected_charge",
    "bscs_customer":               "corrected_customer",
    "bscs_findocs":                "corrected_findocs",
    "bscs_memos":                  "corrected_memos",  
    "bscs_payments":               "corrected_payments",
    "bscs_payments_det":           "corrected_payments_det",
    "bscs_place":                  "corrected_place",
    "bscs_resource_directory":     "corrected_resource_directory",
    "bscs_resource_port":          "corrected_resource_port",
    "bscs_resource_sim":           "corrected_resource_sim",
    "bscs_services":               "corrected_services",
}

DEFAULT_ARGS = {
    "owner":            "migration_team",
    "depends_on_past":  False,
    "retries":          1,
    "email_on_failure": False,
}

def _source_hook() -> MySqlHook:
    return MySqlHook(mysql_conn_id="mysql_conn")

def _staging_hook() -> MySqlHook:
    return MySqlHook(mysql_conn_id="mysql_bss_staging")



class CompleteSqlRuleEngine:
    """
    Moteur de règles SQL complet.
    Chaque méthode _handle_xxx retourne (sql_string, params_dict) ou None si non applicable.
    """

    def __init__(self):
        self.stats = {"rules_applied": 0, "rows_modified": 0, "errors": [], "warnings": []}

    def apply_rule(self, hook: MySqlHook, table: str, rule: dict,
                   pk_col: str = None, batch_pks: list = None) -> dict:
        """Point d'entrée principal."""
        rule_id   = rule.get("rule_id", "?")
        rule_type = rule.get("rule_type", "UNKNOWN")
        col       = rule.get("column_name", "")
        value     = rule.get("rule_value", "") or ""
        target_col = rule.get("target_column") or col

        result = {
            "rule_id":       rule_id,
            "rule_type":     rule_type,
            "column":        target_col,
            "rows_affected": 0,
            "status":        "SKIPPED",
            "sql":           "",
        }

        try:
            handler_name = f"_handle_{rule_type.lower().replace('-', '_')}"
            handler = getattr(self, handler_name, None)

            if handler is None:
                sql, params = self._handle_generic(table, target_col, rule_type, value, rule)
            else:
                sql, params = handler(table, target_col, value, rule)

            if not sql:
                result["status"] = "NO_TEMPLATE"
                logger.warning(
                    "[%s] Rule %s (%s): pas de template", table, rule_id, rule_type
                )
                return result

            # ✅ Ajouter filtre PKs sur les UPDATE uniquement
            is_select = sql.strip().upper().startswith("SELECT")
            if not is_select and pk_col and batch_pks:
                ph_pks = ",".join(["%s"] * len(batch_pks))
                pk_params_list = [str(p) for p in batch_pks]
                sql_upper = sql.strip().upper()
                if " WHERE " in sql_upper:
                    sql = sql.rstrip() + f" AND `{pk_col}` IN ({ph_pks})"
                else:
                    sql = sql.rstrip() + f" WHERE `{pk_col}` IN ({ph_pks})"
                if isinstance(params, dict):
                    params = pk_params_list
                elif isinstance(params, (list, tuple)):
                    params = list(params) + pk_params_list
                else:
                    params = pk_params_list

            result["sql"] = sql[:200] + "..." if len(sql) > 200 else sql

            conn   = hook.get_conn()
            cursor = conn.cursor()

            if params and not is_select:
                cursor.execute(sql, params)
            elif not is_select:
                cursor.execute(sql)
            else:
                cursor.execute(sql)

            affected = cursor.rowcount if not is_select else 0
            conn.commit()
            cursor.close()

            result["rows_affected"] = affected
            result["status"] = (
                "APPLIED"   if affected > 0
                else "CHECKED" if is_select
                else "NO_CHANGE"
            )

            self.stats["rules_applied"] += 1
            if not is_select:
                self.stats["rows_modified"] += affected

            tag = "🔍" if is_select else "✅"
            logger.info(
                "[%s] %s Rule %s [%s] %s → %d rows",
                table, tag, rule_id, rule_type, target_col, affected
            )

        except Exception as e:
            result["status"] = f"ERROR: {str(e)}"
            self.stats["errors"].append(
                {"rule_id": rule_id, "table": table, "error": str(e)}
            )
            logger.error(
                "[%s] ❌ Rule %s [%s] ERROR: %s", table, rule_id, rule_type, e
            )

        return result

    def _handle_ref_check(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table, ref_col = v.split(":", 1)
        ref_table = ref_table.strip()
        ref_col   = ref_col.strip()
        return f"""SELECT COUNT(*) as ref_errors FROM `{t}`
                WHERE `{c}` IS NOT NULL AND TRIM(`{c}`) != ''
                AND CAST(`{c}` AS CHAR) NOT IN (
                    SELECT CAST(`{ref_col}` AS CHAR)
                    FROM `{ref_table}`
                    WHERE `{ref_col}` IS NOT NULL
                )""", {}

    def _handle_default_if_invalid_ref(self, t, c, v, r):
        default = "NULL" if (v or "").upper() == "NULL" else self._escape(v or "UNKNOWN")
        # On applique le défaut si NULL ou vide — la vérification référentielle
    
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')""", {}


    def _handle_cross_table_date_max(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        # Déduire la clé de jointure selon la table cible
        join_key = self._infer_join_key(t, ref_table)
        if not join_key:
            return None, {}

        return f"""UPDATE `{t}` t
                JOIN `{ref_table}` r ON t.`{join_key}` = r.`{join_key}`
                SET t.`{c}` = r.`{ref_col}`
                WHERE t.`{c}` > r.`{ref_col}`
                AND t.`{c}` IS NOT NULL
                AND r.`{ref_col}` IS NOT NULL""", {}


    def _handle_cross_table_date_min(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        join_key = self._infer_join_key(t, ref_table)
        if not join_key:
            return None, {}

        return f"""UPDATE `{t}` t
                JOIN `{ref_table}` r ON t.`{join_key}` = r.`{join_key}`
                SET t.`{c}` = r.`{ref_col}`
                WHERE t.`{c}` < r.`{ref_col}`
                AND t.`{c}` IS NOT NULL
                AND r.`{ref_col}` IS NOT NULL""", {}
    # Synchroniser la valeur du champ depuis une autre table

    def _handle_cross_table_sync(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        join_key = self._infer_join_key(t, ref_table)
        if not join_key:
            return None, {}

        return f"""UPDATE `{t}` t
                JOIN `{ref_table}` r ON t.`{join_key}` = r.`{join_key}`
                SET t.`{c}` = r.`{ref_col}`
                WHERE (t.`{c}` IS NULL
                    OR t.`{c}` != r.`{ref_col}`)
                AND r.`{ref_col}` IS NOT NULL""", {}

    
    # Plafonner la valeur numérique d'un champ à celle d'une colonne d'une autre table

    def _handle_cross_table_max_value(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        join_key = self._infer_join_key(t, ref_table)
        if not join_key:
            return None, {}

        return f"""UPDATE `{t}` t
                JOIN `{ref_table}` r ON t.`{join_key}` = r.`{join_key}`
                SET t.`{c}` = r.`{ref_col}`
                WHERE t.`{c}` > r.`{ref_col}`
                AND t.`{c}` IS NOT NULL
                AND r.`{ref_col}` IS NOT NULL""", {}

    # Forcer la valeur numérique à au moins celle d'une colonne d'une autre table

    def _handle_cross_table_min_value(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        join_key = self._infer_join_key(t, ref_table)
        if not join_key:
            return None, {}

        return f"""UPDATE `{t}` t
                JOIN `{ref_table}` r ON t.`{join_key}` = r.`{join_key}`
                SET t.`{c}` = r.`{ref_col}`
                WHERE t.`{c}` < r.`{ref_col}`
                AND t.`{c}` IS NOT NULL
                AND r.`{ref_col}` IS NOT NULL""", {}

  
    # Forcer le champ courant à NULL si un autre champ est NULL

    def _handle_null_if_other_null(self, t, c, v, r):
        other_col = (v or "").strip()
        if not other_col:
            return None, {}
        return f"""UPDATE `{t}` SET `{c}` = NULL
                WHERE (`{other_col}` IS NULL OR TRIM(`{other_col}`) = '')
                AND `{c}` IS NOT NULL""", {}


    def _handle_default_if_null_when_other_notnull(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        other_col, default = v.split(":", 1)
        other_col = other_col.strip()
        default   = self._escape(default.strip())
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')
                AND `{other_col}` IS NOT NULL
                AND TRIM(`{other_col}`) != ''""", {}

   
    def _handle_correlate_field_value(self, t, c, v, r):
        if not v or v.count(":") < 2:
            return None, {}
        parts       = v.split(":", 2)
        ref_col     = parts[0].strip()
        trigger_val = parts[1].strip()
        target_val  = parts[2].strip()
        return f"""UPDATE `{t}` SET `{c}` = {self._escape(target_val)}
                WHERE `{ref_col}` = {self._escape(trigger_val)}
                AND `{ref_col}` IS NOT NULL""", {}


    def _handle_compute_field(self, t, c, v, r):
        if not v:
            return None, {}
       
        import re as _re
        expr = _re.sub(r'\b([A-Z_][A-Z0-9_]*)\b', lambda m: f"`{m.group(1)}`", v)
        return f"""UPDATE `{t}` SET `{c}` = {expr}
                WHERE `{c}` IS NULL
                OR `{c}` != {expr}""", {}

    def _handle_aggregate_cap(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        ref_table_raw, ref_col = v.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_table     = SOURCE_TO_TARGET.get(ref_table_raw.lower(), ref_table_raw)

        # Clé de jointure supposée PAYMENT_ID pour payments_det → payments
        join_key = "PAYMENT_ID"

        return f"""UPDATE `{t}` det
                JOIN (
                    SELECT det2.`{join_key}`,
                        SUM(det2.`{c}`) AS total_paid,
                        p.`{ref_col}`   AS max_allowed
                    FROM `{t}` det2
                    JOIN `{ref_table}` p ON det2.`{join_key}` = p.`{join_key}`
                    GROUP BY det2.`{join_key}`, p.`{ref_col}`
                    HAVING total_paid > max_allowed
                ) ovr ON det.`{join_key}` = ovr.`{join_key}`
                SET det.`{c}` = ROUND(
                    det.`{c}` * (ovr.max_allowed / ovr.total_paid), 0
                )
                WHERE ovr.max_allowed IS NOT NULL
                AND ovr.total_paid > 0""", {}

    def _handle_aggregate_check_warn(self, t, c, v, r):
        # Réutilise la logique SELECT d'AGGREGATE_CHECK
        return self._handle_aggregate_check(t, c, v, r)
    def _handle_length_range_fix(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        parts   = v.split(":", 1)
        min_len = int(parts[0].strip()) if parts[0].strip().isdigit() else 3
        max_len = int(parts[1].strip()) if parts[1].strip().isdigit() else 10
        return f"""UPDATE `{t}` SET `{c}` =
                CASE
                    WHEN LENGTH(`{c}`) > {max_len}
                        THEN LEFT(`{c}`, {max_len})
                    WHEN LENGTH(`{c}`) < {min_len}
                        THEN LPAD(`{c}`, {min_len}, '0')
                    ELSE `{c}`
                END
                WHERE `{c}` IS NOT NULL
                AND (LENGTH(`{c}`) > {max_len} OR LENGTH(`{c}`) < {min_len})""", {}

    # Plafonner une date à la valeur d'une autre colonne de la même table
    # Format : ref_column
    # ════════════════════════════════════════════════════════════════════════════
    def _handle_date_max_field(self, t, c, v, r):
        ref_col = (v or "").strip()
        if not ref_col:
            return None, {}
        return f"""UPDATE `{t}` SET `{c}` = `{ref_col}`
                WHERE `{c}` > `{ref_col}`
                AND `{c}` IS NOT NULL
                AND `{ref_col}` IS NOT NULL""", {}


    # Si le champ courant ET un autre champ sont tous deux NULL → valeur par défaut

    def _handle_default_if_both_null(self, t, c, v, r):
        if not v or ":" not in v:
            return None, {}
        target_col, default = v.split(":", 1)
        target_col = target_col.strip()
        default    = self._escape(default.strip())
        return f"""UPDATE `{t}` SET `{target_col}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')
                AND (`{target_col}` IS NULL OR TRIM(`{target_col}`) = '')""", {}

    def _infer_join_key(self, source_table: str, ref_table: str) -> str | None:
        join_map = {
            ("corrected_services",               "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_services",               "corrected_billing_account"):   "BILLING_ACCOUNT_ID",
            ("corrected_services",               "corrected_place"):             "PLACE_ID",
            ("corrected_billing_account",        "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_place",                  "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_place",                  "corrected_billing_account"):   "BILLING_ACCOUNT_ID",
            ("corrected_place",                  "corrected_services"):          "PLACE_ID",
            ("corrected_resource_sim",           "corrected_services"):          "CONTRACT_ID",
            ("corrected_resource_port",          "corrected_services"):          "CONTRACT_ID",
            ("corrected_resource_port",          "corrected_resource_sim"):      "CONTRACT_ID",
            ("corrected_resource_directory",     "corrected_services"):          "CONTRACT_ID",
            ("corrected_resource_directory",     "corrected_resource_sim"):      "CONTRACT_ID",
            ("corrected_charge",                 "corrected_services"):          "CO_ID",
            ("corrected_charge",                 "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_findocs",                "corrected_billing_account"):   "BILLING_ACCOUNT_ID",
            ("corrected_findocs",                "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_payments",               "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_payments_det",           "corrected_payments"):          "PAYMENT_ID",
            ("corrected_payments_det",           "corrected_findocs"):           "ID_FINDOC",
            ("corrected_memos",                  "corrected_customer"):          "CUSTOMER_ID",
            ("corrected_billing_account_assign", "corrected_billing_account"):   "BILLING_ACCOUNT_ID",
            ("corrected_billing_account_assign", "corrected_customer"):          "CUSTOMER_ID",
        }
        key = (source_table.lower(), ref_table.lower())
        result = join_map.get(key)
        if not result:
            key_inv = (ref_table.lower(), source_table.lower())
            result  = join_map.get(key_inv)
        return result

    # ← LIGNE VIDE ICI — on est toujours dans la classe mais _infer_join_key est FERMÉE

    def apply_special_rules(self, hook: MySqlHook, table: str) -> int:
        """Règles spéciales multi-requêtes."""
        total = 0
        if table == "corrected_findocs":
            total += self._cross_fill_findocs(hook, table)
        if table == "corrected_payments_det":
            total += self._cross_check_payments_det(hook, table)
        if table == "corrected_services":
            total += self._cross_check_services_resources(hook, table)
        if table == "corrected_billing_account":
            total += self._cross_check_billing_account_services(hook, table)
        if table == "corrected_memos":
            total += self._cross_check_memos(hook, table)
        if table == "corrected_place":
            total += self._cross_check_place(hook, table)
        if table == "corrected_resource_sim":
            total += self._cross_check_resource_sim(hook, table)
        if table == "corrected_resource_port":
            total += self._cross_check_resource_port(hook, table)
        return total

    def _escape(self, value) -> str:
        if value is None: return "NULL"
        if isinstance(value, (int, float)): return str(value)
        escaped = str(value).replace("'", "\\'").replace("\\", "\\\\")
        return "'" + escaped + "'"

    def _handle_generic(self, t, c, rt, v, r):
        """Fallback intelligent."""
        rtl = rt.lower()
        if "null" in rtl and "when_other" in rtl: return self._handle_default_if_null_when_other_notnull(t,c,v,r)
        if "null" in rtl and "both" in rtl:       return self._handle_default_if_both_null(t,c,v,r)
        if "null_if_other" in rtl:                return self._handle_null_if_other_null(t,c,v,r)
        if "ref_check" in rtl:                    return self._handle_ref_check(t,c,v,r)
        if "invalid_ref" in rtl:                  return self._handle_default_if_invalid_ref(t,c,v,r)
        if "cross_table_date_max" in rtl:         return self._handle_cross_table_date_max(t,c,v,r)
        if "cross_table_date_min" in rtl:         return self._handle_cross_table_date_min(t,c,v,r)
        if "cross_table_sync" in rtl:             return self._handle_cross_table_sync(t,c,v,r)
        if "cross_table_max_value" in rtl:        return self._handle_cross_table_max_value(t,c,v,r)
        if "cross_table_min_value" in rtl:        return self._handle_cross_table_min_value(t,c,v,r)
        if "correlate_field_value" in rtl:        return self._handle_correlate_field_value(t,c,v,r)
        if "compute_field" in rtl:                return self._handle_compute_field(t,c,v,r)
        if "aggregate_cap" in rtl:                return self._handle_aggregate_cap(t,c,v,r)
        if "aggregate_check_warn" in rtl:         return self._handle_aggregate_check_warn(t,c,v,r)
        if "length_range_fix" in rtl:             return self._handle_length_range_fix(t,c,v,r)
        if "date_max_field" in rtl:               return self._handle_date_max_field(t,c,v,r)
        if "null" in rtl:                         return self._handle_default_if_null(t,c,v,r)
        if "force" in rtl:                        return self._handle_force_value(t,c,v,r)
        if "upper" in rtl and "trim" in rtl:      return self._handle_upper_trim(t,c,v,r)
        if "upper" in rtl:                        return self._handle_uppercase(t,c,v,r)
        if "lower" in rtl:                        return self._handle_lowercase(t,c,v,r)
        if "trim" in rtl:                         return self._handle_trim(t,c,v,r)
        if "mapping" in rtl:                      return self._handle_mapping(t,c,v,r)
        if "email" in rtl:                        return self._handle_default_if_invalid_email(t,c,v,r)
        if "phone" in rtl:                        return self._handle_default_if_invalid_phone(t,c,v,r)
        if "regex" in rtl:                        return self._handle_regex_validate(t,c,v,r)
        if "abs" in rtl:                          return self._handle_abs_value(t,c,v,r)
        if "round" in rtl:                        return self._handle_round_decimal(t,c,v,r)
        if "date" in rtl:
            if "coherence" in rtl:                return self._handle_date_coherence(t,c,v,r)
            if "min" in rtl or "range" in rtl:    return self._handle_date_min_limit(t,c,v,r)
            if "out_of_range" in rtl or "age" in rtl: return self._handle_default_if_date_out_of_range(t,c,v,r)
        if "length" in rtl:                       return self._handle_fixed_length(t,c,v,r)
        if "mask" in rtl:                         return self._handle_mask_data(t,c,v,r)
        if "space" in rtl:                        return self._handle_remove_spaces(t,c,v,r)
        if "mac" in rtl:                          return self._handle_mac_format(t,c,v,r)
        if "phone_format" in rtl or "phone_" in rtl: return self._handle_phone_format(t,c,v,r)
        if "alpha" in rtl:                        return self._handle_alpha_num_only(t,c,v,r)
        if "empty" in rtl:                        return self._handle_default_if_empty(t,c,v,r)
        if "invalid" in rtl and "length" in rtl:  return self._handle_default_if_invalid_length(t,c,v,r)
        if "unique" in rtl:                       return self._handle_unique_check(t,c,v,r)
        if "fk" in rtl or "foreign" in rtl:       return self._handle_foreign_key_check(t,c,v,r)
        if "mandatory" in rtl:                    return self._handle_mandatory_field(t,c,v,r)
        if "cross_fill" in rtl:                   return None, {}
        if "default_if_invalid_domain" in rtl:    return self._handle_default_if_invalid_domain(t,c,v,r)
        if "sync_from_field" in rtl:              return self._handle_sync_from_field(t,c,v,r)
        if "correlate_date_status" in rtl:        return self._handle_correlate_date_status(t,c,v,r)
        if "prefix_force" in rtl and "regex" not in rtl: return self._handle_prefix_force(t,c,v,r)
        if "regex_prefix_force" in rtl:           return self._handle_regex_prefix_force(t,c,v,r)
        if "default_if_invalid_regex" in rtl:     return self._handle_default_if_invalid_regex(t,c,v,r)
        if "not_equal_field" in rtl:              return self._handle_not_equal_field(t,c,v,r)
        if "min_value_strict" in rtl:             return self._handle_min_value_strict(t,c,v,r)
        if "min_value_or_null" in rtl:            return self._handle_min_value_or_null(t,c,v,r)
        if "max_value" in rtl and "field" not in rtl: return self._handle_max_value(t,c,v,r)
        if "phone_prefix_force" in rtl:           return self._handle_phone_prefix_force(t,c,v,r)
        if "mac_format_upper" in rtl:             return self._handle_mac_format_upper(t,c,v,r)
        if "regex_replace" in rtl:                return self._handle_regex_replace(t,c,v,r)
        return None, {}

    def _handle_force_value(self, t, c, v, r):
        return f"UPDATE `{t}` SET `{c}` = {self._escape(v)}", {}

    def _handle_default_if_null(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = {self._escape(v)}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '' OR `{c}` = 'None')""", {}

    def _handle_default_if_date_out_of_range(self, t, c, v, r):
        default = self._escape(v or "1900-01-01")
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR `{c}` < '1900-01-01'
                OR `{c}` > CURDATE() OR TRIM(`{c}`) = '')""", {}

    def _handle_mapping(self, t, c, v, r):
        if not v or ":" not in v: return None, {}
        cases = []
        for pair in v.split(","):
            if ":" in pair:
                k, val = pair.split(":", 1)
                cases.append(f"WHEN `{c}`='{k.strip()}' THEN '{val.strip()}'")
        if not cases: return None, {}
        return f"""UPDATE `{t}` SET `{c}` = CASE {' '.join(cases)} ELSE `{c}` END
                WHERE `{c}` IS NOT NULL AND `{c}` != ''""", {}

    def _handle_default_if_invalid_email(self, t, c, v, r):
        default = self._escape(v or "migration-ca@orange.com")
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = ''
                OR (`{c}` NOT REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'
                    AND `{c}` != ''))""", {}

    def _handle_default_if_invalid_phone(self, t, c, v, r):
        default = self._escape(v or "NULL")
        is_null = default.upper() == "NULL"
        if is_null:
            return f"""UPDATE `{t}` SET `{c}` = NULL
                    WHERE (`{c}` IS NULL OR TRIM(`{c}`) = ''
                    OR (`{c}` NOT REGEXP '^[+]?[0-9]{{7,15}}$' AND `{c}` != ''))""", {}
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = ''
                OR (`{c}` NOT REGEXP '^[+]?[0-9]{{7,15}}$' AND `{c}` != ''))""", {}

    def _handle_default_if_empty(self, t, c, v, r):
        return f"UPDATE `{t}` SET `{c}` = {self._escape(v)} WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')", {}

    def _handle_default_if_invalid_length(self, t, c, v, r):
        default = self._escape(v or "UNKNOWN")
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE `{c}` IS NOT NULL AND TRIM(`{c}`) != ''
                AND LENGTH(`{c}`) NOT IN (10,11,12,13,14,15,20,21,24)""", {}

    def _handle_upper_trim(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = TRIM(UPPER(`{c}`))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND (BINARY `{c}` != BINARY TRIM(UPPER(`{c}`))
                    OR `{c}` LIKE ' %' OR `{c}` LIKE '% ')""", {}

    def _handle_phone_format(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = CONCAT('225',
                    REGEXP_SUBSTR(REGEXP_REPLACE(`{c}`, '^0', ''), '[0-9]+'))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND (`{c}` NOT REGEXP '^225[0-9]{{8,}}$'
                    OR LEFT(`{c}`,3) IN ('015','057','001'))""", {}

    def _handle_alpha_num_only(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = REGEXP_REPLACE(REGEXP_REPLACE(`{c}`, '[^a-zA-Z0-9]', ''), '\\s+', '')
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND `{c}` REGEXP '[^a-zA-Z0-9]'""", {}

    def _handle_lowercase(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = LOWER(`{c}`)
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND BINARY `{c}` != BINARY LOWER(`{c}`)""", {}

    def _handle_map_or_default(self, t, c, v, r):
        if not v: return None, {}
        parts = v.split("|")
        mapping_str = parts[0]
        default_val = parts[1] if len(parts) > 1 else "UNKNOWN"
        cases = []
        for pair in mapping_str.split(","):
            if ":" in pair:
                k, val = pair.split(":", 1)
                cases.append(f"WHEN `{c}`='{k.strip()}' THEN '{val.strip()}'")
        if cases:
            return f"""UPDATE `{t}` SET `{c}` = CASE {' '.join(cases)} ELSE '{default_val}' END
                    WHERE `{c}` IS NOT NULL AND `{c}` != ''""", {}
        return None, {}

    def _handle_max_value_field(self, t, c, v, r):
        ref = v or "INITAMOUNT"
        return f"UPDATE `{t}` SET `{c}` = `{ref}` WHERE `{c}` > `{ref}` AND `{ref}` IS NOT NULL", {}

    def _handle_round_decimal(self, t, c, v, r):
        dec = int(v) if v and v.isdigit() else 0
        return f"UPDATE `{t}` SET `{c}` = ROUND(`{c}`, {dec}) WHERE `{c}` IS NOT NULL", {}

    def _handle_date_coherence(self, t, c, v, r):
        ref = v or "ISSUEDATE"
        return f"""UPDATE `{t}` SET `{c}` = `{ref}`
                WHERE `{c}` < `{ref}` AND `{c}` IS NOT NULL AND `{ref}` IS NOT NULL""", {}

    def _handle_age_min_check(self, t, c, v, r):
        age = int(v) if v and v.isdigit() else 18
        return f"""UPDATE `{t}` SET `{c}` = DATE_SUB(CURDATE(), INTERVAL {age} YEAR)
                WHERE `{c}` > DATE_SUB(CURDATE(), INTERVAL {age} YEAR)
                AND `{c}` IS NOT NULL""", {}

    def _handle_regex_validate(self, t, c, v, r):
        pattern = v or "^[a-zA-Z0-9]+$"
        default = ""
        if ":" in str(v): pattern, default = v.split(":", 1)[0], v.split(":", 1)[1]
        safe_def = self._escape(default) if default else "NULL"
        return f"""UPDATE `{t}` SET `{c}` = {safe_def}
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND `{c}` NOT REGEXP '{pattern}'""", {}

    def _handle_fixed_length(self, t, c, v, r):
        length = int(v) if v and v.isdigit() else 20
        return f"UPDATE `{t}` SET `{c}` = LEFT(CONCAT(`{c}`, REPEAT('0', {length})), {length}) WHERE `{c}` IS NOT NULL AND LENGTH(`{c}`) != {length}", {}

    def _handle_fixed_length_num(self, t, c, v, r):
        length = int(v) if v and v.isdigit() else 15
        return f"UPDATE `{t}` SET `{c}` = LEFT(`{c}`, {length}) WHERE `{c}` IS NOT NULL AND `{c}` REGEXP '^[0-9]+$' AND LENGTH(`{c}`) != {length}", {}

    def _handle_prefix_check(self, t, c, v, r):
        prefix = v or "61203"
        plen = len(prefix)
        return f"UPDATE `{t}` SET `{c}` = CONCAT('{prefix}', RIGHT(`{c}`, LENGTH(`{c}`) - {plen})) WHERE `{c}` IS NOT NULL AND LEFT(`{c}`, {plen}) != '{prefix}' AND LENGTH(`{c}`) > {plen}", {}

    def _handle_date_min_limit(self, t, c, v, r):
        mindate = v or "2000-01-01"
        return f"UPDATE `{t}` SET `{c}` = '{mindate}' WHERE `{c}` < '{mindate}' AND `{c}` IS NOT NULL", {}

    def _handle_abs_value(self, t, c, v, r):
        return f"UPDATE `{t}` SET `{c}` = ABS(`{c}`) WHERE `{c}` < 0", {}

    def _handle_mac_format(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = LOWER(CONCAT(
                    SUBSTRING(`{c}`,1,2),':',SUBSTRING(`{c}`,3,2),':',
                    SUBSTRING(`{c}`,5,2),':',SUBSTRING(`{c}`,7,2),':',
                    SUBSTRING(`{c}`,9,2),':',SUBSTRING(`{c}`,11,2)))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND `{c}` NOT REGEXP '^[0-9a-f]{{2}}(:[0-9a-f]{{2}}){{5}}$'""", {}

    def _handle_unique_check(self, t, c, v, r):
        return f"""SELECT COUNT(*) as duplicates FROM `{t}`
                WHERE `{c}` IN (SELECT `{c}` FROM `{t}` GROUP BY `{c}` HAVING COUNT(*) > 1)""", {}

    def _handle_mask_data(self, t, c, v, r):
        mask = v or "****-****-****-####"
        if "####" in mask:
            return f"""UPDATE `{t}` SET `{c}` = CONCAT('****-****-****-', RIGHT(`{c}`, 4))
                    WHERE `{c}` IS NOT NULL AND LENGTH(`{c}`) >= 13
                    AND `{c}` NOT LIKE '****%'""", {}
        return f"UPDATE `{t}` SET `{c}` = '***********' WHERE `{c}` IS NOT NULL AND `{c}` != ''", {}

    def _handle_remove_spaces(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = REPLACE(REPLACE(`{c}`, ' ', ''), '-', '')
                WHERE `{c}` IS NOT NULL
                AND (`{c}` LIKE '% ' OR `{c}` LIKE '%-')""", {}

    def _handle_foreign_key_check(self, t, c, v, r):
        if not v or ":" not in v: return None, {}
        fk_table_raw, fk_col = v.split(":", 1)
        fk_table_corr = SOURCE_TO_TARGET.get(fk_table_raw.strip().lower(), fk_table_raw.strip())
        return f"""SELECT COUNT(*) as fk_errors FROM `{t}`
                WHERE `{c}` IS NOT NULL
                AND CAST(`{c}` AS CHAR) NOT IN (
                    SELECT CAST(`{fk_col.strip()}` AS CHAR)
                    FROM `{fk_table_corr}` WHERE `{fk_col.strip()}` IS NOT NULL
                )""", {}

    def _handle_mandatory_field(self, t, c, v, r):
        return f"SELECT COUNT(*) as null_count FROM `{t}` WHERE `{c}` IS NULL OR TRIM(`{c}`) = ''", {}

    def _handle_cross_fill_id_ref(self, t, c, v, r):
        return None, {}

    def _handle_mapping_region(self, t, c, v, r):
        if not v: return None, {}
        parts = v.split("|"); mapping = parts[0]
        default = parts[1] if len(parts) > 1 else "Autre"
        cases = []
        for pair in mapping.split(","):
            if ":" in pair:
                k, val = pair.split(":", 1)
                cases.append(f"WHEN `{c}`='{k.strip()}' THEN '{val.strip()}'")
        if cases:
            return f"""UPDATE `{t}` SET `{c}` = CASE {' '.join(cases)} ELSE '{default}' END
                    WHERE `{c}` IS NOT NULL AND `{c}` != ''""", {}
        return None, {}

    def _handle_date_max_limit(self, t, c, v, r):
        if v and v.upper() in ("CURRENT_TIMESTAMP", "SYSDATE", "NOW()"):
            return f"UPDATE `{t}` SET `{c}` = CURDATE() WHERE `{c}` > CURDATE() AND `{c}` IS NOT NULL", {}
        maxdate = v or "9999-12-31"
        return f"UPDATE `{t}` SET `{c}` = '{maxdate}' WHERE `{c}` > '{maxdate}' AND `{c}` IS NOT NULL", {}

    def _handle_min_value(self, t, c, v, r):
        minval = v if v else "0"
        return f"UPDATE `{t}` SET `{c}` = {minval} WHERE `{c}` < {minval} AND `{c}` IS NOT NULL", {}

    def _handle_max_length(self, t, c, v, r):
        length = int(v) if v and v.isdigit() else 255
        return f"UPDATE `{t}` SET `{c}` = LEFT(`{c}`, {length}) WHERE `{c}` IS NOT NULL AND LENGTH(`{c}`) > {length}", {}

    def _handle_upper_trim_clean(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = TRIM(UPPER(REGEXP_REPLACE(`{c}`, '[^a-zA-Z0-9 ]', '')))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''""", {}

    def _handle_cross_table_check(self, t, c, v, r):
        if not v or "=" not in v: return None, {}
        left, right = v.split("=", 1)
        if ":" not in left or ":" not in right: return None, {}
        lt, lc = left.split(":", 1); rt2, rc = right.split(":", 1)
        lt = lt.strip().replace("bscs_", "corrected_")
        rt2 = rt2.strip().replace("bscs_", "corrected_")
        return f"""SELECT COUNT(*) as cross_errors FROM `{lt}` l
                LEFT JOIN `{rt2}` r ON l.`{lc.strip()}` = r.`{rc.strip()}`
                WHERE r.`{rc.strip()}` IS NULL AND l.`{lc.strip()}` IS NOT NULL""", {}

    def _handle_aggregate_check(self, t, c, v, r):
        if not v: return None, {}
        v = v.strip()
        if v.upper().startswith("SUM<=") and ":" in v:
            ref = v[5:]; ref_table, ref_col = ref.split(":", 1)
            ref_target = ref_table.replace("bscs_", "corrected_")
            return f"""SELECT t.`{c}`, SUM(t.`AMOUNT_PAID`) as total_paid, p.`{ref_col.strip()}` as max_allowed
                    FROM `{t}` t JOIN `{ref_target}` p ON t.`PAYMENT_ID` = p.`PAYMENT_ID`
                    GROUP BY t.`PAYMENT_ID`, p.`{ref_col.strip()}` HAVING total_paid > max_allowed""", {}
        if v.upper().startswith("MIN_COUNT:") and ">=" in v:
            parts = v[10:].split(">=", 1)
            tables_s = parts[0].strip(); min_cnt = int(parts[1].strip()) if parts[1].strip().isdigit() else 1
            sub_tables = [tb.strip().replace("bscs_", "corrected_") for tb in tables_s.split("+")]
            unions = " UNION ALL ".join([f"SELECT `CONTRACT_ID` FROM `{tb}`" for tb in sub_tables])
            return f"""SELECT COUNT(*) as contracts_without_resources FROM `{t}` s
                    WHERE s.`STATUS` = 'A'
                    AND (SELECT COUNT(*) FROM ({unions}) u WHERE u.`CONTRACT_ID` = s.`CONTRACT_ID`) < {min_cnt}""", {}
        if "SUM(" in v.upper() and "INITAMOUNT" in v.upper():
            return f"""SELECT f.`ID_FINDOC`, f.`INITAMOUNT`, f.`UNCLEAREDAMOUNT`,
                        COALESCE(SUM(pd.`AMOUNT_PAID`), 0) as total_paid,
                        (f.`INITAMOUNT` - COALESCE(SUM(pd.`AMOUNT_PAID`), 0)) as expected_uncleared
                    FROM `{t}` f
                    LEFT JOIN `corrected_payments_det` pd ON CAST(pd.`ID_FINDOC` AS CHAR) = CAST(f.`ID_FINDOC` AS CHAR)
                    GROUP BY f.`ID_FINDOC`, f.`INITAMOUNT`, f.`UNCLEAREDAMOUNT`
                    HAVING ABS(f.`UNCLEAREDAMOUNT` - expected_uncleared) > 0.01""", {}
        return None, {}

    def _handle_copy_from(self, t, c, v, r):
        if not v or not v.upper().startswith("COPY_FROM:"): return None, {}
        src_col = v.split(":", 1)[1].strip()
        return f"""UPDATE `{t}` SET `{c}` = `{src_col}`
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')
                AND `{src_col}` IS NOT NULL AND `{src_col}` != ''""", {}

    def _handle_default_if_invalid_domain(self, t, c, v, r):
        label = r.get("detection_rule_label", "") or r.get("rule_label", "") or ""
        domain_list = None
        if " IN " in label.upper():
            import re
            m = re.search(r'\(([^)]+)\)', label)
            if m:
                domain_list = [x.strip().strip("'") for x in m.group(1).split(",")]
        default = self._escape(v or "UNKNOWN")
        if domain_list:
            in_list = ",".join([f"'{x}'" for x in domain_list])
            return f"""UPDATE `{t}` SET `{c}` = {default}
                    WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '' OR `{c}` NOT IN ({in_list}))""", {}
        return f"""UPDATE `{t}` SET `{c}` = {default}
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '')""", {}

    def _handle_sync_from_field(self, t, c, v, r):
        if not v or ":" not in v: return None, {}
        parts = v.split(":", 1); src_col = parts[0].strip(); mapping_str = parts[1]
        cases = []
        for pair in mapping_str.split(","):
            if ":" in pair:
                k, val = pair.split(":", 1)
                cases.append(f"WHEN `{src_col}`='{k.strip()}' THEN '{val.strip()}'")
        if not cases: return None, {}
        return f"""UPDATE `{t}` SET `{c}` = CASE {' '.join(cases)} ELSE `{c}` END
                WHERE (`{c}` IS NULL OR TRIM(`{c}`) = '') AND `{src_col}` IS NOT NULL""", {}

    def _handle_correlate_date_status(self, t, c, v, r):
        if not v or ":" not in v: return None, {}
        parts = v.split(":")
        if len(parts) < 3: return None, {}
        date_col, status_ok, status_ko = parts[0].strip(), parts[1].strip(), parts[2].strip()
        return f"""UPDATE `{t}` SET `{c}` = '{status_ko}'
                WHERE `{c}` = '{status_ok}' AND `{date_col}` > CURDATE()
                AND `{date_col}` IS NOT NULL""", {}

    def _handle_prefix_force(self, t, c, v, r):
        prefix = v or "BA"; plen = len(prefix)
        return f"""UPDATE `{t}` SET `{c}` = CONCAT('{prefix}', `{c}`)
                WHERE `{c}` IS NOT NULL AND TRIM(`{c}`) != ''
                AND LEFT(UPPER(`{c}`), {plen}) != UPPER('{prefix}')""", {}

    def _handle_regex_prefix_force(self, t, c, v, r):
        if not v: return None, {}
        parts = v.split(":", 1); prefix = parts[0].strip()
        default = parts[1].strip() if len(parts) > 1 else f"{prefix}000"
        plen = len(prefix)
        return f"""UPDATE `{t}` SET `{c}` =
                CASE
                    WHEN `{c}` IS NULL OR TRIM(`{c}`) = '' THEN '{default}'
                    WHEN LEFT(UPPER(`{c}`), {plen}) != UPPER('{prefix}') AND `{c}` REGEXP '^[0-9]+$'
                        THEN CONCAT('{prefix}', `{c}`)
                    WHEN LEFT(UPPER(`{c}`), {plen}) != UPPER('{prefix}') THEN '{default}'
                    ELSE `{c}`
                END
                WHERE `{c}` IS NULL OR TRIM(`{c}`) = ''
                OR LEFT(UPPER(`{c}`), {plen}) != UPPER('{prefix}')""", {}

    def _handle_default_if_invalid_regex(self, t, c, v, r):
        import re as _re
        label = r.get("detection_rule_label", "") or r.get("rule_label", "") or ""
        pattern = None
        m = _re.search(r'\^[^\s]+\$', label)
        if m: pattern = m.group(0)
        default = "NULL" if (v or "").upper() == "NULL" else self._escape(v or "UNKNOWN")
        if pattern:
            safe_pattern = pattern.replace("'", "\\'")
            return f"""UPDATE `{t}` SET `{c}` = {default}
                    WHERE `{c}` IS NOT NULL AND TRIM(`{c}`) != ''
                    AND `{c}` NOT REGEXP '{safe_pattern}'""", {}
        return f"UPDATE `{t}` SET `{c}` = {default} WHERE `{c}` IS NULL OR TRIM(`{c}`) = ''", {}

    def _handle_not_equal_field(self, t, c, v, r):
        if not v or ":" not in v: return None, {}
        ref_col, suffix = v.split(":", 1)
        return f"""UPDATE `{t}` SET `{c}` = CONCAT(`{c}`, '{suffix.strip()}')
                WHERE `{c}` IS NOT NULL AND `{ref_col.strip()}` IS NOT NULL
                AND `{c}` = `{ref_col.strip()}`""", {}

    def _handle_min_value_strict(self, t, c, v, r):
        minval = v if v else "0.01"
        return f"UPDATE `{t}` SET `{c}` = {minval} WHERE `{c}` IS NOT NULL AND `{c}` <= 0", {}

    def _handle_min_value_or_null(self, t, c, v, r):
        minval = int(v) if v and v.isdigit() else 1
        return f"UPDATE `{t}` SET `{c}` = NULL WHERE `{c}` IS NOT NULL AND `{c}` < {minval}", {}

    def _handle_max_value(self, t, c, v, r):
        maxval = v if v else "9999"
        return f"UPDATE `{t}` SET `{c}` = {maxval} WHERE `{c}` IS NOT NULL AND `{c}` > {maxval}", {}

    def _handle_phone_prefix_force(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = CONCAT('+', REGEXP_REPLACE(
                    REGEXP_REPLACE(`{c}`, '^00', ''), '^\\\\+', ''))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND `{c}` NOT REGEXP '^\\\\+[0-9]{{7,15}}$'""", {}

    def _handle_mac_format_upper(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = UPPER(CONCAT(
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),1,2),':',
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),3,2),':',
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),5,2),':',
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),7,2),':',
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),9,2),':',
                    SUBSTRING(REPLACE(REPLACE(`{c}`,':',''),'-',''),11,2)))
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                AND `{c}` NOT REGEXP '^([0-9A-F]{{2}}[:-]){{5}}[0-9A-F]{{2}}$'""", {}

    def _handle_regex_replace(self, t, c, v, r):
        return f"""UPDATE `{t}` SET `{c}` = DATE_FORMAT(CURDATE(), '%Y%m')
                WHERE `{c}` IS NOT NULL
                AND `{c}` NOT REGEXP '^(2024|2025|2026)(0[1-9]|1[0-2])$'""", {}

    # ── Alias ────────────────────────────────────────────────────────────────
    _handle_uppercase                          = _handle_upper_trim
    _handle_trim                               = _handle_upper_trim
    _handle_domain_force                       = _handle_force_value
    _handle_regex_clean                        = _handle_alpha_num_only
    _handle_extract_number_from_text           = _handle_alpha_num_only
    _handle_generate_ref_from_id              = _handle_fixed_length
    _handle_clean_ref_format                   = _handle_fixed_length
    _handle_default_if_date_out_of_range_alias = _handle_default_if_date_out_of_range
    _handle_correlate                          = _handle_correlate_date_status
    _handle_gt_if_nn                           = _handle_min_value_or_null
    _handle_both                               = _handle_mandatory_field
    _handle_notnull_if                         = _handle_mandatory_field
    _handle_length                             = _handle_max_length
    _handle_or                                 = _handle_mandatory_field
    _handle_in                                 = _handle_default_if_invalid_domain

    def _cross_fill_findocs(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` SET `REFERENCE` = CONCAT('REF', REGEXP_SUBSTR(`REFERENCE`, '[0-9]+'))
                WHERE `REFERENCE` IS NOT NULL AND `REFERENCE` != ''
                AND (`REFERENCE` NOT REGEXP '^REF[0-9]+$' OR `REFERENCE` REGEXP '^FINV[0-9]+$' OR LENGTH(`REFERENCE`) > 10)""")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` SET `ID_FINDOC` = CAST(REGEXP_SUBSTR(`REFERENCE`, '[0-9]+') AS SIGNED)
                WHERE (`ID_FINDOC` IS NULL OR `ID_FINDOC` = 0) AND `REFERENCE` REGEXP '^REF[0-9]+'""")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` t
                SET t.`REFERENCE` = CONCAT('REF', LPAD(CAST(t.`ID_FINDOC` AS UNSIGNED), 3, '0'))
                WHERE (t.`REFERENCE` IS NULL OR TRIM(t.`REFERENCE`) = '')
                AND t.`ID_FINDOC` IS NOT NULL AND CAST(t.`ID_FINDOC` AS UNSIGNED) > 0
                AND (SELECT COUNT(*) FROM (SELECT `ID_FINDOC` FROM `{table}`) AS sub WHERE sub.`ID_FINDOC` = t.`ID_FINDOC`) = 1""")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` SET `REFERENCE` = CONCAT('REF', REGEXP_SUBSTR(`REFERENCE`, '[0-9]+'))
                WHERE `REFERENCE` REGEXP '^FINV[0-9]+$'""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [FINDOCS] Cross-fill terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [FINDOCS] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_payments_det(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` pd JOIN `corrected_payments` p ON pd.`PAYMENT_ID` = p.`PAYMENT_ID`
                SET pd.`CUSTOMER_ID` = p.`CUSTOMER_ID`
                WHERE pd.`CUSTOMER_ID` != p.`CUSTOMER_ID` AND p.`CUSTOMER_ID` IS NOT NULL""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [PAYMENTS_DET] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [PAYMENTS_DET] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_services_resources(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` s JOIN `corrected_billing_account` ba ON s.`BILLING_ACCOUNT_ID` = ba.`BILLING_ACCOUNT_ID`
                SET s.`CUSTOMER_ID` = ba.`CUSTOMER_ID`
                WHERE s.`CUSTOMER_ID` != ba.`CUSTOMER_ID` AND ba.`CUSTOMER_ID` IS NOT NULL""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [SERVICES] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [SERVICES] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_billing_account_services(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` SET `INVOICING_IND` = 'Y'
                WHERE `BILLING_ACCOUNT_PRIMAIRE` = 'O'
                AND (`INVOICING_IND` IS NULL OR `INVOICING_IND` != 'Y')""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [BILLING_ACCOUNT] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [BILLING_ACCOUNT] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_memos(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` m JOIN `corrected_customer` c ON m.`CUSTOMER_ID` = c.`CUSTOMER_ID`
                SET m.`CREATED_DATE` = c.`VALIDFROM`
                WHERE m.`CREATED_DATE` < c.`VALIDFROM` AND m.`CREATED_DATE` IS NOT NULL AND c.`VALIDFROM` IS NOT NULL""")
            mod += cur.rowcount
            cur.execute(f"UPDATE `{table}` SET `CREATED_DATE` = NOW() WHERE `CREATED_DATE` > NOW() AND `CREATED_DATE` IS NOT NULL")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [MEMOS] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [MEMOS] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_place(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` p JOIN `corrected_customer` c ON p.`CUSTOMER_ID` = c.`CUSTOMER_ID`
                SET p.`COUNTRY` = c.`COUNTRY`
                WHERE p.`COUNTRY` != c.`COUNTRY` AND c.`COUNTRY` IS NOT NULL""")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` SET `UPDATEFROM` = `VALIDFROM`
                WHERE `UPDATEFROM` < `VALIDFROM` AND `UPDATEFROM` IS NOT NULL AND `VALIDFROM` IS NOT NULL""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [PLACE] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [PLACE] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_resource_sim(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"UPDATE `{table}` SET `PIN2` = '0001' WHERE `PIN1` IS NOT NULL AND `PIN2` IS NOT NULL AND `PIN1` = `PIN2`")
            mod += cur.rowcount
            cur.execute(f"UPDATE `{table}` SET `PUK2` = '00000001' WHERE `PUK1` IS NOT NULL AND `PUK2` IS NOT NULL AND `PUK1` = `PUK2`")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` sim JOIN `corrected_services` s ON sim.`CONTRACT_ID` = s.`CONTRACT_ID`
                SET sim.`CUSTOMER_ID` = s.`CUSTOMER_ID`
                WHERE sim.`CUSTOMER_ID` != s.`CUSTOMER_ID` AND s.`CUSTOMER_ID` IS NOT NULL""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [SIM] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [SIM] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

    def _cross_check_resource_port(self, hook, table):
        conn = hook.get_conn(); cur = conn.cursor(); mod = 0
        try:
            cur.execute(f"""UPDATE `{table}` SET `MACADDRESS` = '00:00:00:00:00:00'
                WHERE (`IMEI` IS NULL OR TRIM(`IMEI`) = '') AND (`MACADDRESS` IS NULL OR TRIM(`MACADDRESS`) = '')""")
            mod += cur.rowcount
            cur.execute(f"""UPDATE `{table}` p JOIN `corrected_services` s ON p.`CONTRACT_ID` = s.`CONTRACT_ID`
                SET p.`CUSTOMER_ID` = s.`CUSTOMER_ID`
                WHERE p.`CUSTOMER_ID` != s.`CUSTOMER_ID` AND s.`CUSTOMER_ID` IS NOT NULL""")
            mod += cur.rowcount
            conn.commit()
            logger.info("  [PORT] Cross-check terminé : %d mods", mod)
        except Exception as e:
            conn.rollback(); logger.error("  [PORT] ❌ %s", e); raise
        finally:
            cur.close(); conn.close()
        return mod

# ← FIN DE LA CLASSE — retour au niveau 0
def etl_table(table_name: str, **context) -> dict:
    ti = context["ti"]
    config = ti.xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    rules = [r for r in config["rules"] if r["table_name"] == table_name]
    target = SOURCE_TO_TARGET.get(table_name)

    if not rules:
        return {"table": table_name, "status": "NO_RULES", "rows_loaded": 0}
    if not target:
        return {"table": table_name, "status": "NO_MAPPING", "rows_loaded": 0}

    detection_run_id        = config["detection_run_id"]
    iteration_id            = config["iteration_id"]         
    detection_iteration_id = config.get("detection_iteration_id")   
    
    # ✅ Utiliser detection_iteration_id pour lire les PKs du batch de détection
    pk_rows = _source_hook().get_records(
        "SELECT pk_value FROM bscs_detection_batch_pks "
        "WHERE run_id=%s AND table_name=%s AND iteration_id=%s",  
        [detection_run_id, table_name.lower(), detection_iteration_id]
    )

    if not pk_rows:
        return {"table": table_name, "status": "NO_BATCH_PKS_FOUND", "rows_loaded": 0}

    pk_row = _source_hook().get_first(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_KEY='PRI' LIMIT 1",
        [table_name]
    )
    if not pk_row:
        return {"table": table_name, "status": "NO_PK", "rows_loaded": 0}
    pk_col = pk_row[0]

    #  Utiliser directement les PKs de bscs_detection_batch_pks
    batch_pks    = [r[0] for r in pk_rows]
    pk_set_batch = set(str(p) for p in batch_pks)

    if not batch_pks:
        return {"table": table_name, "status": "EMPTY_BATCH", "rows_loaded": 0}
    pk_set_problematiques = set()
    try:
        ph = ",".join(["%s"] * len(batch_pks))
        pk_prob_rows = _source_hook().get_records(
            f"SELECT DISTINCT row_pk_value FROM bscs_problematic_rows "
            f"WHERE run_id=%s AND table_name=%s AND row_pk_value IN ({ph})",
            [detection_run_id, table_name] + [str(p) for p in batch_pks]
        )
        pk_set_problematiques = {r[0] for r in pk_prob_rows}
    except Exception as e:
        logger.error("[%s] Erreur récupération PKs problématiques : %s", table_name, e)


    pk_err = pk_set_problematiques & pk_set_batch
    pk_ok  = pk_set_batch - pk_err
    pks_err = list(pk_err)
    pks_ok  = list(pk_ok)

    logger.info(
        "📊 [%s] Batch=%d | Problématiques=%d | Saines=%d",
        table_name, len(batch_pks), len(pks_err), len(pks_ok)
    )

    src_eng = _source_hook().get_sqlalchemy_engine()
    df_err = pd.DataFrame()
    df_ok  = pd.DataFrame()

    try:
        with src_eng.connect() as conn:
            tmp_err = f"_tmp_err_{table_name}"
            tmp_ok  = f"_tmp_ok_{table_name}"

            conn.execute(text(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS `{tmp_err}` (pk VARCHAR(255) PRIMARY KEY)"
            ))
            conn.execute(text(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS `{tmp_ok}` (pk VARCHAR(255) PRIMARY KEY)"
            ))

            if pks_err:
                conn.execute(
                    text(f"INSERT IGNORE INTO `{tmp_err}` (pk) VALUES (:pk)"),
                    [{"pk": str(pk)} for pk in pks_err]
                )
                df_err = pd.read_sql(
                    f"SELECT s.* FROM `{table_name}` s "
                    f"INNER JOIN `{tmp_err}` t ON CAST(s.`{pk_col}` AS CHAR) = t.pk",
                    conn
                )

            if pks_ok:
                conn.execute(
                    text(f"INSERT IGNORE INTO `{tmp_ok}` (pk) VALUES (:pk)"),
                    [{"pk": str(pk)} for pk in pks_ok]
                )
                df_ok = pd.read_sql(
                    f"SELECT s.* FROM `{table_name}` s "
                    f"INNER JOIN `{tmp_ok}` t ON CAST(s.`{pk_col}` AS CHAR) = t.pk",
                    conn
                )

            conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{tmp_err}`"))
            conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{tmp_ok}`"))

    except Exception as e:
        logger.error("[%s] Erreur extraction source : %s", table_name, e)
        return {"table": table_name, "status": f"ERROR_EXTRACTION: {e}", "rows_loaded": 0}

    total_err = len(df_err)
    total_ok  = len(df_ok)
    total     = total_err + total_ok

    if total == 0:
        return {"table": table_name, "status": "EMPTY", "rows_loaded": 0}

    logger.info(
        "✅ [%s] Extraction : %d err + %d ok = %d total",
        table_name, total_err, total_ok, total
    )

    df_all = pd.concat([df_err, df_ok], ignore_index=True)

    # Dédoublonnage sur PK
    if pk_col in df_all.columns:
        before = len(df_all)
        df_all = df_all.drop_duplicates(subset=[pk_col], keep="first")
        if len(df_all) != before:
            logger.warning(
                "⚠️ [%s] %d doublons supprimés sur PK '%s'",
                target, before - len(df_all), pk_col
            )

    # Correction ID_FINDOC NULL si applicable
    if "ID_FINDOC" in df_all.columns:
        null_count = df_all["ID_FINDOC"].isna().sum()
        if null_count > 0:
            logger.warning("⚠️ [%s] %d ID_FINDOC NULL → correction automatique", target, null_count)
            if "PAYMENT_ID" in df_all.columns:
                mask = df_all["ID_FINDOC"].isna()
                extracted = df_all.loc[mask, "PAYMENT_ID"].astype(str).str.extract(r"(\d+)", expand=False)
                df_all.loc[mask, "ID_FINDOC"] = extracted.astype(float).fillna(0).astype(int)
            elif "REFERENCE" in df_all.columns:
                mask = df_all["ID_FINDOC"].isna() & df_all["REFERENCE"].notna()
                extracted = df_all.loc[mask, "REFERENCE"].astype(str).str.extract(r"(\d+)", expand=False)
                df_all.loc[mask, "ID_FINDOC"] = extracted.astype(float).fillna(0).astype(int)
            else:
                df_all["ID_FINDOC"] = df_all["ID_FINDOC"].fillna(0).astype(int)

    df_all = df_all.where(pd.notnull(df_all), other=None)
    stg_eng = _staging_hook().get_sqlalchemy_engine()
    iteration_id = config["iteration_id"]

    if batch_pks:
        stg_conn = _staging_hook().get_conn()
        stg_cur  = stg_conn.cursor()
        try:
            ph_del = ",".join(["%s"] * len(batch_pks))

            # ✅ DELETE sans filtre iteration_id → nettoie TOUTES les versions
            # précédentes de ces PKs et garantit l'absence de doublon
            stg_cur.execute(
                f"DELETE FROM `{target}` WHERE `{pk_col}` IN ({ph_del})",
                [str(p) for p in batch_pks]  # ✅ PAS de iteration_id ici
            )
            stg_conn.commit()
            logger.info(
                "[%s] DELETE itération=%s — %d PKs supprimés",
                target, iteration_id, stg_cur.rowcount
            )
        except Exception as e:
            stg_conn.rollback()
            logger.error("[%s] Erreur DELETE batch : %s", target, e)
            raise
        finally:
            stg_cur.close()
            stg_conn.close()

    if not df_all.empty:
        try:
            # Ajouter iteration_id pour traçabilité
            df_all["iteration_id"] = iteration_id
            
            # Conversion PK en string
            df_all[pk_col] = df_all[pk_col].astype(str)
            
            # ✅ INSERT direct - le DELETE précédent garantit l'absence de doublon
            df_all.to_sql(
                name=target,
                con=stg_eng,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi"
            )
            
            logger.info(
                "[%s] INSERT %d lignes (iter=%s)",
                target, len(df_all), iteration_id
            )

        except Exception as e:
            logger.error("[%s] Erreur insertion staging : %s", target, e)
            return {"table": table_name, "status": f"ERROR_INSERT: {e}", "rows_loaded": 0}
    engine  = CompleteSqlRuleEngine()
    hook    = _staging_hook()
    results = []
    for rule in rules:
        res = engine.apply_rule(hook, target, rule, pk_col=pk_col, batch_pks=batch_pks)
        results.append(res)

    special   = engine.apply_special_rules(hook, target)
    total_sql = sum(r["rows_affected"] for r in results) + special

    logger.info(
        "📊 [%s]→[%s] | Batch:%d | Err:%d | Ok:%d | Règles:%d | SQL mods:%d",
        table_name, target, total, total_err, total_ok, len(results), total_sql
    )

    return {
        "table":             table_name,
        "target":            target,
        "extracted":         total,
        "problematic_rows":  total_err,
        "healthy_rows":      total_ok,
        "sql_rules":         len(results),
        "sql_modifications": total_sql,
        "special":           special,
        "details":           results,
        "stats":             engine.stats,
    } 

def load_iteration_config(**context) -> None:
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    iteration_id = conf.get("iteration_id")
    detection_run_id = conf.get("detection_run_id")

    if not iteration_id:
        raise ValueError("'iteration_id' manquant.")

    if not detection_run_id:
        row = _source_hook().get_first(
            """SELECT detection_run_id 
               FROM correction_iteration_config 
               WHERE iteration_id=%s AND to_execute='YES' 
               LIMIT 1""",
            [iteration_id]
        )
        detection_run_id = row[0] if row and row[0] else None

    if not detection_run_id:
        raise ValueError(f"Aucun detection_run_id pour iter={iteration_id}")

    sql = """
        SELECT cic.id, cic.rule_id,
               cic.iteration_id, cic.created_by,
               bmr.source_table, bmr.source_column,
               bmr.rule_type, bmr.default_value,
               cic.detection_rule_label, cic.target_column
        FROM correction_iteration_config cic
        JOIN bss_migration_rules bmr ON cic.rule_id = bmr.rule_id
        WHERE cic.iteration_id = %s AND cic.to_execute = 'YES'
        ORDER BY bmr.source_table, bmr.rule_id
    """
    rows = _source_hook().get_records(sql, [iteration_id])

    if not rows:
        raise ValueError(f"Aucune règle active pour iter={iteration_id}")

    det_iter = _source_hook().get_first(
        """SELECT DISTINCT iteration_id 
           FROM bscs_detected_inconsistency 
           WHERE run_id=%s LIMIT 1""",
        [detection_run_id]
    )
    detection_iteration_id = det_iter[0] if det_iter else None

    rules = []
    for r in rows:
        rules.append({
            "config_id":             r[0],
            "rule_id":               r[1],
            "iteration_id":          r[2],
            "created_by":            r[3],
            "table_name":            r[4],
            "column_name":           r[5],
            "rule_type":             r[6],
            "rule_value":            r[7],
            "detection_rule_label":  r[8],
            "target_column":         r[9],
        })

    config = {
        "iteration_id":          int(iteration_id),
        "detection_run_id":      detection_run_id,
        "detection_iteration_id": detection_iteration_id,
        "rules":                 rules,
        "tables_involved":       sorted(set(r["table_name"] for r in rules))
    }

    context["ti"].xcom_push(key="iteration_config", value=config)
    logger.info(
        "✅ Config OK — iter=%s | run=%s | detection_iter=%s | %d règles",
        iteration_id, detection_run_id, detection_iteration_id, len(rules)
    )    
def generate_report(**context) -> None:
    c = context["ti"].xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    te = 0
    tl = 0
    summary = []
    for tbl in sorted(SOURCE_TO_TARGET.keys()):
        r = context["ti"].xcom_pull(task_ids=f"etl_tables.etl_{tbl}") or {}
        e = r.get("extracted", 0)
        te += e
        tl += r.get("sql_modifications", 0)
        if e > 0:
            summary.append((c["iteration_id"], context["run_id"], tbl, e, r.get("sql_modifications", 0)))

    logger.info("=== BILAN SQL === iter=%s | ext=%d | sql_mods=%d", c["iteration_id"], te, tl)
    
    if summary:
        h = _staging_hook()
        cn = h.get_conn()
        cu = cn.cursor()
        cu.executemany(
            "INSERT INTO migration_run_summary(iteration_id, dag_run_id, table_name, rows_extracted, rows_loaded, run_at) VALUES(%s, %s, %s, %s, %s, NOW())",
            [(row[0], row[1], row[2], row[3], row[3]) for row in summary]
        )
        cn.commit()
        cu.close()

    try:
        src_hook = _source_hook()
        src_cn = src_hook.get_conn()
        src_cu = src_cn.cursor()
        src_cu.execute(
        "UPDATE bscs_iteration_config SET has_correction = 1 WHERE iteration_id = %s",
        [c["detection_iteration_id"]]  # 👈 au lieu de c["iteration_id"]
        )
        src_cn.commit()
        src_cu.close()
        src_cn.close()
        logger.info("✅ has_correction=1 pour detection_iter=%s", c["detection_iteration_id"])
    except Exception as e:
        logger.error("❌ Erreur UPDATE has_correction: %s", e)

    context["ti"].xcom_push(key="detection_run_id", value=c.get("detection_run_id"))
    context["ti"].xcom_push(key="detection_iteration_id", value=c.get("detection_iteration_id"))
    context["ti"].xcom_push(key="iteration_id", value=c.get("iteration_id"))
with DAG(
    dag_id="bss_migration_etl",
    description="ETL Migration BSS ",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bss", "migration", "etl", "sql-complete"],
    params={"iteration_id": 1, "detection_run_id": None},
) as dag:

    t_load = PythonOperator(
        task_id="load_iteration_config",
        python_callable=load_iteration_config
    )

    with TaskGroup(group_id="etl_tables") as tg:
        for tbl in sorted(SOURCE_TO_TARGET.keys()):
            PythonOperator(
                task_id=f"etl_{tbl}",
                python_callable=etl_table,
                op_kwargs={"table_name": tbl}
            )

    t_rpt = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report
    )

    t_trig = TriggerDagRunOperator(
        task_id="trigger_validation",
        trigger_dag_id="post_correction_detection",
        conf={
            "detection_run_id": (
                "{{ ti.xcom_pull("
                "task_ids='generate_report', key='detection_run_id') }}"
            ),
            "detection_iteration_id": (
                "{{ ti.xcom_pull("
                "task_ids='generate_report', key='detection_iteration_id') }}"
            ),
            "iteration_id": (
                "{{ ti.xcom_pull("
                "task_ids='generate_report', key='iteration_id') }}"
            ),
        },
        wait_for_completion=False
    )

    t_load >> tg >> t_rpt >> t_trig