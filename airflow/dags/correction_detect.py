from __future__ import annotations


import logging
import re
from datetime import datetime, timedelta
from typing import Any

import pymysql
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner":            "migration_team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

VIOLATION_FETCH_LIMIT = 500

SOURCE_TO_CORRECTED: dict[str, str] = {
    "bscs_billing_account":        "corrected_billing_account",
    "bscs_billing_account_assign": "corrected_billing_account_assign",
    "bscs_charge":                 "corrected_charge",
    "bscs_customer":               "corrected_customer",
    "bscs_findocs":                "corrected_findocs",
    "bscs_payments":               "corrected_payments",
    "bscs_payments_det":           "corrected_payments_det",
    "bscs_place":                  "corrected_place",
    "bscs_resource_directory":     "corrected_resource_directory",
    "bscs_resource_port":          "corrected_resource_port",
    "bscs_resource_sim":           "corrected_resource_sim",
    "bscs_services":               "corrected_services",
}
CORRECTED_TABLE_PKS: dict[str, str] = {
    "corrected_billing_account":        "BILLING_ACCOUNT_ID",
    "corrected_billing_account_assign": "BILLING_ACCOUNT_ID",
    "corrected_charge":                 "CO_ID",
    "corrected_customer":               "CUSTOMER_ID",
    "corrected_findocs":                "ID_FINDOC",
    "corrected_payments":               "PAYMENT_ID",
    "corrected_payments_det":           "ID",
    "corrected_place":                  "PLACE_ID",
    "corrected_resource_directory":     "ID_RESOURCE",
    "corrected_resource_port":          "ID_RESOURCE",
    "corrected_resource_sim":           "ID_RESOURCE",
    "corrected_services":               "ID_SERVICE",
}
def _get_conn_info() -> dict:
    hook = MySqlHook(mysql_conn_id="mysql_conn")
    c    = hook.get_connection("mysql_conn")
    return {
        "host":     c.host,
        "user":     c.login,
        "password": c.password,
        "database": c.schema,
        "port":     int(c.port or 3306),
        "charset":  "utf8mb4",
    }

def _new_conn(info: dict):
    return pymysql.connect(**info)

def _staging_conn_info() -> dict:
    hook = MySqlHook(mysql_conn_id="mysql_bss_staging")
    c    = hook.get_connection("mysql_bss_staging")
    return {
        "host":     c.host,
        "user":     c.login,
        "password": c.password,
        "database": c.schema,
        "port":     int(c.port or 3306),
        "charset":  "utf8mb4",
    }


def _ph(lst: list) -> str:
    """Génère les placeholders %s pour une liste."""
    return ",".join(["%s"] * len(lst))


def _pk_where(pk_col: str, pks: list, alias: str = "") -> tuple[str, list]:
    """Clause WHERE limitée aux PKs du batch, avec alias optionnel."""
    if not pks:
        return "1=0", []
    pfx = f"`{alias}`." if alias else ""
    return f"{pfx}`{pk_col}` IN ({_ph(pks)})", list(pks)


def _table_corrected(table_name: str) -> str:
    
    return SOURCE_TO_CORRECTED.get(table_name, table_name)


def _rewrite_sql_for_corrected(sql: str) -> str:
   
    for src, corr in SOURCE_TO_CORRECTED.items():
        # On remplace `bscs_xxx` (avec ou sans backticks) par `corrected_xxx`
        sql = re.sub(
            r'`?' + re.escape(src) + r'`?',
            f"`{corr}`",
            sql
        )
    return sql
def _make_result(cnt_sql, ids_sql, params, ids_params, label, category):
    return {
        "cnt_sql":    cnt_sql,
        "ids_sql":    ids_sql,
        "params":     params,
        "ids_params": ids_params,
        "label":      label,
        "category":   category,
    }
def _notnull(table, col, pk_col, pks, label=None, extra_cond=None):
    bw, bp = _pk_where(pk_col, pks)
    parts  = [f"(`{col}` IS NULL OR TRIM(CAST(`{col}` AS CHAR)) = '')"]
    if extra_cond:
        parts.append(extra_cond)
    parts.append(bw)
    w = " AND ".join(parts)
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col} IS_NOT_NULL", "COMPLETUDE"
    )
def _in_domain(table, col, pk_col, pks, values_str, label=None):
    vals   = [v.strip() for v in values_str.split(",")]
    ph_v   = _ph(vals)
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` NOT IN ({ph_v})"
    w      = f"({w_base}) AND {bw}"
    p      = vals + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        p, p,
        label or f"{col} IN ({values_str[:60]})", "CONFORMITE_DOMAINE"
    )


def _regex(table, col, pk_col, pks, pattern, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` != '' AND `{col}` NOT REGEXP %s"
    w      = f"({w_base}) AND {bw}"
    p      = [pattern] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        p, p,
        label or f"{col} REGEX {pattern[:60]}", "CONFORMITE_FORMAT"
    )


def _like(table, col, pk_col, pks, pattern, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` != '' AND `{col}` NOT LIKE %s"
    w      = f"({w_base}) AND {bw}"
    p      = [pattern] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        p, p,
        label or f"{col} LIKE {pattern}", "CONFORMITE_FORMAT"
    )


def _compare_cols(table, col1, op, col2, pk_col, pks,
                  label=None, category="COHERENCE_TEMPORELLE", null_check=True):
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<=", "=": "!=", "!=": "="}
    inv_op = inv[op]
    bw, bp = _pk_where(pk_col, pks)
    nc     = f"`{col1}` IS NOT NULL AND `{col2}` IS NOT NULL AND " if null_check else ""
    w_base = f"{nc}`{col1}` {inv_op} `{col2}`"
    w      = f"({w_base}) AND {bw}"
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col1} {op} {col2}", category
    )


def _compare_val(table, col, op, val, pk_col, pks,
                 label=None, category="VALIDITE_NUMERIQUE"):
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<=", "=": "!=", "!=": "="}
    inv_op = inv[op]
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` {inv_op} %s"
    w      = f"({w_base}) AND {bw}"
    p      = [val] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        p, p,
        label or f"{col} {op} {val}", category
    )


def _compare_curdate(table, col, op, pk_col, pks,
                     label=None, category="VALIDITE_TEMPORELLE"):
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<="}
    inv_op = inv[op]
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` {inv_op} CURDATE()"
    w      = f"({w_base}) AND {bw}"
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col} {op} CURDATE()", category
    )


def _both(table, col1, col2, pk_col, pks, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = (
        f"(`{col1}` IS NOT NULL AND TRIM(CAST(`{col1}` AS CHAR)) != '' "
        f"AND (`{col2}` IS NULL OR TRIM(CAST(`{col2}` AS CHAR)) = '')) "
        f"OR (`{col2}` IS NOT NULL AND TRIM(CAST(`{col2}` AS CHAR)) != '' "
        f"AND (`{col1}` IS NULL OR TRIM(CAST(`{col1}` AS CHAR)) = ''))"
    )
    w = f"({w_base}) AND {bw}"
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col1} BOTH {col2}", "COMPLETUDE"
    )


def _or_notnull(table, col1, col2, pk_col, pks, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = (
        f"(`{col1}` IS NULL OR TRIM(CAST(`{col1}` AS CHAR)) = '') "
        f"AND (`{col2}` IS NULL OR TRIM(CAST(`{col2}` AS CHAR)) = '')"
    )
    w = f"({w_base}) AND {bw}"
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col1} OR {col2} NOT NULL", "COMPLETUDE"
    )


def _notnull_if(table, col, cond_col, pk_col, pks, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = (
        f"(`{cond_col}` IS NOT NULL AND TRIM(CAST(`{cond_col}` AS CHAR)) != '') "
        f"AND (`{col}` IS NULL OR TRIM(CAST(`{col}` AS CHAR)) = '')"
    )
    w = f"({w_base}) AND {bw}"
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"{col} NOTNULL IF {cond_col} NOT NULL", "COMPLETUDE"
    )


def _gt_if_nn(table, col, val, pk_col, pks, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = f"`{col}` IS NOT NULL AND `{col}` <= %s"
    w      = f"({w_base}) AND {bw}"
    p      = [val] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        p, p,
        label or f"{col} > {val} IF NOT NULL", "VALIDITE_NUMERIQUE"
    )


def _length_lt(table, col1, col2, pk_col, pks, label=None):
    bw, bp = _pk_where(pk_col, pks)
    w_base = (
        f"`{col1}` IS NOT NULL AND `{col2}` IS NOT NULL "
        f"AND LENGTH(`{col1}`) >= LENGTH(`{col2}`)"
    )
    w = f"({w_base}) AND {bw}"
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
        bp, bp,
        label or f"LENGTH({col1}) < LENGTH({col2})", "COHERENCE_SEMANTIQUE"
    )

def _correlate_ba_statut_valid_from(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("`BA_VERS_STATUT` = 'ACTIVE' AND `BA_VERS_VALID_FROM` IS NOT NULL "
         "AND `BA_VERS_VALID_FROM` > CURDATE()")
    w = f"({w}) AND {bw}"
    t = "`corrected_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_bill_medium_desc(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("`BILL_MEDIUM` IS NOT NULL AND `BILL_MEDIUM_DESC` IS NOT NULL "
         "AND UPPER(TRIM(`BILL_MEDIUM_DESC`)) != UPPER(TRIM(`BILL_MEDIUM`))")
    w = f"({w}) AND {bw}"
    t = "`corrected_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_currency_desc(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("`CURRENCY_ID` IS NOT NULL AND `CURRENCY_DESC` IS NOT NULL "
         "AND UPPER(TRIM(`CURRENCY_DESC`)) != UPPER(TRIM(`CURRENCY_ID`))")
    w = f"({w}) AND {bw}"
    t = "`corrected_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_exempt(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("(`EXEMPT_STATUS` = 'FULL_EXEMPT' AND (`EXEMPT_RATE` IS NULL OR `EXEMPT_RATE` != 0)) "
         "OR (`EXEMPT_STATUS` = 'PARTIAL_EXEMPT' AND (`EXEMPT_RATE` IS NULL OR `EXEMPT_RATE` <= 0))")
    w = f"({w}) AND {bw}"
    t = "`corrected_customer_tax_exempt`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_findocs_currency(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("`CURRENCYINITAMOUNT` IS NOT NULL AND `INITAMOUNT` IS NOT NULL "
         "AND `EXCHANGERATEVALUE` IS NOT NULL "
         "AND ABS(`CURRENCYINITAMOUNT` - `INITAMOUNT` * `EXCHANGERATEVALUE`) > 0.01")
    w = f"({w}) AND {bw}"
    t = "`corrected_findocs`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_findocs_doctype(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("(`DOCTYPE` = 'IN' AND `DOCTYPE_DET` IS NOT NULL AND `DOCTYPE_DET` < 0) "
         "OR (`DOCTYPE` = 'CR' AND `DOCTYPE_DET` IS NOT NULL AND `DOCTYPE_DET` > 0)")
    w = f"({w}) AND {bw}"
    t = "`corrected_findocs`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_sim_pin_puk(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("(`PIN1` IS NOT NULL AND `PIN1` != '' AND (`PUK1` IS NULL OR TRIM(`PUK1`) = '')) "
         "OR (`PUK1` IS NOT NULL AND `PUK1` != '' AND (`PIN1` IS NULL OR TRIM(`PIN1`) = ''))")
    w = f"({w}) AND {bw}"
    t = "`corrected_resource_sim`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _correlate_payment_plan_status(pk, pks):
    bw, bp = _pk_where(pk, pks)
    w = ("(`INSTALLEMENT_STATUS` = 'OVERDUE' AND "
         "(`INSTALLMENET_DUE_DATE` IS NULL OR `INSTALLMENET_DUE_DATE` >= CURDATE())) "
         "OR (`INSTALLEMENT_STATUS` = 'PAID' AND "
         "(`INSTALLMENET_DUE_DATE` IS NULL OR `INSTALLMENET_DUE_DATE` > CURDATE()))")
    w = f"({w}) AND {bw}"
    t = "`ixc_payment_plan`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp)


def _sum_payments_vs_findocs_corrected(pk, pks):
    bw, bp = _pk_where(pk, pks, alias="pd")
    bw_clause = f" AND {bw}" if bw else ""
    cnt_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT pd.`ID_FINDOC`
            FROM `corrected_payments_det` pd
            INNER JOIN `corrected_findocs` f
                ON CAST(pd.`ID_FINDOC` AS CHAR) = CAST(f.`ID_FINDOC` AS CHAR)
            WHERE f.`INITAMOUNT` IS NOT NULL{bw_clause}
            GROUP BY pd.`ID_FINDOC`, f.`INITAMOUNT`
            HAVING SUM(pd.`AMOUNT_PAID`) > f.`INITAMOUNT`
        ) AS _agg
    """
    ids_sql = f"""
        SELECT pd.`{pk}`
        FROM `corrected_payments_det` pd
        INNER JOIN `corrected_findocs` f
            ON CAST(pd.`ID_FINDOC` AS CHAR) = CAST(f.`ID_FINDOC` AS CHAR)
        WHERE f.`INITAMOUNT` IS NOT NULL{bw_clause}
          AND pd.`ID_FINDOC` IN (
              SELECT sub.`ID_FINDOC`
              FROM `corrected_payments_det` sub
              INNER JOIN `corrected_findocs` sf
                  ON CAST(sub.`ID_FINDOC` AS CHAR) = CAST(sf.`ID_FINDOC` AS CHAR)
              WHERE sf.`INITAMOUNT` IS NOT NULL
              GROUP BY sub.`ID_FINDOC`, sf.`INITAMOUNT`
              HAVING SUM(sub.`AMOUNT_PAID`) > sf.`INITAMOUNT`
          )
        LIMIT {VIOLATION_FETCH_LIMIT}
    """
    return cnt_sql, ids_sql, bp, bp

SOURCE_DB_NAME = "app"
REF_TABLES = {
    "map_currency": f"`{SOURCE_DB_NAME}`.`map_currency`",
    "map_country":  f"`{SOURCE_DB_NAME}`.`map_country`",
}


def load_correction_context(**context) -> None:
    dag_run  = context["dag_run"]
    conf     = dag_run.conf or {}

    detection_run_id = conf.get("detection_run_id")
    iteration_id     = conf.get("detection_iteration_id")


    if not detection_run_id:
        raise ValueError("❌ 'detection_run_id' manquant")
    if not iteration_id:
        raise ValueError("❌ 'detection_iteration_id' manquant")

    try:
        iteration_id = int(iteration_id)
    except (ValueError, TypeError) as e:
        raise ValueError(f"❌ iteration_id non convertible : {e}")

    # ✅ Créer conn et cur ICI, avant tout usage
    conn_info = _get_conn_info()
    conn      = _new_conn(conn_info)
    cur       = conn.cursor()

    try:
        
        logger.info("📋 detection_run_id=%s | iteration_id=%s", 
                    detection_run_id, iteration_id)

        # ✅ REMPLACER PAR
        cur.execute("""
            SELECT
                bdi.rule_id,
                bdi.rule_origin,
                bdi.table_name,
                bdi.column_name,
                COALESCE(ov.check_type,   'NOCHECK') AS check_type,
                COALESCE(ov.check_params, '')         AS check_params,
                COALESCE(
                    ov.corrected_table,
                    CONCAT('corrected_', REPLACE(bdi.table_name, 'bscs_', ''))
                )                                     AS target_table,
                COALESCE(ov.corrected_column, bdi.column_name) AS target_column,
                (ov.id IS NULL OR ov.active = 0)      AS no_override
            FROM bscs_detected_inconsistency bdi
            LEFT JOIN post_correction_rules_override ov
                ON  ov.detection_rule_id     = bdi.rule_id
                AND ov.detection_rule_origin = bdi.rule_origin
                AND ov.source_table          = bdi.table_name
                AND ov.source_column         = bdi.column_name
                AND ov.active                = 1
            WHERE bdi.run_id = %s
            ORDER BY bdi.table_name, bdi.rule_id
        """, (detection_run_id,))
        check_rows = cur.fetchall()

        if not check_rows:
            raise ValueError(
                f"❌ Aucune inconsistance détectée pour run_id={detection_run_id}"
            )

        rules = []
        for row in check_rows:
            no_override = bool(row[8])
            check_type  = "NOCHECK" if no_override else (row[4] or "NOCHECK")
            rules.append({
                "detection_rule_id": row[0],
                "rule_origin":       row[1] or "SAME",
                "table_name":        row[6],   # corrected_xxx
                "column_name":       row[7],
                "check_type":        check_type,
                "check_params":      row[5] or "",
                "no_check":          no_override,
                # src_table pour update bscs_detected_inconsistency
                "src_table":         row[2],   # bscs_xxx original
            })

        # PKs du batch
        cur.execute("""
            SELECT table_name, pk_value
            FROM bscs_detection_batch_pks
            WHERE run_id = %s AND iteration_id = %s
        """, (detection_run_id, iteration_id))
        batch_pk_rows = cur.fetchall()
        logger.info("📦 %d PKs dans bscs_detection_batch_pks pour run_id='%s'",
                    len(batch_pk_rows), detection_run_id)

        pks_by_table = {}
        for tname, pk_val in batch_pk_rows:
            corr = SOURCE_TO_CORRECTED.get(tname.strip().lower(), tname)
            pks_by_table.setdefault(corr, []).append(pk_val)
        logger.info("📦 pks_by_table : %s", {k: len(v) for k, v in pks_by_table.items()})

        # Index detected inconsistency
        cur.execute("""
            SELECT id, table_name, rule_id, rule_origin
            FROM bscs_detected_inconsistency
            WHERE run_id = %s
        """, (detection_run_id,))
        di_index = {}
        for di_id, tname, rid, rorigin in cur.fetchall():
            if rid is None:
                continue
            key = (tname.strip().lower(), int(rid), str(rorigin).strip().upper())
            di_index[key] = di_id

    finally:
        cur.close()
        conn.close()
    payload = {
        "detection_run_id":  detection_run_id,
        "iteration_id":     iteration_id,
        "rules":             rules,
        "pks_by_table":      pks_by_table,
        "di_index":          {
            f"{k[0]}||{k[1]}||{k[2]}": v
            for k, v in di_index.items()
        },
    }

    context["ti"].xcom_push(key="correction_context", value=payload)
    logger.info(
        "✅ Context chargé — run=%s | iter=%s "
        "| %d règles | %d tables | %d sans override",
        detection_run_id, iteration_id,
        len(rules),
        len(set(r["table_name"] for r in rules)),
        sum(1 for r in rules if r["no_check"])
    )
def detect_after_correction(**context) -> None:
    ti      = context["ti"]
    payload = ti.xcom_pull(key="correction_context",
                           task_ids="load_correction_context")
    if not payload:
        raise ValueError("Aucun contexte de correction disponible")

    detection_run_id = payload["detection_run_id"]
    iteration_id = payload.get("iteration_id") or payload.get("detection_iteration_id")
    rules            = payload["rules"]
    pks_by_table     = payload["pks_by_table"]
    stg_info         = _staging_conn_info()

    # ── DIAGNOSTIC AU DÉMARRAGE ───────────────────────────────────────────
    try:
        diag_conn = _new_conn(stg_info)
        diag_cur  = diag_conn.cursor()
        diag_cur.execute("SELECT DATABASE()")
        diag_db = diag_cur.fetchone()[0]
        diag_cur.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME LIKE 'corrected_%%'
              AND COLUMN_KEY = 'PRI'
            ORDER BY TABLE_NAME
        """, (diag_db,))
        pk_rows = diag_cur.fetchall()
        logger.info("🔍 DATABASE()='%s' | PKs trouvées dans staging:", diag_db)
        for tname, col in pk_rows:
            logger.info("    → %s.%s", tname, col)
        if not pk_rows:
            logger.error("❌ AUCUNE PK pour les tables corrected_* dans '%s'", diag_db)
        diag_cur.close()
        diag_conn.close()
    except Exception as e:
        logger.error("❌ Erreur diagnostic PKs : %s", e)

    # Purger les anciennes entrées
    try:
        dc = _new_conn(stg_info)
        dk = dc.cursor()
        dk.execute("DELETE FROM staging_problematic_rows_after WHERE run_id = %s",
                   (detection_run_id,))
        dc.commit()
        dk.close()
        dc.close()
    except Exception as e:
        logger.error("❌ Erreur purge : %s", e)

    results  = []
    pk_cache = {}

    for rule in rules:
        corrected_table = rule["table_name"]
        check_type      = rule["check_type"]
        check_params    = rule["check_params"] or ""
        column          = rule["column_name"]
        detection_rid   = rule["detection_rule_id"]
        origin          = rule["rule_origin"]

        pks = pks_by_table.get(corrected_table, [])
        if not pks:
            logger.warning("⚠️ Aucune PK batch pour %s", corrected_table)
            results.append({
                "key":                 f"{corrected_table}||{detection_rid}||{origin}",
                "src_table":           corrected_table.replace("corrected_", "bscs_"),
                "corr_table":          corrected_table,
                "target_column":       column,
                "rule_id":             detection_rid,
                "rule_origin":         origin,
                "rule_label":          column,
                "category":            "UNKNOWN",
                "count_source":        0,
                "nb_violations_after": 0,
                "reliable":            True,
                "pk_ids_after":        [],
                "is_skipped":          True,
                "skip_reason":         "NO_BATCH",
            })
            continue

        if corrected_table not in pk_cache:
            pk = CORRECTED_TABLE_PKS.get(corrected_table)
            if pk:
                pk_cache[corrected_table] = pk
            else:
                logger.error("❌ PK inconnue pour %s", corrected_table)
                pk_cache[corrected_table] = None

        pk_col = pk_cache.get(corrected_table)
        # Règle sans check associé → marquer is_skipped=NO_RULE
        if rule.get("no_check"):
            results.append({
                "key":                 f"{corrected_table}||{detection_rid}||{origin}",
                "src_table":           rule.get("src_table", 
                                        corrected_table.replace("corrected_", "bscs_")),
                "corr_table":          corrected_table,
                "target_column":       column,
                "rule_id":             detection_rid,
                "rule_origin":         origin,
                "rule_label":          column,
                "category":            "UNKNOWN",
                "count_source":        len(pks),
                "nb_violations_after": 0,
                "reliable":            True,
                "pk_ids_after":        [],
                "is_skipped":          True,
                "skip_reason":         "NO_OVERRIDE",
            })
            continue
        if not pk_col:
            logger.warning("⚠️ PK manquante pour %s — règle ignorée", corrected_table)
            results.append({
                "key":                 f"{corrected_table}||{detection_rid}||{origin}",
                "src_table": rule.get("src_table", 
                                corrected_table.replace("corrected_", "bscs_")),
                "corr_table":          corrected_table,
                "target_column":       column,
                "rule_id":             detection_rid,
                "rule_origin":         origin,
                "rule_label":          column,
                "category":            "UNKNOWN",
                "count_source":        len(pks),
                "nb_violations_after": 0,
                "reliable":            True,
                "pk_ids_after":        [],
                "is_skipped":          True,
                "skip_reason":         "NO_PK",
            })
            continue

        res_dict = _build_post_check_query(
            corrected_table, column, pk_col, pks,
            check_type, check_params,
            detection_rid, origin
        )

        if not res_dict:
            logger.warning("⚠️ Pas de requête pour check_type=%s [%s.%s]",
                        check_type, corrected_table, column)
            results.append({
                "key":                 f"{corrected_table}||{detection_rid}||{origin}",
                "src_table":           corrected_table.replace("corrected_", "bscs_"),
                "corr_table":          corrected_table,
                "target_column":       column,
                "rule_id":             detection_rid,
                "rule_origin":         origin,
                "rule_label":          column,
                "category":            "UNKNOWN",
                "count_source":        len(pks),
                "nb_violations_after": 0,
                "reliable":            True,
                "pk_ids_after":        [],
                "is_skipped":          True,
                "skip_reason":         "BUILD_FAILED",
            })
            continue

        nb_viol      = 0
        pk_ids_after = []

        try:
            conn = _new_conn(stg_info)
            cur  = conn.cursor()
            cur.execute(res_dict["cnt_sql"], res_dict["params"])
            nb_viol = cur.fetchone()[0]
            if nb_viol > 0 and res_dict.get("ids_sql"):
                cur.execute(res_dict["ids_sql"], res_dict["ids_params"])
                pk_ids_after = [str(r[0]) for r in cur.fetchall()]
            cur.close()
            conn.close()

            if pk_ids_after:
                detection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ins_conn = _new_conn(stg_info)
                ins_cur  = ins_conn.cursor()
                ins_cur.executemany("""
                    INSERT IGNORE INTO staging_problematic_rows_after
                        (run_id, iteration_id, table_name, column_name,
                        rule_label, row_pk_value, detection_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, [
                    (detection_run_id, iteration_id,
                    corrected_table.replace("corrected_", "bscs_"),
                    column, res_dict["label"], pk_val, detection_date)
                    for pk_val in pk_ids_after
                ])
                ins_conn.commit()
                ins_cur.close()
                ins_conn.close()

            logger.info("  ✅ [%s] %s [%s] → %d/%d violations après correction",
                        check_type, res_dict["label"], corrected_table, nb_viol, len(pks))

        except Exception as e:
            logger.error("❌ Erreur check [%s.%s] type=%s : %s",
                        corrected_table, column, check_type, e)
            nb_viol = -1

        results.append({
            "key":                 f"{corrected_table}||{detection_rid}||{origin}",
            "src_table":           corrected_table.replace("corrected_", "bscs_"),
            "corr_table":          corrected_table,
            "target_column":       column,
            "rule_id":             detection_rid,
            "rule_origin":         origin,
            "rule_label":          res_dict.get("label", column),
            "category":            res_dict.get("category", "UNKNOWN"),
            "count_source":        len(pks),
            "nb_violations_after": max(nb_viol, 0),
            "reliable":            nb_viol >= 0,
            "pk_ids_after":        pk_ids_after,
            "is_skipped":          False,
            "skip_reason":         None,
        })
        # ── FIN DE BOUCLE — push résultats ───────────────────────────────────
    ti.xcom_push(key="detection_after_results", value=results)
    logger.info("🎉 Post-correction terminée : %d checks", len(results))

def _build_post_check_query(table, col, pk_col, pks,
                             check_type, check_params,
                             detection_rule_id, origin) -> dict:
    bw, bp = _pk_where(pk_col, pks)

    if check_type == "NOTNULL":
        w = f"(`{col}` IS NULL OR TRIM(`{col}`) = '') AND {bw}"
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} IS NOT NULL [post-correction]", "COMPLETUDE"
        )

    if check_type == "IN_DOMAIN":
        vals = [v.strip() for v in check_params.split(",")]
        ph   = _ph(vals)
        w    = f"`{col}` IS NOT NULL AND `{col}` NOT IN ({ph}) AND {bw}"
        p    = vals + bp
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            p, p,
            f"{col} IN ({check_params[:60]}) [Comarch]", "CONFORMITE_DOMAINE"
        )

    if check_type == "REGEX":
        w = (f"`{col}` IS NOT NULL AND `{col}` != '' "
             f"AND `{col}` NOT REGEXP %s AND {bw}")
        p = [check_params] + bp
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            p, p,
            f"{col} REGEX [post-correction]", "CONFORMITE_FORMAT"
        )

    if check_type == "FIXED_LENGTH":
        try:
            length = int(check_params)
        except (ValueError, TypeError):
            logger.warning("⚠️ FIXED_LENGTH params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        w = f"`{col}` IS NOT NULL AND LENGTH(`{col}`) != {length} AND {bw}"
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"LENGTH({col}) = {length} [post-correction]", "CONFORMITE_FORMAT"
        )

    if check_type == "COMPARE_VAL":
        if not check_params or "," not in check_params:
            logger.warning("⚠️ COMPARE_VAL params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        parts  = check_params.split(",", 1)
        op     = parts[0].strip()
        val    = parts[1].strip()
        inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<=",
                  "=": "!=", "!=": "="}
        inv_op = inv.get(op, "!=")
        w = f"`{col}` IS NOT NULL AND `{col}` {inv_op} %s AND {bw}"
        p = [val] + bp
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            p, p,
            f"{col} {op} {val} [post-correction]", "VALIDITE_NUMERIQUE"
        )

    if check_type == "COMPARE_COLS":
        if not check_params or check_params.count(",") < 2:
            logger.warning("⚠️ COMPARE_COLS params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        parts  = check_params.split(",", 2)
        c1     = parts[0].strip()
        op     = parts[1].strip()
        c2     = parts[2].strip()
        inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<=",
                  "=": "!=", "!=": "="}
        inv_op = inv.get(op, "!=")
        w = (f"`{c1}` IS NOT NULL AND `{c2}` IS NOT NULL "
             f"AND `{c1}` {inv_op} `{c2}` AND {bw}")
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{c1} {op} {c2} [post-correction]", "COHERENCE_SEMANTIQUE"
        )

    if check_type == "COMPARE_CURDATE":
        if not check_params:
            logger.warning("⚠️ COMPARE_CURDATE params vide [%s.%s]", table, col)
            return None
        op     = check_params.strip()
        inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<="}
        inv_op = inv.get(op, ">")
        w = f"`{col}` IS NOT NULL AND `{col}` {inv_op} CURDATE() AND {bw}"
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} {op} CURDATE() [post-correction]", "VALIDITE_TEMPORELLE"
        )

    if check_type == "AGE_CHECK":
        age = check_params.strip() if check_params else "18"
        w = (f"`{col}` IS NOT NULL "
             f"AND `{col}` > DATE_SUB(CURDATE(), INTERVAL {age} YEAR) "
             f"AND {bw}")
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} AGE >= {age} ans [post-correction]", "VALIDITE_TEMPORELLE"
        )

    if check_type == "UNIQUE_CHECK":
        w = (f"`{col}` IN "
             f"(SELECT `{col}` FROM `{table}` "
             f"GROUP BY `{col}` HAVING COUNT(*) > 1) AND {bw}")
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} UNIQUE [post-correction]", "UNICITE"
        )

    if check_type == "FK_CHECK":
        if not check_params or ":" not in check_params:
            logger.warning("⚠️ FK_CHECK params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        ref_table_raw, ref_col = check_params.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        # Résoudre bscs_xxx → corrected_xxx
        ref_table_corr = SOURCE_TO_CORRECTED.get(
            ref_table_raw.lower(),
            ref_table_raw
        )
        # Garde : si la table référencée est vide → skip (faux positifs)
        w = (
            f"`{col}` IS NOT NULL AND TRIM(CAST(`{col}` AS CHAR)) != '' "
            f"AND (SELECT COUNT(*) FROM `{ref_table_corr}`) > 0 "
            f"AND CAST(`{col}` AS CHAR) NOT IN ("
            f"SELECT CAST(`{ref_col}` AS CHAR) FROM `{ref_table_corr}` "
            f"WHERE `{ref_col}` IS NOT NULL) "
            f"AND {bw}"
        )
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} FK→{ref_table_corr}.{ref_col} [post-correction]",
            "INTEGRITE_REFERENTIELLE"
        )

    if check_type == "REF_CHECK":
        if not check_params or ":" not in check_params:
            logger.warning("⚠️ REF_CHECK params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        ref_table_raw, ref_col = check_params.split(":", 1)
        ref_table_raw = ref_table_raw.strip()
        ref_col       = ref_col.strip()
        ref_full = REF_TABLES.get(ref_table_raw, f"`{ref_table_raw}`")
        w = (
            f"`{col}` IS NOT NULL AND TRIM(`{col}`) != '' "
            f"AND UPPER(TRIM(`{col}`)) NOT IN ("
            f"SELECT UPPER(TRIM(`{ref_col}`)) FROM {ref_full} "
            f"WHERE `{ref_col}` IS NOT NULL) "
            f"AND {bw}"
        )
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} REF→{ref_table_raw}.{ref_col} [post-correction]",
            "CONFORMITE_REFERENTIEL"
        )

    if check_type == "CORRELATE":
        if not check_params or ":" not in check_params:
            logger.warning("⚠️ CORRELATE params invalide '%s' [%s.%s]",
                           check_params, table, col)
            return None
        parts = check_params.split(":", 2)
        if len(parts) < 3:
            logger.warning("⚠️ CORRELATE params incomplet '%s' [%s.%s]",
                           check_params, table, col)
            return None
        date_col   = parts[0].strip()
        status_ok  = parts[1].strip()
        status_ko  = parts[2].strip()
        w = (
            f"`{col}` = '{status_ok}' "
            f"AND `{date_col}` > CURDATE() "
            f"AND `{date_col}` IS NOT NULL "
            f"AND {bw}"
        )
        return _make_result(
            f"SELECT COUNT(*) FROM `{table}` WHERE {w}",
            f"SELECT `{pk_col}` FROM `{table}` WHERE {w} LIMIT {VIOLATION_FETCH_LIMIT}",
            bp, bp,
            f"{col} CORRELATE {date_col} [post-correction]", "COHERENCE_SEMANTIQUE"
        )

    logger.warning("⚠️ check_type='%s' non géré [%s.%s]", check_type, table, col)
    return None
def update_inconsistency(**context) -> None:
    ti      = context["ti"]
    payload = ti.xcom_pull(key="correction_context",
                           task_ids="load_correction_context")
    results = ti.xcom_pull(key="detection_after_results",
                           task_ids="detect_after_correction") or []

    if not payload or not results:
        logger.warning("⚠️ Aucun résultat à mettre à jour.")
        return

    detection_run_id = payload.get("detection_run_id")
    iteration_id     = payload.get("iteration_id")
    conn_info = _get_conn_info()
    conn      = _new_conn(conn_info)

    updated = 0
    skipped = 0

    # Compter les PKs distinctes par (table_name) pour ce run/iteration
    stg_info = _staging_conn_info()
    stg_conn = _new_conn(stg_info)
    stg_cur  = stg_conn.cursor()
    stg_cur.execute("""
        SELECT table_name, COUNT(DISTINCT row_pk_value) AS lignes_en_erreur
        FROM staging_problematic_rows_after
        WHERE run_id = %s
        GROUP BY table_name
    """, (detection_run_id,))
    nb_to_correct_map = {
        row[0]: row[1] for row in stg_cur.fetchall()
    }
    stg_cur.close()
    stg_conn.close()
    logger.info("📊 nb_to_correct_after (distinct PKs) par table : %s", nb_to_correct_map)
    cur = conn.cursor()
    src_table = ""
    try:
        
        logger.info("🔧 UPDATE ciblé — %d résultats", len(results))

        for res in results:
            src_table           = res.get("src_table", "")        # ← EN PREMIER
            column              = res.get("target_column", "")
            rule_id             = res.get("rule_id")
            origin              = res.get("rule_origin", "SAME")
            viol_after          = res.get("nb_violations_after", 0)
            count_source        = res.get("count_source", 1)
            nb_to_correct_after = nb_to_correct_map.get(src_table, 0)  # ← APRÈS src_table
            nb_to_migrate_after = max(count_source - nb_to_correct_after, 0)
            taux_after          = round(nb_to_correct_after * 100.0 / max(count_source, 1), 2)
            is_skipped  = res.get("is_skipped", False)
            skip_reason = res.get("skip_reason", None)

            if not rule_id:
                logger.warning("⚠️ rule_id manquant pour [%s.%s] — ignoré", src_table, column)
                skipped += 1
                continue

            if not res.get("reliable", True):
                logger.warning("⚠️ Résultat non fiable [%s.%s] — update ignoré", src_table, column)
                skipped += 1
                continue

            # Séparer les UPDATE selon is_skipped
            if is_skipped:
                if not rule_id:
                    logger.warning("⚠️ rule_id manquant pour skipped [%s.%s] — ignoré", src_table, column)
                    skipped += 1
                    continue
                try:
                    cur.execute("""
                        UPDATE bscs_detected_inconsistency
                        SET is_skipped   = %s,
                            skip_reason  = %s,
                            execution_date = NOW()
                        WHERE run_id      = %s
                        AND rule_id     = %s
                        AND rule_origin = %s
                        AND table_name  = %s
                        AND column_name = %s
                        AND error_category != 'POST_CORRECTION'
                    """, (
                        True, skip_reason,
                        detection_run_id, rule_id, origin,
                        src_table, column,
                    ))
                    rows_affected = cur.rowcount
                    if rows_affected == 0:
                        logger.warning("⚠️ Aucune ligne trouvée pour skipped [%s.%s]", src_table, column)
                        skipped += 1
                    else:
                        updated += rows_affected
                        logger.info("  ⏭ [%s.%s | rule=%s] → ignorée (%s)", 
                                    src_table, column, rule_id, skip_reason)
                except Exception as e:
                    logger.error("❌ UPDATE skipped [rule_id=%s | %s.%s] : %s", rule_id, src_table, column, e)
                    skipped += 1
                continue  # ← pas de calcul de taux etc.

            # UPDATE normal (règle exécutée)
            try:
                cur.execute("""
                    UPDATE bscs_detected_inconsistency
                    SET nb_violations_after = %s,
                        nb_to_correct_after = %s,
                        nb_to_migrate_after = %s,
                        taux_rejet_after    = %s,
                        is_skipped          = 0,
                        skip_reason         = NULL,
                        execution_date      = NOW()
                    WHERE run_id      = %s
                    AND rule_id     = %s
                    AND rule_origin = %s
                    AND table_name  = %s
                    AND column_name = %s
                    AND error_category != 'POST_CORRECTION'
                """, (
                    viol_after, nb_to_correct_after, nb_to_migrate_after , taux_after,
                    detection_run_id, rule_id, origin,
                    src_table, column,
                ))
                # ... reste du code identique

                rows_affected = cur.rowcount
                if rows_affected == 0:
                    logger.warning(
                        "⚠️ Aucune ligne trouvée [run=%s | rule_id=%s | origin=%s | %s.%s]",
                        detection_run_id, rule_id, origin, src_table, column
                    )
                    skipped += 1
                else:
                    updated += rows_affected
                    # APRÈS
                    logger.info(
                        "  ✅ [%s.%s | rule=%s | origin=%s] → violations=%d | to_migrate=%d | taux=%.2f%%",
                        src_table, column, rule_id, origin, viol_after, nb_to_migrate_after, taux_after
                    )

            except Exception as e:
                logger.error("❌ UPDATE [rule_id=%s | %s.%s] : %s", rule_id, src_table, column, e)
                skipped += 1

        conn.commit()

        # Supprimer les lignes POST_CORRECTION parasites créées par l'ancienne version
        cur.execute("""
            DELETE FROM bscs_detected_inconsistency
            WHERE run_id = %s AND error_category = 'POST_CORRECTION'
        """, (detection_run_id,))
        deleted = cur.rowcount
        conn.commit()

        logger.info("=" * 70)
        logger.info("✅ %d lignes mises à jour | %d ignorées | %d parasites supprimées",
                    updated, skipped, deleted)
        logger.info("=" * 70)

        by_table = {}
        for res in results:
            t = res.get("src_table", "?")
            by_table.setdefault(t, {"total": 0, "viol": 0, "ok": 0})
            by_table[t]["viol" if res.get("nb_violations_after", 0) > 0 else "ok"] += 1
            by_table[t]["total"] += 1

        for t, stats in sorted(by_table.items()):
            logger.info("   📊 %s: %d checks | ✅ %d OK | ⚠️  %d violations restantes",
                        t, stats["total"], stats["ok"], stats["viol"])

    except Exception as e:
        conn.rollback()
        logger.error("❌ Erreur update_inconsistency : %s", e)
        raise
    finally:
        cur.close()
        conn.close()
with DAG(
    dag_id="post_correction_detection",
    description=(
        "Détection post-correction — applique les règles de l'itération "
        "sur les tables corrected_*** et met à jour bscs_detected_inconsistency"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bss", "migration", "detection", "post-correction"],
    params={
        "detection_run_id": None,
        "detection_iteration_id": None,
    },
) as dag:

    t_load = PythonOperator(
        task_id="load_correction_context",
        python_callable=load_correction_context,
    )

    t_detect = PythonOperator(
        task_id="detect_after_correction",
        python_callable=detect_after_correction,
    )

    t_update = PythonOperator(
        task_id="update_inconsistency",
        python_callable=update_inconsistency,
    )

    t_load >> t_detect >> t_update