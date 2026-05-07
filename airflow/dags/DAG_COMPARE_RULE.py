

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import logging
import pymysql

default_args = {
    "owner": "data_detection",
    "start_date": datetime(2026, 3, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="detected_inconsistencies",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Détection des incohérences BSCS ",
)

VIOLATION_FETCH_LIMIT = 500


def _batch_where(pk_col, batch_ids, alias=""):
    """Génère la clause WHERE batch et ses paramètres."""
    if batch_ids is None:
        return "", []
    if len(batch_ids) == 0:
        return "1=0", []
    pfx = f"`{alias}`." if alias else ""
    ph  = ",".join(["%s"] * len(batch_ids))
    return f"{pfx}`{pk_col}` IN ({ph})", list(batch_ids)


def _make_result(cnt_sql, ids_sql, params, ids_params, label, category):
    return {
        "cnt_sql":    cnt_sql,
        "ids_sql":    ids_sql,
        "params":     params,
        "ids_params": ids_params,
        "label":      label,
        "category":   category,
    }

def _notnull(table, col, pk_col, batch_ids, label=None, extra_cond=None):
    bw, bp = _batch_where(pk_col, batch_ids)
    where_parts = [f"(`{col}` IS NULL OR TRIM(CAST(`{col}` AS CHAR)) = '')"]
    if extra_cond:
        where_parts.append(extra_cond)
    if bw:
        where_parts.append(bw)
    w = " AND ".join(where_parts)
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col} IS_NOT_NULL",
        "COMPLETUDE"
    )
def _in_domain(table, col, pk_col, batch_ids, values_str, label=None):
    vals   = [v.strip() for v in values_str.split(",")]
    ph     = ",".join(["%s"] * len(vals))
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` NOT IN ({ph})"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    p      = vals + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        p, p,
        label or f"{col} IN ({values_str[:60]})",
        "CONFORMITE_DOMAINE"
    )


def _regex(table, col, pk_col, batch_ids, pattern, label=None):
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` != '' AND `{col}` NOT REGEXP %s"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    p      = [pattern] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        p, p,
        label or f"{col} REGEX {pattern[:60]}",
        "CONFORMITE_FORMAT"
    )


def _like(table, col, pk_col, batch_ids, pattern, label=None):
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` != '' AND `{col}` NOT LIKE %s"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    p      = [pattern] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        p, p,
        label or f"{col} LIKE {pattern}",
        "CONFORMITE_FORMAT"
    )


def _compare_cols(table, col1, op, col2, pk_col, batch_ids,
                  label=None, category="COHERENCE_TEMPORELLE", null_check=True):
    """Vérifie col1 op col2 (ex: col1 <= col2 → violation si col1 > col2)."""
    inv = {"<=": ">", ">=": "<", "<": ">=", ">": "<=", "=": "!=", "!=": "="}
    inv_op = inv[op]
    bw, bp = _batch_where(pk_col, batch_ids)
    nc     = f"`{col1}` IS NOT NULL AND `{col2}` IS NOT NULL AND " if null_check else ""
    w_base = f"{nc}`{col1}` {inv_op} `{col2}`"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col1} {op} {col2}",
        category
    )


def _compare_val(table, col, op, val, pk_col, batch_ids,
                 label=None, category="VALIDITE_NUMERIQUE"):
    """Vérifie col op val (ex: col >= 0 → violation si col < 0)."""
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<=", "=": "!=", "!=": "="}
    inv_op = inv[op]
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` {inv_op} %s"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    p      = [val] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        p, p,
        label or f"{col} {op} {val}",
        category
    )


def _compare_curdate(table, col, op, pk_col, batch_ids,
                     label=None, category="VALIDITE_TEMPORELLE"):
    """Vérifie col op CURDATE() → violation si col inv_op CURDATE()."""
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<="}
    inv_op = inv[op]
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` {inv_op} CURDATE()"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col} {op} CURDATE()",
        category
    )


def _both(table, col1, col2, pk_col, batch_ids, label=None):
    """Les deux colonnes doivent être renseignées ensemble."""
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = (
        f"(`{col1}` IS NOT NULL AND TRIM(CAST(`{col1}` AS CHAR)) != '' "
        f"AND (`{col2}` IS NULL OR TRIM(CAST(`{col2}` AS CHAR)) = '')) "
        f"OR (`{col2}` IS NOT NULL AND TRIM(CAST(`{col2}` AS CHAR)) != '' "
        f"AND (`{col1}` IS NULL OR TRIM(CAST(`{col1}` AS CHAR)) = ''))"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col1} BOTH {col2}",
        "COMPLETUDE"
    )


def _or_notnull(table, col1, col2, pk_col, batch_ids, label=None):
    """Au moins l'une des deux colonnes doit être renseignée."""
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = (
        f"(`{col1}` IS NULL OR TRIM(CAST(`{col1}` AS CHAR)) = '') "
        f"AND (`{col2}` IS NULL OR TRIM(CAST(`{col2}` AS CHAR)) = '')"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col1} OR {col2} NOT NULL",
        "COMPLETUDE"
    )


def _notnull_if(table, col, cond_col, pk_col, batch_ids, label=None):
    """col ne peut pas être null si cond_col est renseigné."""
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = (
        f"(`{cond_col}` IS NOT NULL AND TRIM(CAST(`{cond_col}` AS CHAR)) != '') "
        f"AND (`{col}` IS NULL OR TRIM(CAST(`{col}` AS CHAR)) = '')"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    t = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"{col} NOTNULL IF {cond_col} NOT NULL",
        "COMPLETUDE"
    )


def _gt_if_nn(table, col, val, pk_col, batch_ids, label=None):
    """col doit être > val quand col n'est pas null."""
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col}` IS NOT NULL AND `{col}` <= %s"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    p      = [val] + bp
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        p, p,
        label or f"{col} > {val} IF NOT NULL",
        "VALIDITE_NUMERIQUE"
    )


def _length_lt(table, col1, col2, pk_col, batch_ids, label=None):
    """LENGTH(col1) doit être < LENGTH(col2)."""
    bw, bp = _batch_where(pk_col, batch_ids)
    w_base = f"`{col1}` IS NOT NULL AND `{col2}` IS NOT NULL AND LENGTH(`{col1}`) >= LENGTH(`{col2}`)"
    w      = f"({w_base}) AND {bw}" if bw else w_base
    t      = f"`{table}`"
    return _make_result(
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT `{pk_col}` FROM {t} WHERE {w}",
        bp, bp,
        label or f"LENGTH({col1}) < LENGTH({col2})",
        "COHERENCE_SEMANTIQUE"
    )


# ── DIFF : requêtes inter-tables ───────────────────────────────────────────────

def _diff_fk(t1, col1, t2, col2, pk_col, batch_ids, label=None):
    """Intégrité référentielle : col1 de t1 doit exister dans col2 de t2."""
    bw, bp = _batch_where(pk_col, batch_ids, alias="t1")
    w_base = (
        f"t1.`{col1}` IS NOT NULL "
        f"AND t1.`{col1}` NOT IN (SELECT `{col2}` FROM `{t2}` WHERE `{col2}` IS NOT NULL)"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    return _make_result(
        f"SELECT COUNT(*) FROM `{t1}` t1 WHERE {w}",
        f"SELECT t1.`{pk_col}` FROM `{t1}` t1 WHERE {w}",
        bp, bp,
        label or f"{t1}.{col1} EXISTS IN {t2}.{col2}",
        "INTEGRITE_REFERENTIELLE"
    )


def _diff_equal(t1, col1, t2, col2, join_col, pk_col, batch_ids, label=None):
    """Égalité de valeur entre deux tables après jointure."""
    bw, bp = _batch_where(pk_col, batch_ids, alias="t1")
    w_base = (
        f"t1.`{col1}` IS NOT NULL AND t2.`{col2}` IS NOT NULL "
        f"AND t1.`{col1}` != t2.`{col2}`"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    return _make_result(
        f"SELECT COUNT(*) FROM `{t1}` t1 INNER JOIN `{t2}` t2 ON t1.`{join_col}` = t2.`{join_col}` WHERE {w}",
        f"SELECT t1.`{pk_col}` FROM `{t1}` t1 INNER JOIN `{t2}` t2 ON t1.`{join_col}` = t2.`{join_col}` WHERE {w}",
        bp, bp,
        label or f"{t1}.{col1} = {t2}.{col2}",
        "COHERENCE_INTER_ENTITES"
    )


def _diff_compare(t1, col1, op, t2, col2, join_col_t1, join_col_t2,
                  pk_col, batch_ids, label=None, category="COHERENCE_INTER_ENTITES"):
    """Comparaison de valeur entre deux tables après jointure."""
    inv    = {"<=": ">", ">=": "<", "<": ">=", ">": "<="}
    inv_op = inv[op]
    bw, bp = _batch_where(pk_col, batch_ids, alias="t1")
    w_base = (
        f"t1.`{col1}` IS NOT NULL AND t2.`{col2}` IS NOT NULL "
        f"AND t1.`{col1}` {inv_op} t2.`{col2}`"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    jc1, jc2 = join_col_t1, join_col_t2
    return _make_result(
        f"SELECT COUNT(*) FROM `{t1}` t1 INNER JOIN `{t2}` t2 ON t1.`{jc1}` = t2.`{jc2}` WHERE {w}",
        f"SELECT t1.`{pk_col}` FROM `{t1}` t1 INNER JOIN `{t2}` t2 ON t1.`{jc1}` = t2.`{jc2}` WHERE {w}",
        bp, bp,
        label or f"{t1}.{col1} {op} {t2}.{col2}",
        category
    )


def _diff_ref(t1, col1, t2, col2, pk_col, batch_ids,
              label=None, category="CONFORMITE_REFERENTIEL"):
    """Vérification dans une table référentiel (map_country, map_currency)."""
    bw, bp = _batch_where(pk_col, batch_ids, alias="t1")
    w_base = (
        f"t1.`{col1}` IS NOT NULL AND TRIM(t1.`{col1}`) != '' "
        f"AND UPPER(TRIM(t1.`{col1}`)) NOT IN "
        f"(SELECT UPPER(TRIM(`{col2}`)) FROM `{t2}` WHERE `{col2}` IS NOT NULL)"
    )
    w = f"({w_base}) AND {bw}" if bw else w_base
    return _make_result(
        f"SELECT COUNT(*) FROM `{t1}` t1 WHERE {w}",
        f"SELECT t1.`{pk_col}` FROM `{t1}` t1 WHERE {w}",
        bp, bp,
        label or f"{t1}.{col1} IN {t2}.{col2}",
        category
    )


# ══════════════════════════════════════════════════════════════════════════════
# CATALOGUE PRINCIPAL : SAME TABLE
# Clé : rule_id (int)
# Valeur : lambda(pk_col, batch_ids) → dict résultat
# ══════════════════════════════════════════════════════════════════════════════

SAME_CATALOG = {

    # ── BSCS_BILLING_ACCOUNT ──────────────────────────────────────────────────
    1:   lambda pk, b: _compare_cols("bscs_billing_account", "BA_ENTRY_DATE", "<=", "BA_VERS_VALID_FROM", pk, b,
                                     "BA_ENTRY_DATE <= BA_VERS_VALID_FROM"),
    2:   lambda pk, b: _compare_cols("bscs_billing_account", "BA_VERS_VALID_FROM", ">=", "BA_ENTRY_DATE", pk, b,
                                     "BA_VERS_VALID_FROM >= BA_ENTRY_DATE"),
    3:   lambda pk, b: _compare_cols("bscs_billing_account", "LAST_BILLED_DATE", ">=", "BA_VERS_VALID_FROM", pk, b,
                                     "LAST_BILLED_DATE >= BA_VERS_VALID_FROM"),
    4:   lambda pk, b: _compare_val("bscs_billing_account", "BILLING_ACCOUNT_ID", ">=", 1001, pk, b,
                                     "BILLING_ACCOUNT_ID >= 1001"),
    5:   lambda pk, b: _like("bscs_billing_account", "BILLING_ACCOUNT_CODE", pk, b, "BA%",
                              "BILLING_ACCOUNT_CODE LIKE BA%"),
    7:   lambda pk, b: _in_domain("bscs_billing_account", "BA_VERS_STATUT", pk, b,
                                   "ACTIVE,SUSPENDED,CLOSED,PENDING"),
    170: lambda pk, b: _notnull("bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    171: lambda pk, b: _notnull("bscs_billing_account", "CUSTOMER_ID", pk, b),
    174: lambda pk, b: _compare_curdate("bscs_billing_account", "BA_VERS_VALID_FROM", "<=", pk, b),
    177: lambda pk, b: _regex("bscs_billing_account", "BILLING_ACCOUNT_CODE", pk, b, "^BA[A-Z0-9-]+$"),
    178: lambda pk, b: _notnull("bscs_billing_account", "CURRENCY_ID", pk, b),
    179: lambda pk, b: _regex("bscs_billing_account", "CURRENCY_DESC", pk, b, "^[A-Z]{3}$"),
    317: lambda pk, b: _in_domain("bscs_billing_account", "BILL_MEDIUM", pk, b,
                                   "EMAIL,POSTAL,SMS,PORTAL"),
    318: lambda pk, b: _notnull("bscs_billing_account", "BILLING_ACCOUNT_NAME", pk, b),
    319: lambda pk, b: _make_result(
        *_correlate_ba_statut_valid_from(pk, b)[:4],
        "BA_VERS_STATUT ACTIVE consistent avec BA_VERS_VALID_FROM",
        "COHERENCE_SEMANTIQUE"
    ),
    408: lambda pk, b: _in_domain("bscs_billing_account", "CALL_DETAIL_FLAG", pk, b, "Y,N"),
    409: lambda pk, b: _in_domain("bscs_billing_account", "BILLING_ACCOUNT_PRIMAIRE", pk, b, "Y,N"),
    410: lambda pk, b: _make_result(
        *_correlate_bill_medium_desc(pk, b)[:4],
        "BILL_MEDIUM_DESC consistent avec BILL_MEDIUM",
        "COHERENCE_SEMANTIQUE"
    ),
    411: lambda pk, b: _make_result(
        *_correlate_currency_desc(pk, b)[:4],
        "CURRENCY_DESC must match map_currency.code_iso for given CURRENCY_ID",
        "COHERENCE_SEMANTIQUE"
    ),

    # ── BSCS_BILLING_ACCOUNT_ASSIGN ───────────────────────────────────────────
    8:   lambda pk, b: _compare_curdate("bscs_billing_account_assign", "VALID_FROM", "<=", pk, b),
    9:   lambda pk, b: _in_domain("bscs_billing_account_assign", "INVOICING_IND", pk, b, "Y,N"),

    # ── BSCS_CHARGE ───────────────────────────────────────────────────────────
    10:  lambda pk, b: _compare_val("bscs_charge", "AMOUNT", ">", 0, pk, b, "AMOUNT > 0"),
    11:  lambda pk, b: _compare_cols("bscs_charge", "AMOUNT_GROSS", ">=", "AMOUNT", pk, b),
    12:  lambda pk, b: _compare_cols("bscs_charge", "ENTDATE", "<=", "VALID_FROM", pk, b),
    13:  lambda pk, b: _regex("bscs_charge", "PERIOD", pk, b,
                               "^(2024|2025|2026)(0[1-9]|1[0-2])$"),
    180: lambda pk, b: _notnull("bscs_charge", "CUSTOMER_ID", pk, b),
    184: lambda pk, b: _notnull("bscs_charge", "VALID_FROM", pk, b),
    186: lambda pk, b: _compare_curdate("bscs_charge", "VALID_FROM", "<=", pk, b),
    187: lambda pk, b: _regex("bscs_charge", "PERIOD", pk, b,
                               "^(2024|2025|2026)(0[1-9]|1[0-2])$"),
   
    413: lambda pk, b: _gt_if_nn("bscs_charge", "CO_ID", 0, pk, b),
    414: lambda pk, b: _regex("bscs_charge", "GLCODE", pk, b, "^GL[0-9]+$"),
    415: lambda pk, b: _regex("bscs_charge", "SNCODE", pk, b, "^SN[0-9]+$"),
    416: lambda pk, b: _regex("bscs_charge", "TMCODE", pk, b, "^TM[0-9]+$"),
    417: lambda pk, b: _regex("bscs_charge", "VSCODE", pk, b, "^VS[0-9]+$"),

    # ── BSCS_CUSTOMER ─────────────────────────────────────────────────────────
    75:  lambda pk, b: _regex("bscs_customer", "COUNTRYCODEOFBIRTH", pk, b, "^[A-Z]{3}$"),
    76:  lambda pk, b: _notnull("bscs_customer", "COUNTRY", pk, b),
    109: lambda pk, b: _notnull("bscs_customer", "FIRSTNAME", pk, b),
    110: lambda pk, b: _notnull("bscs_customer", "LASTNAME", pk, b),
    111: lambda pk, b: _notnull("bscs_customer", "CUSTOMER_ID", pk, b),
    112: lambda pk, b: _notnull("bscs_customer", "CURRENCY", pk, b),
    114: lambda pk, b: _notnull("bscs_customer", "DATEOFBIRTH", pk, b),
    115: lambda pk, b: _compare_cols("bscs_customer", "VALIDFROM", "<=", "VALIDTO", pk, b),
    116: lambda pk, b: _compare_cols("bscs_customer", "CREDITPROFILEVALIDFROM", "<=",
                                      "CREDITPROFILEVALIDTO", pk, b),
    117: lambda pk, b: _compare_curdate("bscs_customer", "DATEOFBIRTH", "<=", pk, b),
    118: lambda pk, b: _compare_val("bscs_customer", "CREDITSCOREINTVALUE", ">=", 0, pk, b),
    119: lambda pk, b: _compare_val("bscs_customer", "CREDITSCOREINTVALUE", "<=", 1000, pk, b),
    120: lambda pk, b: _compare_val("bscs_customer", "CREDITRISKRATINGINTVALUE", ">=", 0, pk, b),
    121: lambda pk, b: _regex("bscs_customer", "EMAILSPEC", pk, b,
                               "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
    122: lambda pk, b: _in_domain("bscs_customer", "BILLINGCYCLE", pk, b,
                                   "MONTHLY,QUARTERLY,ANNUAL"),
    123: lambda pk, b: _in_domain("bscs_customer", "PAYMENTTYPE", pk, b,
                                   "DIRECT_DEBIT,CREDIT_CARD,BANK_TRANSFER,CHEQUE,ESPECES,CRYPTO"),
    124: lambda pk, b: _regex("bscs_customer", "BIC", pk, b,
                               "^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$"),
    125: lambda pk, b: _regex("bscs_customer", "BANKACCOUNTNUMBER", pk, b,
                               "^[A-Z]{2}[0-9]{2}[A-Z0-9]{4,}$"),
    126: lambda pk, b: _regex("bscs_customer", "PAYMENTCARDNUMBER", pk, b, "^[0-9]{13,19}$"),
    127: lambda pk, b: _in_domain("bscs_customer", "PAYMENTCARDTYPE", pk, b,
                                   "VISA,MASTERCARD,AMEX,CB"),
    128: lambda pk, b: _compare_curdate("bscs_customer", "PAYMENTCARDEXPIRATIONDATE", ">=", pk, b),
    129: lambda pk, b: _in_domain("bscs_customer", "CIVILITE", pk, b,
                                   "M,MME,DR,PR,M.,Mme,Dr."),
    130: lambda pk, b: _in_domain("bscs_customer", "LANGUAGE", pk, b,
                                   "FR,AR,EN,ES,DE,IT"),
    131: lambda pk, b: _in_domain("bscs_customer", "LANGUAGEPREF", pk, b,
                                   "FR,AR,EN,ES,DE,IT"),
    132: lambda pk, b: _in_domain("bscs_customer", "MARKETSEGMENTS", pk, b,
                                   "RESIDENTIAL,BUSINESS,CORPORATE,ENTERPRISE,SME"),
    133: lambda pk, b: _in_domain("bscs_customer", "ORGANIZATIONTYPE", pk, b,
                                   "INDIVIDUAL,ENTERPRISE,SME"),
    134: lambda pk, b: _in_domain("bscs_customer", "ROLESTATESYMBOL", pk, b,
                                   "ACTIVE,INACTIVE,SUSPENDED"),
    135: lambda pk, b: _in_domain("bscs_customer", "ROLETYPE", pk, b,
                                   "CUSTOMER,PROSPECT,PARTNER"),
    136: lambda pk, b: _in_domain("bscs_customer", "PREFERREDMEDIUM", pk, b,
                                   "EMAIL,SMS,COURRIER"),
    137: lambda pk, b: _in_domain("bscs_customer", "PAYMENTTERMS", pk, b,
                                   "NET15,NET30,NET45,NET60"),
    139: lambda pk, b: _compare_val("bscs_customer", "DATEOFBIRTH", ">=", "1900-01-01", pk, b,
                                     category="VALIDITE_TEMPORELLE"),
    140: lambda pk, b: _notnull("bscs_customer", "VALIDFROM", pk, b),
    141: lambda pk, b: _notnull("bscs_customer", "VALIDTO", pk, b),
    
    143: lambda pk, b: _regex("bscs_customer", "PHONESPEC", pk, b, "^\\+[0-9]{7,15}$"),
    144: lambda pk, b: _notnull("bscs_customer", "EMAILSPEC", pk, b),
    145: lambda pk, b: _notnull("bscs_customer", "IDDOCUMENTNUMBER", pk, b),
    146: lambda pk, b: _in_domain("bscs_customer", "TYPEOFIDDOCUMENT", pk, b,
                                   "CIN,PASSEPORT,CNI,PERMIS"),
    320: lambda pk, b: _compare_cols("bscs_customer", "CREDITSCOREVALIDTO", ">=",
                                      "CREDITSCOREVALIDFROM", pk, b),
    321: lambda pk, b: _compare_cols("bscs_customer", "CREDITSCOREVALIDFROM", ">=",
                                      "CREDITPROFILEVALIDFROM", pk, b),
    322: lambda pk, b: _compare_cols("bscs_customer", "CREDITSCOREVALIDTO", "<=",
                                      "CREDITPROFILEVALIDTO", pk, b),  # <- was missing
    323: lambda pk, b: _both("bscs_customer", "PAYMENTCARDNUMBER", "PAYMENTCARDTYPE", pk, b),
    324: lambda pk, b: _both("bscs_customer", "BANKACCOUNTNUMBER", "BIC", pk, b),
    327: lambda pk, b: _regex("bscs_customer", "CUSTCODE", pk, b, "^CC[0-9]{3}$"),

    # ── BSCS_CUSTOMER_TAX_EXEMPT ──────────────────────────────────────────────
    14:  lambda pk, b: _compare_val("bscs_customer_tax_exempt", "EXEMPT_RATE", ">=", 0, pk, b),
    15:  lambda pk, b: _compare_val("bscs_customer_tax_exempt", "EXEMPT_RATE", "<=", 100, pk, b),
    16:  lambda pk, b: _compare_cols("bscs_customer_tax_exempt", "VALID_FROM", "<=",
                                      "EXPIRATION_DATE", pk, b),
    17:  lambda pk, b: _in_domain("bscs_customer_tax_exempt", "EXEMPT_STATUS", pk, b,
                                   "FULL_EXEMPT,PARTIAL_EXEMPT,NO_EXEMPT,PENDING"),
    268: lambda pk, b: _notnull("bscs_customer_tax_exempt", "CUSTOMER_ID", pk, b),
    273: lambda pk, b: _notnull("bscs_customer_tax_exempt", "VALID_FROM", pk, b),
    274: lambda pk, b: _both("bscs_customer_tax_exempt", "TAXCODE", "TAXCODE_NAME", pk, b),
    275: lambda pk, b: _compare_cols("bscs_customer_tax_exempt", "EXPIRATION_DATE", ">=",
                                      "VALID_FROM", pk, b),
    276: lambda pk, b: _make_result(
        *_correlate_exempt(pk, b)[:4],
        "EXEMPT_RATE cohérent avec EXEMPT_STATUS",
        "COHERENCE_SEMANTIQUE"
    ),

    # ── BSCS_FINDOCS ──────────────────────────────────────────────────────────
    98:  lambda pk, b: _notnull("bscs_findocs", "CUSTOMER_ID", pk, b),
    99:  lambda pk, b: _notnull("bscs_findocs", "CURRENCY", pk, b),
    100: lambda pk, b: _notnull("bscs_findocs", "DUEDATE", pk, b),
    101: lambda pk, b: _notnull("bscs_findocs", "ISSUEDATE", pk, b),
    102: lambda pk, b: _notnull("bscs_findocs", "REFERENCE", pk, b),
    103: lambda pk, b: _compare_val("bscs_findocs", "INITAMOUNT", ">", 0, pk, b),
    104: lambda pk, b: _compare_cols("bscs_findocs", "UNCLEAREDAMOUNT", "<=", "INITAMOUNT", pk, b),
    105: lambda pk, b: _compare_cols("bscs_findocs", "ISSUEDATE", "<=", "DUEDATE", pk, b),
    106: lambda pk, b: _in_domain("bscs_findocs", "DOCTYPE", pk, b, "IN,CR"),
    107: lambda pk, b: _compare_val("bscs_findocs", "EXCHANGERATEVALUE", ">", 0, pk, b),
    108: lambda pk, b: _regex("bscs_findocs", "CURRENCY", pk, b, "^[A-Z]{3}$"),
    147: lambda pk, b: _compare_cols("bscs_findocs", "NETINITAMOUNT", "<=", "INITAMOUNT", pk, b),
    148: lambda pk, b: _compare_val("bscs_findocs", "UNCLEAREDAMOUNT", ">=", 0, pk, b),
    149: lambda pk, b: _compare_val("bscs_findocs", "CURRENCYINITAMOUNT", ">=", 0, pk, b),
    150: lambda pk, b: _compare_val("bscs_findocs", "DUEDATE", ">=", "2000-01-01", pk, b,
                                     category="VALIDITE_TEMPORELLE"),
    151: lambda pk, b: _compare_val("bscs_findocs", "ISSUEDATE", ">=", "2000-01-01", pk, b,
                                     category="VALIDITE_TEMPORELLE"),
    154: lambda pk, b: _regex("bscs_findocs", "REFERENCE", pk, b, "^REF[0-9A-Z-]{3,}$"),
    155: lambda pk, b: _regex("bscs_findocs", "EXTREFERENCE", pk, b, "^EXT[0-9A-Z]{3,}$"),
    156: lambda pk, b: _compare_val("bscs_findocs", "DOCTYPE_DET", ">=", 0, pk, b),
    157: lambda pk, b: _in_domain("bscs_findocs", "COMPLAINT", pk, b, "Y,N"),
    329: lambda pk, b: _make_result(
        *_correlate_findocs_currency(pk, b)[:4],
        "CURRENCYINITAMOUNT cohérent avec INITAMOUNT * EXCHANGERATEVALUE",
        "COHERENCE_SEMANTIQUE"
    ),
    331: lambda pk, b: _make_result(
        *_correlate_findocs_doctype(pk, b)[:4],
        "DOCTYPE cohérent avec DOCTYPE_DET",
        "COHERENCE_SEMANTIQUE"
    ),
    332: lambda pk, b: _compare_cols("bscs_findocs", "REFERENCE", "!=", "EXTREFERENCE", pk, b,
                                      category="COHERENCE_SEMANTIQUE"),

    # ── BSCS_MEMOS ────────────────────────────────────────────────────────────
    205: lambda pk, b: _notnull("bscs_memos", "CUSTOMER_ID", pk, b),
    206: lambda pk, b: _notnull("bscs_memos", "CONTRACT_ID", pk, b),
    207: lambda pk, b: _notnull("bscs_memos", "CREATED_DATE", pk, b),
    439: lambda pk, b: _notnull("bscs_memos", "CREATED_BY", pk, b),
    440: lambda pk, b: _compare_val("bscs_memos", "TICKLER_NUMBER", ">", 0, pk, b),
    441: lambda pk, b: _length_lt("bscs_memos", "SHORT_DESCRIPTION", "LONG_DESCRIPTION", pk, b),
    442: lambda pk, b: _compare_curdate("bscs_memos", "CREATED_DATE", "<=", pk, b),
    443: lambda pk, b: _in_domain("bscs_memos", "TICKLER_CATEGORY", pk, b,
                                   "BILLING,DISPUTE,SALES,TECHNICAL,COLLECTION,CREDIT,"
                                   "MARKETING,COMPLAINT,SERVICE,ADMIN,LEGAL,SYSTEM"),

    # ── BSCS_PAYMENTS ─────────────────────────────────────────────────────────
    78:  lambda pk, b: _in_domain("bscs_payments", "CANAL", pk, b,
                                   "WEB,AGENCE,MOBILE,AUTOMATIQUE"),
    79:  lambda pk, b: _in_domain("bscs_payments", "PAYMENT_MODE", pk, b,
                                   "VIREMENT,CHEQUE,CARTE,PRELEVEMENT,ESPECES"),
    80:  lambda pk, b: _in_domain("bscs_payments", "TRANSACTION_TYPE", pk, b, "CREDIT,DEBIT"),
    81:  lambda pk, b: _compare_val("bscs_payments", "PAYMENT_AMOUNT", ">", 0, pk, b),
    82:  lambda pk, b: _compare_curdate("bscs_payments", "PAYMENT_DATE", "<=", pk, b),
    83:  lambda pk, b: _regex("bscs_payments", "AGENCE", pk, b, "^AG_[A-Z]+_[0-9]+$"),
    84:  lambda pk, b: _regex("bscs_payments", "PAYMENT_ID", pk, b, "^PAY[0-9]+$"),
    85:  lambda pk, b: _notnull("bscs_payments", "AGENCE", pk, b),
    86:  lambda pk, b: _notnull("bscs_payments", "PAYMENT_AMOUNT", pk, b),
    87:  lambda pk, b: _notnull("bscs_payments", "PAYMENT_CURRENCY", pk, b),
    88:  lambda pk, b: _notnull("bscs_payments", "PAYMENT_REFERENCE", pk, b),
    89:  lambda pk, b: _notnull("bscs_payments", "PAYMENT_DATE", pk, b),
    90:  lambda pk, b: _notnull("bscs_payments", "CANAL", pk, b),
    91:  lambda pk, b: _notnull("bscs_payments", "PAYMENT_MODE", pk, b),
    446: lambda pk, b: _regex("bscs_payments", "PAYMENT_CURRENCY", pk, b, "^[A-Z]{3}$"),
    448: lambda pk, b: _compare_cols("bscs_payments", "REV_PAYMENT_ID", "!=", "PAYMENT_ID", pk, b,
                                      category="COHERENCE_SEMANTIQUE"),
    449: lambda pk, b: _notnull("bscs_payments", "USERNAME", pk, b),

    # ── BSCS_PAYMENTS_DET ─────────────────────────────────────────────────────
    92:  lambda pk, b: _notnull("bscs_payments_det", "CUSTOMER_ID", pk, b),
    93:  lambda pk, b: _notnull("bscs_payments_det", "PAYMENT_ID", pk, b),
    94:  lambda pk, b: _notnull("bscs_payments_det", "ID_FINDOC", pk, b),
    95:  lambda pk, b: _notnull("bscs_payments_det", "AMOUNT_PAID", pk, b),
    96:  lambda pk, b: _compare_val("bscs_payments_det", "AMOUNT_PAID", ">", 0, pk, b),
    97:  lambda pk, b: _regex("bscs_payments_det", "PAYMENT_ID", pk, b, "^PAY[0-9]+$"),

    # ── BSCS_PLACE ────────────────────────────────────────────────────────────
    21:  lambda pk, b: _compare_cols("bscs_place", "VALIDFROM", "<=", "UPDATEFROM", pk, b),
    22:  lambda pk, b: _in_domain("bscs_place", "PLACE_TYPE", pk, b,
                                   "BILLING,USAGE,SERVICE"),
    23:  lambda pk, b: _regex("bscs_place", "POSTALCODE", pk, b, "^[0-9]{4,5}$"),
    77:  lambda pk, b: _notnull("bscs_place", "COUNTRY", pk, b),
    189: lambda pk, b: _notnull("bscs_place", "CUSTOMER_ID", pk, b),
    190: lambda pk, b: _notnull("bscs_place", "PLACE_ID", pk, b),
    192: lambda pk, b: _regex("bscs_place", "COUNTRY", pk, b, "^[A-Z]{2}$"),

    # ── BSCS_PORTABILITY_HIST ─────────────────────────────────────────────────
    24:  lambda pk, b: _compare_cols("bscs_portability_hist", "ENTRY_DATE", "<=", "PORTING_DATE", pk, b),
    25:  lambda pk, b: _in_domain("bscs_portability_hist", "STATUS", pk, b,
                                   "COMPLETED,PENDING,IN_PROGRESS,REJECTED"),
    26:  lambda pk, b: _regex("bscs_portability_hist", "DN_NUM", pk, b, "^[0-9]{8,15}$"),
    211: lambda pk, b: _notnull("bscs_portability_hist", "CUSTOMER_ID", pk, b),
    212: lambda pk, b: _notnull("bscs_portability_hist", "CONTRACT_ID", pk, b),
    216: lambda pk, b: _compare_curdate("bscs_portability_hist", "PORTING_DATE", "<=", pk, b),
    280: lambda pk, b: _compare_val("bscs_portability_hist", "ACTION_ID", ">", 0, pk, b),
    281: lambda pk, b: _compare_cols("bscs_portability_hist", "PORTING_DATE", ">=", "ENTRY_DATE", pk, b),
    282: lambda pk, b: _notnull("bscs_portability_hist", "RAISON_PORTABILITY", pk, b),

    # ── BSCS_PORTABILITY_IN ───────────────────────────────────────────────────
    27:  lambda pk, b: _compare_cols("bscs_portability_in", "ENTRY_DATE", "<=", "PORTING_DATE", pk, b),
    28:  lambda pk, b: _in_domain("bscs_portability_in", "STATUS", pk, b,
                                   "COMPLETED,PENDING,IN_PROGRESS,REJECTED"),
    29:  lambda pk, b: _regex("bscs_portability_in", "DN_NUM", pk, b, "^[0-9]{8,15}$"),
    217: lambda pk, b: _notnull("bscs_portability_in", "CUSTOMER_ID", pk, b),
    218: lambda pk, b: _notnull("bscs_portability_in", "CONTRACT_ID", pk, b),
    283: lambda pk, b: _compare_val("bscs_portability_in", "ACTION_ID", ">", 0, pk, b),
    284: lambda pk, b: _compare_cols("bscs_portability_in", "PORTING_DATE", ">=", "ENTRY_DATE", pk, b),
    285: lambda pk, b: _notnull("bscs_portability_in", "RAISON_PORTABILITY", pk, b),

    # ── BSCS_PRE_ACTIVATION ───────────────────────────────────────────────────
    30:  lambda pk, b: _in_domain("bscs_pre_activation", "CLASSE_SIM", pk, b,
                                   "PREPAID,POSTPAID,HYBRID"),
    31:  lambda pk, b: _in_domain("bscs_pre_activation", "SIMTYPE", pk, b,
                                   "NANO,MICRO,STANDARD"),
    32:  lambda pk, b: _regex("bscs_pre_activation", "PIN1", pk, b, "^[0-9]{4}$"),
    33:  lambda pk, b: _regex("bscs_pre_activation", "PUK1", pk, b, "^[0-9]{8}$"),
    34:  lambda pk, b: _regex("bscs_pre_activation", "MSISDN", pk, b,
                               "^(216|212|213|33)[0-9]{8}$"),
    35:  lambda pk, b: _regex("bscs_pre_activation", "ICCID", pk, b, "^89310[0-9]{18}$"),
    228: lambda pk, b: _notnull("bscs_pre_activation", "MSISDN", pk, b),
    229: lambda pk, b: _notnull("bscs_pre_activation", "ICCID", pk, b),
    286: lambda pk, b: _compare_cols("bscs_pre_activation", "PIN1", "!=", "PIN2", pk, b,
                                      category="COHERENCE_SEMANTIQUE"),
    287: lambda pk, b: _compare_cols("bscs_pre_activation", "PUK1", "!=", "PUK2", pk, b,
                                      category="COHERENCE_SEMANTIQUE"),
    288: lambda pk, b: _both("bscs_pre_activation", "ICCID", "IMSI", pk, b),
    289: lambda pk, b: _both("bscs_pre_activation", "TRANSPORTKEY", "ENCRYPTIONKEY", pk, b),
    363: lambda pk, b: _notnull("bscs_pre_activation", "ADM1", pk, b),
    364: lambda pk, b: _notnull("bscs_pre_activation", "OFFRE", pk, b),
    472: lambda pk, b: _regex("bscs_pre_activation", "ELECTRICPROFILE", pk, b, "^EP[0-9]+$"),
    473: lambda pk, b: _regex("bscs_pre_activation", "GRAPHICPROFILE", pk, b, "^GP[0-9]+$"),

    # ── BSCS_RESOURCE_DIRECTORY ───────────────────────────────────────────────
    36:  lambda pk, b: _in_domain("bscs_resource_directory", "RESOURCE_TYPE", pk, b,
                                   "MSISDN,SIM,PORT,IMEI"),
    38:  lambda pk, b: _compare_curdate("bscs_resource_directory", "DN_MODDATE", "<=", pk, b),
    242: lambda pk, b: _notnull("bscs_resource_directory", "CONTRACT_ID", pk, b),
    290: lambda pk, b: _compare_val("bscs_resource_directory", "ID_RESOURCE", ">", 0, pk, b),
    291: lambda pk, b: _regex("bscs_resource_directory", "DNCODE", pk, b, "^[A-Z0-9]{3,10}$"),
    292: lambda pk, b: _compare_val("bscs_resource_directory", "PRODUCT_ID", ">", 0, pk, b),
    477: lambda pk, b: _regex("bscs_resource_directory", "MSISDN", pk, b,
                               "^(216|212|213|33)[0-9]{8,9}$"),
    478: lambda pk, b: _in_domain("bscs_resource_directory", "RESOURCESTATESYMBOL", pk, b,
                                   "ACTIVE,INACTIVE,PENDING,SUSPENDED,BLOCKED"),

    # ── BSCS_RESOURCE_PORT ────────────────────────────────────────────────────
    39:  lambda pk, b: _in_domain("bscs_resource_port", "TYPERESOURCE", pk, b,
                                   "ETHERNET,FIBER,DSL"),
    40:  lambda pk, b: _compare_curdate("bscs_resource_port", "PORT_MODDATE", "<=", pk, b),
    41:  lambda pk, b: _regex("bscs_resource_port", "IMEI", pk, b, "^[0-9]{15}$"),
    42:  lambda pk, b: _regex("bscs_resource_port", "MACADDRESS", pk, b,
                               "^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$"),
    238: lambda pk, b: _notnull("bscs_resource_port", "CONTRACT_ID", pk, b),
    293: lambda pk, b: _compare_val("bscs_resource_port", "ID_RESOURCE", ">", 0, pk, b),
    294: lambda pk, b: _both("bscs_resource_port", "IMEI", "MACADDRESS", pk, b),
    295: lambda pk, b: _compare_val("bscs_resource_port", "SM_ID", ">", 0, pk, b),

    # ── BSCS_RESOURCE_SIM ─────────────────────────────────────────────────────
    43:  lambda pk, b: _in_domain("bscs_resource_sim", "SIMTYPE", pk, b,
                                   "NANO,MICRO,STANDARD"),
    44:  lambda pk, b: _compare_curdate("bscs_resource_sim", "SM_MODDATE", "<=", pk, b),
    45:  lambda pk, b: _in_domain("bscs_resource_sim", "VENDORSYMBOL", pk, b,
                                   "GEMALTO,THALES,IDEMIA"),
    233: lambda pk, b: _notnull("bscs_resource_sim", "CONTRACT_ID", pk, b),
    296: lambda pk, b: _compare_val("bscs_resource_sim", "ID_RESOURCE", ">", 0, pk, b),
    297: lambda pk, b: _both("bscs_resource_sim", "ICCID", "IMSI", pk, b),
    298: lambda pk, b: _make_result(
        *_correlate_sim_pin_puk(pk, b)[:4],
        "PIN1 cohérent avec PUK1",
        "COHERENCE_SEMANTIQUE"
    ),
    374: lambda pk, b: _notnull("bscs_resource_sim", "ADM1", pk, b),
    490: lambda pk, b: _regex("bscs_resource_sim", "IMSI", pk, b, "^[0-9]{15}$"),
    491: lambda pk, b: _regex("bscs_resource_sim", "MSISDN", pk, b,
                               "^(216|212|213|33)[0-9]{8,9}$"),
    492: lambda pk, b: _in_domain("bscs_resource_sim", "VENDORSYMBOL", pk, b,
                                   "GEMALTO,THALES,IDEMIA"),

    # ── BSCS_SERVICES ─────────────────────────────────────────────────────────
    46:  lambda pk, b: _compare_cols("bscs_services", "COMMITMENTFROM", "<=", "COMMITMENTTO", pk, b),
    47:  lambda pk, b: _compare_cols("bscs_services", "VALID_FROM_DATE", "<=", "COMMITMENTFROM", pk, b),
    48:  lambda pk, b: _in_domain("bscs_services", "STATUS", pk, b,
                                   "ACTIVE,SUSPENDED,TERMINATED,PENDING"),
    49:  lambda pk, b: _in_domain("bscs_services", "TYPE_PRODUCT", pk, b,
                                   "INTERNET,VOICE,DATA,TV,VOICE_DATA,INTERNATIONAL"),
    50:  lambda pk, b: _compare_val("bscs_services", "CUSTOMPRICEVALUE", ">=", 0, pk, b),
    197: lambda pk, b: _notnull("bscs_services", "CONTRACT_ID", pk, b),
    198: lambda pk, b: _notnull("bscs_services", "CUSTOMER_ID", pk, b),
    300: lambda pk, b: _compare_val("bscs_services", "ID_SERVICE", ">", 0, pk, b),
    301: lambda pk, b: _compare_cols("bscs_services", "COMMITMENTTO", ">=", "COMMITMENTFROM", pk, b),
    341: lambda pk, b: _compare_cols("bscs_services", "VALIDTO_PRICE", ">=", "VALIDFROM_PRICE", pk, b),
    343: lambda pk, b: _regex("bscs_services", "RATEPLAN", pk, b, "^RATE_PLAN_[A-Z]+$"),
    494: lambda pk, b: _compare_cols("bscs_services", "ENTRY_DATE", "<=", "VALID_FROM_DATE", pk, b),
    495: lambda pk, b: _compare_val("bscs_services", "CUSTOMPRICEVALUE_OT", ">=", 0, pk, b),
    497: lambda pk, b: _like("bscs_services", "RATEPLAN", pk, b, "RATE_PLAN_%"),
    498: lambda pk, b: _notnull("bscs_services", "SERVICE_SHDES", pk, b),

    # ── BSCS_SERVICES_PARAMETER ───────────────────────────────────────────────
    51:  lambda pk, b: _compare_curdate("bscs_services_parameter", "PRM_VALID_FROM", "<=", pk, b),
    52:  lambda pk, b: _compare_val("bscs_services_parameter", "PRM_NO", ">", 0, pk, b),
    265: lambda pk, b: _notnull("bscs_services_parameter", "CONTRACT_ID", pk, b),
    302: lambda pk, b: _compare_val("bscs_services_parameter", "PARAMETER_ID", ">", 0, pk, b),
    303: lambda pk, b: _both("bscs_services_parameter", "PRM_NO", "PRM_SHDES", pk, b),
    304: lambda pk, b: _notnull_if("bscs_services_parameter", "PRM_VALUE", "PRM_DES", pk, b),
    503: lambda pk, b: _regex("bscs_services_parameter", "SCCODE", pk, b, "^SC[0-9]+$"),

    # ── BSCS_SOUSCRIPTION ─────────────────────────────────────────────────────
    53:  lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_ENTDATE", "<=", "VALID_FROM", pk, b),
    54:  lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_SIGNED", "<=", "CONTRACT_ENTDATE", pk, b),
    55:  lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_FIRST_ACTIVATED", ">=",
                                      "CONTRACT_SIGNED", pk, b),
    56:  lambda pk, b: _in_domain("bscs_souscription", "CONTRACT_STATUS", pk, b,
                                   "ACTIVE,SUSPENDED,TERMINATED,PENDING"),
    57:  lambda pk, b: _like("bscs_souscription", "CONTRACT_CODE", pk, b, "CTR%"),
    58:  lambda pk, b: _in_domain("bscs_souscription", "CUG_STATUS", pk, b,
                                   "ACTIVE,INACTIVE"),
    158: lambda pk, b: _notnull("bscs_souscription", "CONTRACT_ID", pk, b),
    159: lambda pk, b: _notnull("bscs_souscription", "CUSTOMER_ID", pk, b),
    160: lambda pk, b: _notnull("bscs_souscription", "VALID_FROM", pk, b),
    161: lambda pk, b: _compare_curdate("bscs_souscription", "VALID_FROM", "<=", pk, b),
    165: lambda pk, b: _regex("bscs_souscription", "CONTRACT_CODE", pk, b, "^CTR[A-Z0-9-]+$"),
    166: lambda pk, b: _in_domain("bscs_souscription", "BILLINGCYCLE", pk, b,
                                   "MONTHLY,QUARTERLY,ANNUAL"),
    167: lambda pk, b: _regex("bscs_souscription", "CURRENCY", pk, b, "^[A-Z]{3}$"),
    344: lambda pk, b: _compare_cols("bscs_souscription", "LAST_BILLED_DATE", ">=",
                                      "CONTRACT_FIRST_ACTIVATED", pk, b),
    345: lambda pk, b: _compare_cols("bscs_souscription", "CUG_ACTIVE_DATE", "<=",
                                      "CUG_DEACTIVE_DATE", pk, b),
    346: lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_INSTALLED", ">=",
                                      "CONTRACT_SIGNED", pk, b),
    506: lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_ENTDATE", ">=",
                                      "CONTRACT_SIGNED", pk, b),
    507: lambda pk, b: _compare_cols("bscs_souscription", "CONTRACT_INSTALLED", ">=",
                                      "CONTRACT_ENTDATE", pk, b),
    510: lambda pk, b: _compare_cols("bscs_souscription", "CO_MODDATE", ">=", "VALID_FROM", pk, b),

    # ── CARRY_OVER ────────────────────────────────────────────────────────────
    59:  lambda pk, b: _compare_val("carry_over", "CO_AMNT", ">=", 0, pk, b),
    60:  lambda pk, b: _compare_val("carry_over", "CO_DATA", ">=", 0, pk, b),
    61:  lambda pk, b: _compare_val("carry_over", "CO_SMS", ">=", 0, pk, b),
    62:  lambda pk, b: _compare_val("carry_over", "CO_VOIX", ">=", 0, pk, b),
    63:  lambda pk, b: _compare_curdate("carry_over", "EXPIRE_DATE_CARRYOVER", ">=", pk, b),
    248: lambda pk, b: _notnull("carry_over", "FORFAIT", pk, b),
    305: lambda pk, b: _compare_val("carry_over", "CO_ID", ">", 0, pk, b),
    307: lambda pk, b: _or_notnull("carry_over", "CO_AMNT", "CO_DATA", pk, b),

    # ── CONTRACT_HISTORY ──────────────────────────────────────────────────────
    64:  lambda pk, b: _compare_curdate("contract_history", "CH_VALID_FROM", "<=", pk, b),
    65:  lambda pk, b: _in_domain("contract_history", "CH_STATUT", pk, b,
                                   "ACTIVE,SUSPENDED,TERMINATED,PENDING"),
    249: lambda pk, b: _notnull("contract_history", "CONTRACT_ID", pk, b),
    252: lambda pk, b: _notnull("contract_history", "CH_VALID_FROM", pk, b),
    308: lambda pk, b: _compare_val("contract_history", "HISTORY_ID", ">", 0, pk, b),
    309: lambda pk, b: _notnull("contract_history", "CH_REASON", pk, b),
    310: lambda pk, b: _in_domain("contract_history", "CH_STATUT", pk, b,
                                   "ACTIVE,SUSPENDED,TERMINATED,PENDING"),
    538: lambda pk, b: _in_domain("contract_history", "CH_STATUT", pk, b,
                                   "ACTIVE,SUSPENDED,TERMINATED,PENDING"),

    # ── IXC_DUNPROCESS ────────────────────────────────────────────────────────
    66:  lambda pk, b: _compare_val("ixc_dunprocess", "AMOUNT", ">=", 0, pk, b),
    67:  lambda pk, b: _compare_val("ixc_dunprocess", "STEP", ">=", 0, pk, b),
    68:  lambda pk, b: _compare_cols("ixc_dunprocess", "STEP_DUE_DATE", ">=", "STEP_START_DATE", pk, b),
    69:  lambda pk, b: _compare_cols("ixc_dunprocess", "STARTDATE", "<=", "STEP_START_DATE", pk, b),
    70:  lambda pk, b: _compare_cols("ixc_dunprocess", "STEP_AMOUNT", "<=", "AMOUNT", pk, b),
    253: lambda pk, b: _notnull("ixc_dunprocess", "CUSTOMER_ID", pk, b),
    254: lambda pk, b: _notnull("ixc_dunprocess", "FACTURE_ID", pk, b),
    311: lambda pk, b: _compare_cols("ixc_dunprocess", "CURR_DUN_STEP", ">=", "STEP", pk, b),
    312: lambda pk, b: _compare_val("ixc_dunprocess", "STEP_AMOUNT", ">", 0, pk, b),
    313: lambda pk, b: _notnull("ixc_dunprocess", "DUN_SCENARIO", pk, b),
    524: lambda pk, b: _compare_val("ixc_dunprocess", "ID_PROCESSUS_DUN", ">", 0, pk, b),

    # ── IXC_PAYMENT_PLAN ──────────────────────────────────────────────────────
    71:  lambda pk, b: _compare_val("ixc_payment_plan", "INSTALLEMENT_AMOUNT", ">", 0, pk, b),
    72:  lambda pk, b: _compare_cols("ixc_payment_plan", "INSTALLMENET_CREATE_DATE", "<=",
                                      "INSTALLMENET_DUE_DATE", pk, b),
    73:  lambda pk, b: _in_domain("ixc_payment_plan", "INSTALLEMENT_STATUS", pk, b,
                                   "PAID,PENDING,OVERDUE,CANCELLED"),
    74:  lambda pk, b: _compare_curdate("ixc_payment_plan", "INSTALLMENET_DUE_DATE", ">=", pk, b),
    263: lambda pk, b: _notnull("ixc_payment_plan", "CUSTOMER_ID", pk, b),
    264: lambda pk, b: _notnull("ixc_payment_plan", "INVOICE_REFERENCE", pk, b),
    314: lambda pk, b: _compare_val("ixc_payment_plan", "ID_INSTALLEMENT", ">", 0, pk, b),
    315: lambda pk, b: _compare_val("ixc_payment_plan", "ID_PAYMENT_PLAN", ">", 0, pk, b),
    316: lambda pk, b: _make_result(
        *_correlate_payment_plan_status(pk, b)[:4],
        "INSTALLEMENT_STATUS cohérent avec INSTALLMENET_DUE_DATE",
        "COHERENCE_SEMANTIQUE"
    ),
}

def _correlate_currency_desc(pk, b):
    bw, bp = _batch_where(pk, b)
    w = (
        "ba.`CURRENCY_ID` IS NOT NULL "
        "AND ba.`CURRENCY_DESC` IS NOT NULL "
        "AND mc.`id` IS NOT NULL "
        "AND ba.`CURRENCY_DESC` != mc.`code_iso`"
    )
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_billing_account` ba LEFT JOIN `map_currency` mc ON ba.`CURRENCY_ID` = mc.`id`"
    return (
        f"SELECT COUNT(*) FROM {t} WHERE {w}",
        f"SELECT ba.`{pk}` FROM {t} WHERE {w}",
        bp, bp
    )
DIFF_CATALOG = {

    # ── BSCS_BILLING_ACCOUNT ↔ BSCS_CUSTOMER ─────────────────────────────────
    1:   lambda pk, b: _diff_fk("bscs_billing_account", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    2: lambda pk, b: _diff_equal("bscs_billing_account", "CURRENCY_DESC",
                              "bscs_customer", "CURRENCY",
                              "CUSTOMER_ID", pk, b,
                              label="Billing account currency ISO must match customer currency ISO"),
    3:   lambda pk, b: _diff_compare("bscs_billing_account", "BA_VERS_VALID_FROM", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    4:   lambda pk, b: _diff_compare("bscs_billing_account", "BA_VERS_VALID_FROM", "<=",
                                      "bscs_customer", "VALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    203: lambda pk, b: _diff_compare("bscs_billing_account", "BA_ENTRY_DATE", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    204: lambda pk, b: _diff_compare("bscs_billing_account", "LAST_BILLED_DATE", "<=",
                                      "bscs_customer", "VALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    

    244: lambda pk, b: _diff_fk("bscs_billing_account", "CURRENCY_ID",
                             "map_currency", "id", pk, b,
                             label="CURRENCY_ID must exist in map_currency.id"),
    # ── BSCS_BILLING_ACCOUNT ↔ BSCS_PLACE ────────────────────────────────────
    224: lambda pk, b: _diff_fk("bscs_billing_account", "BILLING_ACCOUNT_ID",
                                 "bscs_place", "BILLING_ACCOUNT_ID", pk, b),

    # ── BSCS_BILLING_ACCOUNT_ASSIGN ──────────────────────────────────────────
    6:   lambda pk, b: _diff_equal("bscs_billing_account_assign", "CUSTOMER_ID",
                                    "bscs_billing_account", "CUSTOMER_ID",
                                    "BILLING_ACCOUNT_ID", pk, b),
    7:   lambda pk, b: _diff_fk("bscs_billing_account_assign", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    225: lambda pk, b: _diff_fk("bscs_billing_account_assign", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    251: lambda pk, b: _diff_fk("bscs_billing_account_assign", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    252: lambda pk, b: _diff_fk("bscs_billing_account_assign", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),

    # ── BSCS_CHARGE ───────────────────────────────────────────────────────────
    8:   lambda pk, b: _diff_fk("bscs_charge", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    9:   lambda pk, b: _diff_compare("bscs_charge", "VALID_FROM", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    10:  lambda pk, b: _diff_compare("bscs_charge", "VALID_FROM", "<=",
                                      "bscs_customer", "VALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    44:  lambda pk, b: _diff_compare("bscs_charge", "VALID_FROM", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),
    95:  lambda pk, b: _diff_fk("bscs_charge", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    161: lambda pk, b: _diff_compare("bscs_charge", "AMOUNT", "<=",
                                      "bscs_services", "CUSTOMPRICEVALUE",
                                      "SPCODE", "ID_SPCODE", pk, b),
    162: lambda pk, b: _diff_compare("bscs_charge", "VALID_FROM", ">=",
                                      "bscs_services", "VALID_FROM_DATE",
                                      "SPCODE", "ID_SPCODE", pk, b),
    163: lambda pk, b: _diff_fk("bscs_charge", "SPCODE",
                                 "bscs_services", "ID_SPCODE", pk, b),
    235: lambda pk, b: _diff_compare("bscs_charge", "VALID_FROM", ">=",
                                      "bscs_billing_account", "BA_VERS_VALID_FROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    249: lambda pk, b: _diff_ref("bscs_charge", "CURRENCY",
                                  "map_currency", "code_iso", pk, b),

    # ── BSCS_CUSTOMER ─────────────────────────────────────────────────────────
    47:  lambda pk, b: _diff_equal("bscs_customer", "COUNTRY",
                                    "bscs_place", "COUNTRY",
                                    "CUSTOMER_ID", pk, b),
    214: lambda pk, b: _diff_ref("bscs_customer", "COUNTRYCODEOFBIRTH",
                                  "map_country", "code_iso3", pk, b),
    241: lambda pk, b: _diff_ref("bscs_customer", "COUNTRY",
                                  "map_country", "nom_en", pk, b),
    242: lambda pk, b: _diff_ref("bscs_customer", "CURRENCY",
                                  "map_currency", "code_iso", pk, b),

    # ── BSCS_CUSTOMER_TAX_EXEMPT ──────────────────────────────────────────────
    12:  lambda pk, b: _diff_fk("bscs_customer_tax_exempt", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    13:  lambda pk, b: _diff_compare("bscs_customer_tax_exempt", "VALID_FROM", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    14:  lambda pk, b: _diff_compare("bscs_customer_tax_exempt", "VALID_FROM", "<=",
                                      "bscs_customer", "VALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    154: lambda pk, b: _diff_compare("bscs_customer_tax_exempt", "VALID_FROM", ">=",
                                      "bscs_customer", "CREDITPROFILEVALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    155: lambda pk, b: _diff_compare("bscs_customer_tax_exempt", "VALID_FROM", "<=",
                                      "bscs_customer", "CREDITPROFILEVALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),

    # ── BSCS_FINDOCS ──────────────────────────────────────────────────────────
    71:  lambda pk, b: _diff_fk("bscs_findocs", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    137: lambda pk, b: _diff_fk("bscs_findocs", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    216: lambda pk, b: _diff_ref("bscs_findocs", "CUSTOMER_COUNTRY",
                                  "map_country", "nom_en", pk, b),
    246: lambda pk, b: _diff_ref("bscs_findocs", "CUSTOMER_COUNTRY",
                                  "map_country", "nom_en", pk, b),
    247: lambda pk, b: _diff_ref("bscs_findocs", "CURRENCY",
                                  "map_currency", "code_iso", pk, b),

    # ── BSCS_MEMOS ────────────────────────────────────────────────────────────
    15:  lambda pk, b: _diff_fk("bscs_memos", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    16:  lambda pk, b: _diff_fk("bscs_memos", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    17:  lambda pk, b: _diff_compare("bscs_memos", "CREATED_DATE", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    236: lambda pk, b: _diff_compare("bscs_memos", "CREATED_DATE", ">=",
                                      "bscs_souscription", "CONTRACT_ENTDATE",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),

    # ── BSCS_PAYMENTS ─────────────────────────────────────────────────────────
    67:  lambda pk, b: _diff_fk("bscs_payments", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    142: lambda pk, b: _diff_fk("bscs_payments", "PAYMENT_REFERENCE",
                                 "bscs_findocs", "REFERENCE", pk, b),
    238: lambda pk, b: _diff_compare("bscs_payments", "PAYMENT_DATE", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    248: lambda pk, b: _diff_ref("bscs_payments", "PAYMENT_CURRENCY",
                                  "map_currency", "code_iso", pk, b),

    # ── BSCS_PAYMENTS_DET ────────────────────────────────────────────────────
    68:  lambda pk, b: _diff_fk("bscs_payments_det", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    69:  lambda pk, b: _diff_fk("bscs_payments_det", "PAYMENT_ID",
                                 "bscs_payments", "PAYMENT_ID", pk, b),
    70:  lambda pk, b: _diff_fk("bscs_payments_det", "ID_FINDOC",
                                 "bscs_findocs", "ID_FINDOC", pk, b),
    178: lambda pk, b: _make_result(
        *_sum_payments_vs_findocs(pk, b)[:4],
        "SUM(AMOUNT_PAID) <= INITAMOUNT par facture",
        "COHERENCE_SEMANTIQUE"
    ),
    239: lambda pk, b: _diff_compare("bscs_payments_det", "AMOUNT_PAID", "<=",
                                      "bscs_charge", "AMOUNT",
                                      "ID_FINDOC", "ID_FINDOC", pk, b),

    # ── BSCS_PLACE ────────────────────────────────────────────────────────────
    18:  lambda pk, b: _diff_fk("bscs_place", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    19:  lambda pk, b: _diff_fk("bscs_place", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    20:  lambda pk, b: _diff_equal("bscs_place", "COUNTRY",
                                    "bscs_customer", "COUNTRY",
                                    "CUSTOMER_ID", pk, b),
    245: lambda pk, b: _diff_ref("bscs_place", "COUNTRY",
                                  "map_country", "nom_en", pk, b),

    # ── BSCS_PORTABILITY_HIST ─────────────────────────────────────────────────
    21:  lambda pk, b: _diff_fk("bscs_portability_hist", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    22:  lambda pk, b: _diff_fk("bscs_portability_hist", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    166: lambda pk, b: _diff_equal("bscs_portability_hist", "DN_NUM",
                                    "bscs_resource_directory", "MSISDN",
                                    "CONTRACT_ID", pk, b),
    167: lambda pk, b: _diff_fk("bscs_portability_hist", "CONTRACT_ID",
                                 "bscs_resource_directory", "CONTRACT_ID", pk, b),
    237: lambda pk, b: _diff_compare("bscs_portability_hist", "PORTING_DATE", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),

    # ── BSCS_PORTABILITY_IN ───────────────────────────────────────────────────
    23:  lambda pk, b: _diff_fk("bscs_portability_in", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    24:  lambda pk, b: _diff_fk("bscs_portability_in", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    168: lambda pk, b: _diff_equal("bscs_portability_in", "DN_NUM",
                                    "bscs_resource_directory", "MSISDN",
                                    "CONTRACT_ID", pk, b),
    169: lambda pk, b: _diff_fk("bscs_portability_in", "CONTRACT_ID",
                                 "bscs_resource_directory", "CONTRACT_ID", pk, b),

    # ── BSCS_PRE_ACTIVATION ───────────────────────────────────────────────────
    151: lambda pk, b: _diff_fk("bscs_pre_activation", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    157: lambda pk, b: _diff_equal("bscs_pre_activation", "IMSI",
                                    "bscs_resource_sim", "IMSI",
                                    "ICCID", pk, b),
    158: lambda pk, b: _diff_equal("bscs_pre_activation", "MSISDN",
                                    "bscs_resource_sim", "MSISDN",
                                    "ICCID", pk, b),
    159: lambda pk, b: _diff_equal("bscs_pre_activation", "PIN1",
                                    "bscs_resource_sim", "PIN1",
                                    "ICCID", pk, b),
    160: lambda pk, b: _diff_equal("bscs_pre_activation", "PUK1",
                                    "bscs_resource_sim", "PUK1",
                                    "ICCID", pk, b),
    231: lambda pk, b: _diff_equal("bscs_pre_activation", "OFFRE",
                                    "bscs_services", "SERVICE_SHDES",
                                    "CONTRACT_ID", pk, b),
    259: lambda pk, b: _diff_fk("bscs_pre_activation", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    260: lambda pk, b: _diff_fk("bscs_pre_activation", "ICCID",
                                 "bscs_resource_sim", "ICCID", pk, b),

    # ── BSCS_RESOURCE_DIRECTORY ───────────────────────────────────────────────
    112: lambda pk, b: _diff_fk("bscs_resource_directory", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    209: lambda pk, b: _diff_equal("bscs_resource_directory", "PRODUCT_ID",
                                    "bscs_services", "ID_SPCODE",
                                    "CONTRACT_ID", pk, b),
    210: lambda pk, b: _diff_equal("bscs_resource_directory", "RESOURCE_TYPE",
                                    "bscs_services", "TYPE_PRODUCT",
                                    "CONTRACT_ID", pk, b),
    228: lambda pk, b: _diff_equal("bscs_resource_directory", "MSISDN",
                                    "bscs_resource_sim", "MSISDN",
                                    "CONTRACT_ID", pk, b),
    229: lambda pk, b: _diff_fk("bscs_resource_directory", "CONTRACT_ID",
                                 "bscs_resource_sim", "CONTRACT_ID", pk, b),
    256: lambda pk, b: _diff_fk("bscs_resource_directory", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),

    # ── BSCS_RESOURCE_PORT ────────────────────────────────────────────────────
    114: lambda pk, b: _diff_fk("bscs_resource_port", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    207: lambda pk, b: _diff_equal("bscs_resource_port", "PRODUCT_ID",
                                    "bscs_services", "ID_SPCODE",
                                    "CONTRACT_ID", pk, b),
    208: lambda pk, b: _diff_equal("bscs_resource_port", "TYPERESOURCE",
                                    "bscs_services", "TYPE_PRODUCT",
                                    "CONTRACT_ID", pk, b),
    230: lambda pk, b: _diff_equal("bscs_resource_port", "CONTRACT_ID",
                                    "bscs_resource_sim", "CONTRACT_ID",
                                    "CONTRACT_ID", pk, b),
    257: lambda pk, b: _diff_fk("bscs_resource_port", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),

    # ── BSCS_RESOURCE_SIM ─────────────────────────────────────────────────────
    116: lambda pk, b: _diff_fk("bscs_resource_sim", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    205: lambda pk, b: _diff_equal("bscs_resource_sim", "PRODUCT_ID",
                                    "bscs_services", "ID_SPCODE",
                                    "CONTRACT_ID", pk, b),
    206: lambda pk, b: _diff_fk("bscs_resource_sim", "CONTRACT_ID",
                                 "bscs_services", "CONTRACT_ID", pk, b),
    258: lambda pk, b: _diff_fk("bscs_resource_sim", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),

    # ── BSCS_SERVICES ─────────────────────────────────────────────────────────
    28:  lambda pk, b: _diff_fk("bscs_services", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    29:  lambda pk, b: _diff_fk("bscs_services", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    30:  lambda pk, b: _diff_fk("bscs_services", "PLACE_ID",
                                 "bscs_place", "PLACE_ID", pk, b),
    31:  lambda pk, b: _diff_fk("bscs_services", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    43:  lambda pk, b: _diff_compare("bscs_services", "VALID_FROM_DATE", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    212: lambda pk, b: _diff_compare("bscs_services", "VALID_FROM_DATE", ">=",
                                      "bscs_place", "VALIDFROM",
                                      "PLACE_ID", "PLACE_ID", pk, b),
    234: lambda pk, b: _diff_compare("bscs_services", "VALID_FROM_DATE", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),
    253: lambda pk, b: _diff_fk("bscs_services", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    254: lambda pk, b: _diff_fk("bscs_services", "PLACE_ID",
                                 "bscs_place", "PLACE_ID", pk, b),

    # ── BSCS_SERVICES_PARAMETER ───────────────────────────────────────────────
    32:  lambda pk, b: _diff_fk("bscs_services_parameter", "CONTRACT_ID",
                                 "bscs_services", "CONTRACT_ID", pk, b),
    123: lambda pk, b: _diff_fk("bscs_services_parameter", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    164: lambda pk, b: _diff_equal("bscs_services_parameter", "SERVICE_SHDES",
                                    "bscs_services", "SERVICE_SHDES",
                                    "CONTRACT_ID", pk, b),
    226: lambda pk, b: _diff_equal("bscs_services_parameter", "ID_SERVICE_PARAMETERS",
                                    "bscs_services", "ID_SERVICE_PARAMETERS",
                                    "CONTRACT_ID", pk, b),
    255: lambda pk, b: _diff_fk("bscs_services_parameter", "CONTRACT_ID",
                                 "bscs_services", "CONTRACT_ID", pk, b),

    # ── BSCS_SOUSCRIPTION ─────────────────────────────────────────────────────
    33:  lambda pk, b: _diff_fk("bscs_souscription", "BILLING_ACCOUNT_ID",
                                 "bscs_billing_account", "BILLING_ACCOUNT_ID", pk, b),
    42:  lambda pk, b: _diff_compare("bscs_souscription", "VALID_FROM", ">=",
                                      "bscs_customer", "VALIDFROM",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    75:  lambda pk, b: _diff_fk("bscs_souscription", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    79:  lambda pk, b: _diff_compare("bscs_souscription", "VALID_FROM", "<=",
                                      "bscs_customer", "VALIDTO",
                                      "CUSTOMER_ID", "CUSTOMER_ID", pk, b),
    233: lambda pk, b: _diff_compare("bscs_souscription", "VALID_FROM", ">=",
                                      "bscs_billing_account", "BA_VERS_VALID_FROM",
                                      "BILLING_ACCOUNT_ID", "BILLING_ACCOUNT_ID", pk, b),
    250: lambda pk, b: _diff_ref("bscs_souscription", "CURRENCY",
                                  "map_currency", "code_iso", pk, b),

    # ── CARRY_OVER ────────────────────────────────────────────────────────────
    34:  lambda pk, b: _diff_fk("carry_over", "FORFAIT",
                                 "bscs_souscription", "COMMERCIAL_OFFER", pk, b),
    170: lambda pk, b: _diff_compare("carry_over", "EXPIRE_DATE_CARRYOVER", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "FORFAIT", "COMMERCIAL_OFFER", pk, b),
    232: lambda pk, b: _diff_equal("carry_over", "FORFAIT",
                                    "bscs_services", "RATEPLAN",
                                    "FORFAIT", pk, b),

    # ── CONTRACT_HISTORY ──────────────────────────────────────────────────────
    35:  lambda pk, b: _diff_fk("contract_history", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    36:  lambda pk, b: _diff_compare("contract_history", "CH_VALID_FROM", ">=",
                                      "bscs_souscription", "VALID_FROM",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),
    128: lambda pk, b: _diff_fk("contract_history", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    173: lambda pk, b: _diff_compare("contract_history", "CH_VALID_FROM", ">=",
                                      "bscs_souscription", "CONTRACT_ENTDATE",
                                      "CONTRACT_ID", "CONTRACT_ID", pk, b),

    # ── IXC_DUNPROCESS ────────────────────────────────────────────────────────
    37:  lambda pk, b: _diff_fk("ixc_dunprocess", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    38:  lambda pk, b: _diff_fk("ixc_dunprocess", "FACTURE_ID",
                                 "bscs_findocs", "ID_FINDOC", pk, b),
    131: lambda pk, b: _diff_fk("ixc_dunprocess", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),

    # ── IXC_PAYMENT_PLAN ──────────────────────────────────────────────────────
    39:  lambda pk, b: _diff_fk("ixc_payment_plan", "ID_PROCESSUS_DUN",
                                 "ixc_dunprocess", "ID_PROCESSUS_DUN", pk, b),
    40:  lambda pk, b: _diff_fk("ixc_payment_plan", "INVOICE_REFERENCE",
                                 "bscs_findocs", "REFERENCE", pk, b),
    41:  lambda pk, b: _diff_fk("ixc_payment_plan", "CUSTOMER_ID",
                                 "bscs_customer", "CUSTOMER_ID", pk, b),
    135: lambda pk, b: _diff_fk("ixc_payment_plan", "CONTRACT_ID",
                                 "bscs_souscription", "CONTRACT_ID", pk, b),
    174: lambda pk, b: _diff_compare("ixc_payment_plan", "INSTALLEMENT_AMOUNT", "<=",
                                      "ixc_dunprocess", "STEP_AMOUNT",
                                      "ID_PROCESSUS_DUN", "ID_PROCESSUS_DUN", pk, b),
    175: lambda pk, b: _diff_compare("ixc_payment_plan", "INSTALLMENET_DUE_DATE", "<=",
                                      "ixc_dunprocess", "STEP_DUE_DATE",
                                      "ID_PROCESSUS_DUN", "ID_PROCESSUS_DUN", pk, b),

  
}

def _correlate_ba_statut_valid_from(pk, b):
    bw, bp = _batch_where(pk, b)
    w = "`BA_VERS_STATUT` = 'ACTIVE' AND `BA_VERS_VALID_FROM` IS NOT NULL AND `BA_VERS_VALID_FROM` > CURDATE()"
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_bill_medium_desc(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("`BILL_MEDIUM` IS NOT NULL AND `BILL_MEDIUM_DESC` IS NOT NULL "
         "AND UPPER(TRIM(`BILL_MEDIUM_DESC`)) != UPPER(TRIM(`BILL_MEDIUM`))")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_currency_desc(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("`CURRENCY_ID` IS NOT NULL AND `CURRENCY_DESC` IS NOT NULL "
         "AND UPPER(TRIM(`CURRENCY_DESC`)) != UPPER(TRIM(`CURRENCY_ID`))")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_billing_account`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_exempt(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("(`EXEMPT_STATUS` = 'FULL_EXEMPT' AND (`EXEMPT_RATE` IS NULL OR `EXEMPT_RATE` != 0)) "
         "OR (`EXEMPT_STATUS` = 'PARTIAL_EXEMPT' AND (`EXEMPT_RATE` IS NULL OR `EXEMPT_RATE` <= 0))")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_customer_tax_exempt`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_findocs_currency(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("`CURRENCYINITAMOUNT` IS NOT NULL AND `INITAMOUNT` IS NOT NULL "
         "AND `EXCHANGERATEVALUE` IS NOT NULL "
         "AND ABS(`CURRENCYINITAMOUNT` - `INITAMOUNT` * `EXCHANGERATEVALUE`) > 0.01")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_findocs`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_findocs_doctype(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("(`DOCTYPE` = 'IN' AND `DOCTYPE_DET` IS NOT NULL AND `DOCTYPE_DET` < 0) "
         "OR (`DOCTYPE` = 'CR' AND `DOCTYPE_DET` IS NOT NULL AND `DOCTYPE_DET` > 0)")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_findocs`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_sim_pin_puk(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("(`PIN1` IS NOT NULL AND `PIN1` != '' AND (`PUK1` IS NULL OR TRIM(`PUK1`) = '')) "
         "OR (`PUK1` IS NOT NULL AND `PUK1` != '' AND (`PIN1` IS NULL OR TRIM(`PIN1`) = ''))")
    w = f"({w}) AND {bw}" if bw else w
    t = "`bscs_resource_sim`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _correlate_payment_plan_status(pk, b):
    bw, bp = _batch_where(pk, b)
    w = ("(`INSTALLEMENT_STATUS` = 'OVERDUE' AND "
         "(`INSTALLMENET_DUE_DATE` IS NULL OR `INSTALLMENET_DUE_DATE` >= CURDATE())) "
         "OR (`INSTALLEMENT_STATUS` = 'PAID' AND "
         "(`INSTALLMENET_DUE_DATE` IS NULL OR `INSTALLMENET_DUE_DATE` > CURDATE()))")
    w = f"({w}) AND {bw}" if bw else w
    t = "`ixc_payment_plan`"
    return (f"SELECT COUNT(*) FROM {t} WHERE {w}",
            f"SELECT `{pk}` FROM {t} WHERE {w}", bp, bp)


def _sum_payments_vs_findocs(pk, b):
    """Agrégat : SUM(AMOUNT_PAID) > INITAMOUNT par facture."""
    bw, bp = _batch_where(pk, b, alias="pd")
    bw_clause = f" AND {bw}" if bw else ""
    cnt_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT pd.`ID_FINDOC`
            FROM `bscs_payments_det` pd
            INNER JOIN `bscs_findocs` f
                ON CAST(pd.`ID_FINDOC` AS CHAR) = CAST(f.`ID_FINDOC` AS CHAR)
            WHERE f.`INITAMOUNT` IS NOT NULL{bw_clause}
            GROUP BY pd.`ID_FINDOC`, f.`INITAMOUNT`
            HAVING SUM(pd.`AMOUNT_PAID`) > f.`INITAMOUNT`
        ) AS _agg
    """
    ids_sql = f"""
        SELECT pd.`{pk}`
        FROM `bscs_payments_det` pd
        INNER JOIN `bscs_findocs` f
            ON CAST(pd.`ID_FINDOC` AS CHAR) = CAST(f.`ID_FINDOC` AS CHAR)
        WHERE f.`INITAMOUNT` IS NOT NULL{bw_clause}
          AND pd.`ID_FINDOC` IN (
              SELECT sub.`ID_FINDOC`
              FROM `bscs_payments_det` sub
              INNER JOIN `bscs_findocs` sf
                  ON CAST(sub.`ID_FINDOC` AS CHAR) = CAST(sf.`ID_FINDOC` AS CHAR)
              WHERE sf.`INITAMOUNT` IS NOT NULL
              GROUP BY sub.`ID_FINDOC`, sf.`INITAMOUNT`
              HAVING SUM(sub.`AMOUNT_PAID`) > sf.`INITAMOUNT`
          )
        LIMIT {VIOLATION_FETCH_LIMIT}
    """
    return cnt_sql, ids_sql, bp, bp
def get_conn_info():
    hook = MySqlHook(mysql_conn_id="mysql_conn")
    c    = hook.get_connection("mysql_conn")
    return {
        "host":     c.host,
        "user":     c.login,
        "password": c.password,
        "database": c.schema,
        "port":     int(c.port or 3306),
    }


def new_conn(conn_info):
    return pymysql.connect(**conn_info)


def table_exists(cursor, table_name):
    cursor.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table_name,))
    return cursor.fetchone()[0] > 0


def get_primary_key(cursor, table_name):
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
          AND COLUMN_KEY = 'PRI' LIMIT 1
    """, (table_name,))
    r = cursor.fetchone()
    return r[0] if r else None


def get_batch_ids(cursor, table_name, pk_col, batch_size, offset, run_id=None, iteration_id=None):
    if not pk_col:
        return None
    if not batch_size or batch_size <= 0:
        if run_id and iteration_id:
            cursor.execute(
                f"""SELECT `{pk_col}` FROM `{table_name}`
                    WHERE CAST(`{pk_col}` AS CHAR) NOT IN (
                        SELECT pk_value FROM bscs_detection_batch_pks
                        WHERE table_name = %s
                        AND iteration_id = %s
                        AND run_id != %s
                    )
                    ORDER BY `{pk_col}`""",
                (table_name.lower(), iteration_id, run_id)
            )
            rows = cursor.fetchall()
            return [r[0] for r in rows] if rows else []
        return None
    # batch avec LIMIT/OFFSET — inchangé
    cursor.execute(
        f"""SELECT `{pk_col}` FROM `{table_name}`
            ORDER BY `{pk_col}` LIMIT %s OFFSET %s""",
        (batch_size, offset)
    )
    rows = cursor.fetchall()
    return [r[0] for r in rows]


def count_batch(cursor, table_name, pk_col, batch_ids):
    """Compte les lignes du batch courant."""
    if batch_ids is None:
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    elif len(batch_ids) == 0:
        return 0
    else:
        ph = ",".join(["%s"] * len(batch_ids))
        cursor.execute(
            f"SELECT COUNT(*) FROM `{table_name}` WHERE `{pk_col}` IN ({ph})",
            batch_ids
        )
    return cursor.fetchone()[0]


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 : init_run
# ══════════════════════════════════════════════════════════════════════════════

def init_run(**context):
    dag_run      = context["dag_run"]
    conf         = dag_run.conf or {}
    triggered_by = conf.get("triggered_by") or str(dag_run.run_type)
    run_id       = dag_run.run_id

    conn_info = get_conn_info()
    conn      = new_conn(conn_info)
    try:
        cur = conn.cursor()

        iteration_id = conf.get("iteration_id")
        if iteration_id is None:
            cur.execute("""
                SELECT COALESCE(MAX(iteration_id), 1)
                FROM bscs_iteration_config
                WHERE flag_to_check = 1
            """)
            iteration_id = cur.fetchone()[0]
        else:
            iteration_id = int(iteration_id)

        cur.execute("""
            INSERT INTO bscs_quality_run
                (run_id, iteration_id, execution_date, triggered_by, status)
            VALUES (%s, %s, %s, %s, 'RUNNING')
            ON DUPLICATE KEY UPDATE
                status='RUNNING',
                iteration_id=VALUES(iteration_id),
                execution_date=VALUES(execution_date)
        """, (run_id, iteration_id, datetime.now(), triggered_by))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    context["ti"].xcom_push(key="run_id",       value=run_id)
    context["ti"].xcom_push(key="iteration_id", value=int(iteration_id))
    logging.info(f"✅ Run initialisé : run_id={run_id} | iteration_id={iteration_id}")


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 : get_rules
# Lit BSCS_ITERATION_CONFIG et charge les règles depuis les catalogues
# ══════════════════════════════════════════════════════════════════════════════

def get_rules(**context):
    iteration_id = context["ti"].xcom_pull(key="iteration_id", task_ids="init_run")
    conn_info    = get_conn_info()
    conn         = new_conn(conn_info)

    try:
        cur = conn.cursor()

        # Charger toutes les lignes cochées pour cette itération
        cur.execute("""
            SELECT
                id, iteration_id, table_name, column_name, rule,
                flag_to_check, batch_size, offset_current,
                rule_id, rule_origin
            FROM bscs_iteration_config
            WHERE iteration_id = %s AND flag_to_check = 1
            ORDER BY table_name, rule_id
        """, (iteration_id,))
        config_rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    if not config_rows:
        logging.warning("⚠️ Aucune règle cochée dans bscs_iteration_config")
        context["ti"].xcom_push(key="config_rows", value=[])
        return

    # Sérialiser pour XCom (tuples → listes)
    serializable = [list(r) for r in config_rows]
    context["ti"].xcom_push(key="config_rows", value=serializable)
    logging.info(f"✅ {len(config_rows)} règles chargées depuis la config")


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 : detect_inconsistency
# ══════════════════════════════════════════════════════════════════════════════

def detect_inconsistency(**context):
    config_rows  = context["ti"].xcom_pull(key="config_rows",  task_ids="get_rules")
    iteration_id = context["ti"].xcom_pull(key="iteration_id", task_ids="init_run")
    run_id       = context["ti"].xcom_pull(key="run_id",       task_ids="init_run")

    if not config_rows:
        logging.warning("⚠️ Aucune règle à traiter.")
        context["ti"].xcom_push(key="resultat",        value=[])
        context["ti"].xcom_push(key="problematic_ids", value=[])
        return

    conn_info       = get_conn_info()
    rule_results    = []
    all_problematic = []
    updated_offsets = set()

    for row in config_rows:
        (cfg_id, iter_id, table_name, column_name, rule_label,
         flag, batch_size, offset_current, rule_id, rule_origin) = row

        table_lower  = table_name.strip().lower()
        rule_id_int  = int(rule_id) if rule_id is not None else -1
        origin_upper = str(rule_origin or "").strip().upper()

        catalog  = SAME_CATALOG if origin_upper == "SAME" else DIFF_CATALOG
        rule_fn  = catalog.get(rule_id_int)

        if rule_fn is None:
            logging.warning(
                f"⚠️ Règle {rule_id_int} ({origin_upper}) non trouvée "
                f"— [{table_name}] ignorée"
            )
            continue

        nb_viol      = 0
        pk_ids       = []
        count_source = 0
        # ── Initialiser lbl ici pour éviter UnboundLocalError ────────────────
        lbl          = rule_label or f"rule_{rule_id_int}"
        category     = "UNKNOWN"

        conn = None
        try:
            conn = new_conn(conn_info)
            cur  = conn.cursor()

            if not table_exists(cur, table_name):
                logging.warning(
                    f"⚠️ Table {table_name} inexistante "
                    f"— règle {rule_id_int} ignorée"
                )
                continue

            pk_col         = get_primary_key(cur, table_name)
            batch_size_int = int(batch_size  or 0)
            offset_int     = int(offset_current or 0)

            # ── Warning si pas de PK sur règle DIFF ──────────────────────────
            if pk_col is None:
                logging.warning(
                    f"⚠️ [{table_name}] Aucune PK détectée — "
                    f"règle {rule_id_int} ({origin_upper}) exécutée sans filtre batch. "
                    f"Les résultats peuvent être redondants entre runs."
                )

            # ── Récupération du batch en excluant les PKs déjà traités ───────
            batch_ids = get_batch_ids(
                cur, table_name, pk_col,
                batch_size_int, offset_int,
                run_id=run_id,
                iteration_id=iteration_id
            )

            if batch_ids is not None and len(batch_ids) == 0:
                logging.info(
                    f"[{table_name}] Batch vide à offset={offset_int} "
                    f"— règle {rule_id_int} ignorée"
                )
                continue

            count_source = count_batch(cur, table_name, pk_col, batch_ids)

            # ── Enregistrement PKs batch (une seule fois par table par run) ───
            # DOIT être après le calcul de batch_ids
            if pk_col and table_lower not in updated_offsets:
                pks_to_register = (
                    batch_ids if batch_ids is not None else []
                )

                # batch_ids=None → pas de batch configuré → charger tous les PKs
                if batch_ids is None:
                    try:
                        pc_all = new_conn(conn_info)
                        cr_all = pc_all.cursor()
                        cr_all.execute(
                            f"SELECT `{pk_col}` FROM `{table_name}`"
                        )
                        pks_to_register = [
                            str(r[0]) for r in cr_all.fetchall()
                        ]
                        cr_all.close()
                        pc_all.close()
                    except Exception as e:
                        logging.error(
                            f"❌ Erreur chargement all PKs [{table_name}]: {e}"
                        )
                        pks_to_register = []

                if pks_to_register:
                    try:
                        pc = new_conn(conn_info)
                        pr = pc.cursor()

                        # Vérifier lesquels sont déjà dans un run précédent
                        ph_chk = ",".join(["%s"] * len(pks_to_register))
                        pr.execute(
                            f"""SELECT pk_value FROM bscs_detection_batch_pks
                                WHERE table_name = %s
                                AND iteration_id = %s     # ← même itération
                                AND run_id != %s          # ← pas le run courant
                                AND pk_value IN ({ph_chk})""",
                            [table_lower, iteration_id, run_id] + [str(p) for p in pks_to_register]
                        )
                        already_done = {r[0] for r in pr.fetchall()}

                        new_pks = [
                            p for p in pks_to_register
                            if str(p) not in already_done
                        ]

                        if already_done:
                            logging.warning(
                                f"⚠️ [{table_name}] {len(already_done)} PKs "
                                f"déjà traités dans un run précédent → exclus"
                            )

                        if new_pks:
                            pr.executemany(
                                """INSERT IGNORE INTO bscs_detection_batch_pks
                                   (run_id, iteration_id, table_name, pk_value)
                                   VALUES (%s, %s, %s, %s)""",
                                [
                                    (run_id, iteration_id, table_lower, str(pk))
                                    for pk in new_pks
                                ]
                            )
                            pc.commit()
                            logging.info(
                                f"✅ [{table_name}] {len(new_pks)} "
                                f"nouveaux PKs enregistrés"
                            )
                        else:
                            logging.info(
                                f"ℹ️ [{table_name}] Aucun nouveau PK "
                                f"(tous déjà traités)"
                            )

                        pr.close()
                        pc.close()

                    except Exception as e:
                        logging.error(
                            f"❌ Erreur enregistrement PKs [{table_name}]: {e}"
                        )

            if count_source == 0:
                logging.info(
                    f"[{table_name}] Aucune ligne dans le batch "
                    f"— règle {rule_id_int} ignorée"
                )
                continue

            # ── Construction requête via catalogue ────────────────────────────
            result = rule_fn(pk_col, batch_ids)

            cnt_sql    = result["cnt_sql"]
            ids_sql    = result["ids_sql"]
            params     = result["params"]
            ids_params = result["ids_params"]
            lbl        = result["label"]       # ← écrase la valeur par défaut
            category   = result["category"]

            # ── Exécution COUNT ───────────────────────────────────────────────
            cur.execute(cnt_sql, params)
            nb_viol = cur.fetchone()[0]

            # ── Exécution IDs si violations ───────────────────────────────────
            if nb_viol > 0 and ids_sql and pk_col:
                if f"LIMIT {VIOLATION_FETCH_LIMIT}" in ids_sql:
                    cur.execute(ids_sql, ids_params)
                else:
                    cur.execute(
                        ids_sql + f" LIMIT {VIOLATION_FETCH_LIMIT}",
                        ids_params
                    )
                pk_ids = [str(r[0]) for r in cur.fetchall()]

            logging.info(
                f"  ✅ [{origin_upper}:{rule_id_int}] [{table_name}] {lbl} "
                f"→ {nb_viol}/{count_source} violations"
            )

        except Exception as e:
            logging.error(
                f"❌ Erreur règle {rule_id_int} [{table_name}]: {e}",
                exc_info=True
            )
            nb_viol      = 0
            pk_ids       = []
            count_source = count_source or 0
            # lbl et category ont déjà leur valeur par défaut définie plus haut

        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # ── Mise à jour offset batch ──────────────────────────────────────────
        if batch_size_int > 0 and table_lower not in updated_offsets:
            if batch_ids is not None and len(batch_ids) > 0:
                try:
                    uc = new_conn(conn_info)
                    ur = uc.cursor()
                    ur.execute(
                        """UPDATE bscs_iteration_config
                           SET offset_current = offset_current + %s
                           WHERE iteration_id = %s
                             AND LOWER(table_name) = %s
                             AND flag_to_check = 1""",
                        (batch_size_int, iteration_id, table_lower)
                    )
                    uc.commit()
                    ur.close()
                    uc.close()
                    updated_offsets.add(table_lower)
                    logging.info(
                        f"📍 [{table_name}] Offset avancé de {batch_size_int} "
                        f"→ prochain offset = {offset_int + batch_size_int}"
                    )
                except Exception as e:
                    logging.error(
                        f"❌ Erreur update offset [{table_name}]: {e}"
                    )
            else:
                updated_offsets.add(table_lower)
                logging.info(
                    f"ℹ️ [{table_name}] Offset non avancé (batch vide)"
                )

        # ── Agrégation résultats ──────────────────────────────────────────────
        rule_results.append({
            "rule_id":          rule_id_int,
            "rule_origin":      origin_upper,
            "table_name":       table_name,
            "column_name":      column_name or "",
            "rule":             lbl,
            "rule_description": rule_label or "",
            "error_category":   category,
            "count_source":     count_source,
            "nb_violations":    nb_viol,
        })

        for pk in pk_ids:
            all_problematic.append({
                "table_name":   table_name,
                "column_name":  column_name or "",
                "rule_label":   lbl,
                "row_pk_value": str(pk),
            })

    context["ti"].xcom_push(key="resultat",        value=rule_results)
    context["ti"].xcom_push(key="problematic_ids", value=all_problematic)
    logging.info(
        f"🎉 Détection terminée : {len(rule_results)} règles "
        f"| {len(all_problematic)} lignes problématiques"
    )
def save_result(**context):
    run_id          = context["ti"].xcom_pull(key="run_id",          task_ids="init_run")
    iteration_id    = context["ti"].xcom_pull(key="iteration_id",    task_ids="init_run")
    resultat        = context["ti"].xcom_pull(key="resultat",        task_ids="detect_inconsistency")
    problematic_ids = context["ti"].xcom_pull(key="problematic_ids", task_ids="detect_inconsistency")

    if not resultat:
        logging.warning("Aucun résultat à sauvegarder.")  # ✅ FIXED: logger → logging
        logging.info("Sauvegarde terminée pour run_id=%s", run_id)
        _finalise_run(run_id, iteration_id, status="SUCCESS", resultat=[])
        return

    execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn_info      = get_conn_info()
    conn           = new_conn(conn_info)

    try:
        cur = conn.cursor()

        cur.execute("DELETE FROM bscs_detected_inconsistency WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM bscs_problematic_rows       WHERE run_id = %s", (run_id,))
        conn.commit()

        cur.executemany("""
            INSERT INTO bscs_detected_inconsistency
                (run_id, iteration_id, execution_date,
                table_name, column_name,
                rule, rule_description, error_category,
                rule_id, rule_origin,
                count_source, nb_violations,
                nb_to_correct, nb_to_migrate, taux_rejet,
                nb_to_correct_after, nb_violations_after, taux_rejet_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [(
            run_id, iteration_id, execution_date,
            r["table_name"], r["column_name"],
            r["rule"] or "RULE_INCONNUE",
            r["rule_description"], r["error_category"],
            r.get("rule_id"),
            r.get("rule_origin"),
            r["count_source"], r["nb_violations"],
            r["nb_violations"],   # nb_to_correct
            0,                    # nb_to_migrate
            0.0,                  # taux_rejet
            0,                    # nb_to_correct_after
            0,                    # nb_violations_after
            0.0,                  # taux_rejet_after
        ) for r in resultat])
        conn.commit()

        if problematic_ids:
            cur.executemany("""
                INSERT IGNORE INTO bscs_problematic_rows
                    (run_id, iteration_id, table_name, column_name,
                    rule_label, row_pk_value, detection_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [(
                run_id, iteration_id,
                p["table_name"], p["column_name"],
                p["rule_label"], p["row_pk_value"],
                execution_date,
            ) for p in problematic_ids])
            conn.commit()

        count_source_par_table = {}
        for r in resultat:
            t = r["table_name"]
            if t not in count_source_par_table:
                count_source_par_table[t] = r["count_source"]

        cur.execute("""
            SELECT table_name, COUNT(DISTINCT row_pk_value) AS lignes_en_erreur
            FROM bscs_problematic_rows
            WHERE run_id = %s
            GROUP BY table_name
        """, (run_id,))
        erreur_par_table = {row[0]: row[1] for row in cur.fetchall()}

        for tname, src in count_source_par_table.items():
            en_erreur = erreur_par_table.get(tname, 0)
            lignes_ok = max(src - en_erreur, 0)
            taux      = round(en_erreur * 100.0 / src, 2) if src > 0 else 0.0
            cur.execute("""
                UPDATE bscs_detected_inconsistency
                SET nb_to_correct = %s,
                    nb_to_migrate = %s,
                    taux_rejet    = %s
                WHERE run_id = %s AND table_name = %s
            """, (en_erreur, lignes_ok, taux, run_id, tname))

        conn.commit()

    finally:
        cur.close()
        conn.close()

    _finalise_run(run_id, iteration_id, status="SUCCESS", resultat=resultat)
    logging.info("Sauvegarde terminée pour run_id=%s", run_id)  # ✅ FIXED: logger → logging
def _finalise_run(run_id, iteration_id, status, resultat=None, error_message=None):
    nb_violations = sum(r["nb_violations"] for r in resultat) if resultat else 0
    nb_tables     = len({r["table_name"]   for r in resultat}) if resultat else 0
    nb_rules      = len(resultat) if resultat else 0

    conn_info = get_conn_info()
    conn      = new_conn(conn_info)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE bscs_quality_run
            SET status=%s, nb_tables=%s, nb_rules=%s,
                nb_violations=%s, error_message=%s
            WHERE run_id=%s
        """, (status, nb_tables, nb_rules, nb_violations, error_message, run_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()
    logging.info(
        f"📝 Run finalisé — {status} | "
        f"violations={nb_violations} | tables={nb_tables} | règles={nb_rules}"
    )


def handle_failure(context):
    err = str(context.get("exception", "Unknown error"))
    try:
        run_id       = context["ti"].xcom_pull(key="run_id",       task_ids="init_run")
        iteration_id = context["ti"].xcom_pull(key="iteration_id", task_ids="init_run")
        resultat     = context["ti"].xcom_pull(key="resultat",     task_ids="detect_inconsistency") or []
        _finalise_run(run_id, iteration_id, status="FAILED",
                      resultat=resultat, error_message=err[:500])
    except Exception as e:
        logging.error(f"❌ Impossible de finaliser le run : {e}")

with dag:
    t1 = PythonOperator(
        task_id="init_run",
        python_callable=init_run,
        on_failure_callback=handle_failure,
    )
    t2 = PythonOperator(
        task_id="get_rules",
        python_callable=get_rules,
        on_failure_callback=handle_failure,
    )
    t3 = PythonOperator(
        task_id="detect_inconsistency",
        python_callable=detect_inconsistency,
        on_failure_callback=handle_failure,
    )
    t4 = PythonOperator(
        task_id="save_result",
        python_callable=save_result,
        on_failure_callback=handle_failure,
    )

    t1 >> t2 >> t3 >> t4