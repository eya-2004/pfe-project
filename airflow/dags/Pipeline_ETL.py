from __future__ import annotations

import json
import logging
import re
from datetime import datetime, date
from typing import Any

import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.task_group import TaskGroup


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Map table source BSCS → table staging corrected_*
# ---------------------------------------------------------------------------
SOURCE_TO_TARGET: dict[str, str] = {
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


# ===========================================================================
# MOTEUR DE RÈGLES
# ===========================================================================

class RuleEngine:
    """
    Applique les règles de transformation sur un DataFrame pandas.
    Chaque méthode _handle_<rule_type_lower> reçoit (df, col, rvalue, rule)
    et retourne le DataFrame modifié.
    """

    def __init__(self, rules: list[dict]):
        self.rules   = rules
        self._fk_sets: dict[str, set] = {}

    def set_fk_sets(self, fk_sets: dict[str, set]) -> None:
        self._fk_sets = fk_sets

    # ------------------------------------------------------------------
    # Point d'entrée
    # ------------------------------------------------------------------
    def apply_all(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        table_rules = [r for r in self.rules if r["table_name"] == table_name]
        for rule in table_rules:
            col   = rule["column_name"]
            rtype = rule["rule_type"]
            rval  = rule.get("rule_value", "") or ""

            if col not in df.columns:
                logger.warning("[%s] Colonne '%s' absente — règle %s ignorée.", table_name, col, rule["rule_id"])
                continue

            handler = getattr(self, f"_handle_{rtype.lower()}", None)
            if handler is None:
                logger.warning("[%s] rule_type '%s' non implémenté (règle %s).", table_name, rtype, rule["rule_id"])
                continue

            try:
                df = handler(df, col, rval, rule)
                logger.info("[%s] Règle %s (%s) ✓ col='%s'", table_name, rule["rule_id"], rtype, col)
            except Exception as exc:
                logger.error("[%s] Règle %s (%s) ERREUR : %s", table_name, rule["rule_id"], rtype, exc)

        return df

    # ==================================================================
    # HANDLERS — 1 par rule_type
    # ==================================================================

    # ------------------------------------------------------------------
    # FORCE_VALUE
    # Écrase toutes les valeurs par une valeur fixe.
    # Ex: COUNTRY → "CI" | CREDITRISKRATINGINTVALUE → -1
    # Pandas: df[col] = scalar
    # ------------------------------------------------------------------
    def _handle_force_value(self, df, col, rval, rule):
        df[col] = rval
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_NULL
    # Remplace NULL et chaîne vide par la valeur par défaut.
    # Ex: FIRSTNAME NULL → "XXX"
    # Pandas: fillna + mask sur chaîne vide
    # ------------------------------------------------------------------
    def _handle_default_if_null(self, df, col, rval, rule):
        df[col] = df[col].apply(
            lambda v: rval if (v is None
                               or (isinstance(v, float) and pd.isna(v))
                               or str(v).strip() == "") else v
        )
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_EMPTY
    # Remplace les valeurs vides ou whitespace.
    # Ex: MARKETSEGMENTS vide → "Personne Privée"
    # Pandas: apply avec test strip()
    # ------------------------------------------------------------------
    def _handle_default_if_empty(self, df, col, rval, rule):
        df[col] = df[col].apply(
            lambda v: rval if (v is None or str(v).strip() == "") else v
        )
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_INVALID_EMAIL
    # Remplace les emails invalides (regex RFC5322 simplifiée).
    # Ex: EMAILSPEC invalide → "migration-ca@orange.com"
    # Pandas: apply + re.match
    # ------------------------------------------------------------------
    _EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", re.IGNORECASE)

    def _handle_default_if_invalid_email(self, df, col, rval, rule):
        def fix(v):
            if v is None or str(v).strip() == "":
                return rval
            return v if self._EMAIL_RE.match(str(v).strip()) else rval
        df[col] = df[col].apply(fix)
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_INVALID_PHONE
    # Remplace les téléphones invalides (< 8 chiffres).
    # rval vide → NULL (Individual) | rval renseigné → valeur par défaut (Org)
    # Pandas: apply + re.sub pour compter les chiffres
    # ------------------------------------------------------------------
    def _handle_default_if_invalid_phone(self, df, col, rval, rule):
        default_val = rval if rval.strip() else None

        def fix(v):
            if v is None or str(v).strip() == "":
                return default_val
            digits = re.sub(r"\D", "", str(v))
            return v if len(digits) >= 8 else default_val

        df[col] = df[col].apply(fix)
        return df

    # ------------------------------------------------------------------
    # PHONE_FORMAT
    # Normalisation indicatif CI (+225).
    # rval: "^(0[157])(.*)$ -> 225$1$2"
    # Pandas: apply + re.sub avec groupe de capture
    # ------------------------------------------------------------------
    def _handle_phone_format(self, df, col, rval, rule):
        if "->" not in rval:
            return df
        pattern_str, repl = [x.strip() for x in rval.split("->", 1)]
        pattern = re.compile(pattern_str)

        def normalize(v):
            if v is None:
                return v
            s = str(v).strip()
            return pattern.sub(repl, s) if pattern.match(s) else s

        df[col] = df[col].apply(normalize)
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_DATE_OUT_OF_RANGE
    # Date NULL / < 1900 / > aujourd'hui → valeur par défaut.
    # rval: "1900-01-01"
    # Pandas: pd.to_datetime + comparison vectorisée
    # ------------------------------------------------------------------
    def _handle_default_if_date_out_of_range(self, df, col, rval, rule):
        default_date = pd.to_datetime(rval, errors="coerce")
        today        = pd.Timestamp.today()

        df[col] = pd.to_datetime(df[col], errors="coerce")
        mask_invalid = df[col].isna() | (df[col].dt.year < 1900) | (df[col] > today)
        df.loc[mask_invalid, col] = default_date
        return df

    # ------------------------------------------------------------------
    # AGE_MIN_CHECK
    # Valide la majorité (ex: 18 ans+) — sinon date par défaut 1900-01-01.
    # rval: "18"
    # Pandas: pd.to_datetime + DateOffset comparison
    # ------------------------------------------------------------------
    def _handle_age_min_check(self, df, col, rval, rule):
        min_age  = int(rval)
        cutoff   = pd.Timestamp.today() - pd.DateOffset(years=min_age)
        fallback = pd.Timestamp("1900-01-01")

        df[col] = pd.to_datetime(df[col], errors="coerce")
        # Trop jeune (date > cutoff) ou NULL → fallback
        mask_invalid = df[col].isna() | (df[col] > cutoff)
        df.loc[mask_invalid, col] = fallback
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_INVALID_LENGTH
    # BankAccountNumber de longueur invalide (≠ 21 ou 24) → PaymentType = Cash.
    # rval: "Cash"
    # Pandas: apply sur la ligne entière pour modifier une autre colonne
    # ------------------------------------------------------------------
    def _handle_default_if_invalid_length(self, df, col, rval, rule):
        valid_lengths = {21, 24}

        def fix_row(row):
            v = row[col]
            if v is not None and str(v).strip() != "":
                if len(str(v).strip()) not in valid_lengths:
                    row = row.copy()
                    if "PAYMENTTYPE" in row.index:
                        row["PAYMENTTYPE"] = rval
            return row

        df = df.apply(fix_row, axis=1)
        return df

    # ------------------------------------------------------------------
    # MAPPING
    # Correspondance clé → valeur (table de mapping).
    # rval: "Active:A, Deactivated:D, Suspended:S"
    #       "TITLE=Monsieur → Homme | autre → Femme | NULL → Homme"
    #       "Cash:CA, Check:CH, CreditCard:CC, Mobile:MP"
    # Pandas: map() sur Series avec dict
    # ------------------------------------------------------------------
    def _handle_mapping(self, df, col, rval, rule):
        mapping = {}
        for pair in rval.split(","):
            pair = pair.strip()
            if ":" in pair:
                k, v = pair.split(":", 1)
                mapping[k.strip()] = v.strip()

        df[col] = df[col].apply(
            lambda v: mapping.get(str(v).strip(), v) if v is not None else v
        )
        return df

    # ------------------------------------------------------------------
    # MAPPING_REGION
    # Mapping avec valeur par défaut après le séparateur "|".
    # rval: "ABJ:Abidjan, BOU:Bouaké | DEF:Autre"
    # Pandas: apply avec dict + fallback
    # ------------------------------------------------------------------
    def _handle_mapping_region(self, df, col, rval, rule):
        default_val = None
        if "|" in rval:
            pairs_part, def_part = rval.split("|", 1)
            default_val = def_part.strip().split(":", 1)[-1].strip()
        else:
            pairs_part = rval

        mapping = {}
        for pair in pairs_part.split(","):
            if ":" in pair:
                k, v = pair.strip().split(":", 1)
                mapping[k.strip()] = v.strip()

        df[col] = df[col].apply(
            lambda v: mapping.get(str(v).strip(), default_val) if v is not None else default_val
        )
        return df

    # ------------------------------------------------------------------
    # MAP_OR_DEFAULT
    # Mapping + fallback global.
    # rval: "1:XOF, 2:EUR | 1"  |  "EUR:XOF, USD:XOF | XOF"
    # Pandas: apply avec dict + fallback sur None/vide
    # ------------------------------------------------------------------
    def _handle_map_or_default(self, df, col, rval, rule):
        default_val = None
        if "|" in rval:
            pairs_part, def_part = rval.split("|", 1)
            default_val = def_part.strip()
        else:
            pairs_part = rval

        mapping = {}
        for pair in pairs_part.split(","):
            if ":" in pair:
                k, v = pair.strip().split(":", 1)
                mapping[k.strip()] = v.strip()

        df[col] = df[col].apply(
            lambda v: mapping.get(str(v).strip(), default_val)
            if (v is not None and str(v).strip() != "")
            else default_val
        )
        return df

    # ------------------------------------------------------------------
    # UPPER_TRIM
    # Suppression espaces + mise en majuscule.
    # Ex: LASTNAME "  dupont  " → "DUPONT"
    # Pandas: str.strip().str.upper() vectorisé
    # ------------------------------------------------------------------
    def _handle_upper_trim(self, df, col, rval, rule):
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col] = df[col].replace("NONE", None)
        return df

    # ------------------------------------------------------------------
    # UPPER_TRIM_CLEAN
    # UPPER_TRIM + suppression des caractères spéciaux (hors lettres/chiffres/espace).
    # Ex: BILLING_ACCOUNT_NAME "  Cômpte-B2C! " → "COMPTE B2C"
    # Pandas: str accessor + re.sub
    # ------------------------------------------------------------------
    def _handle_upper_trim_clean(self, df, col, rval, rule):
        def clean(v):
            if v is None:
                return v
            s = str(v).strip().upper()
            s = re.sub(r"[^A-Z0-9\s\-]", "", s)
            return re.sub(r"\s+", " ", s).strip() or None

        df[col] = df[col].apply(clean)
        return df

    # ------------------------------------------------------------------
    # ALPHA_NUM_ONLY
    # Suppression des caractères spéciaux (. / , - \ |).
    # Ex: STREET "Rue de l'Église, n°12" → "Rue de lEglise n12"
    # Pandas: str.replace avec regex
    # ------------------------------------------------------------------
    def _handle_alpha_num_only(self, df, col, rval, rule):
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[.\-,/\\|#@!?]", " ", regex=True)
            .str.strip()
            .replace("None", None)
        )
        return df

    # ------------------------------------------------------------------
    # LOWERCASE
    # Forcer en minuscules.
    # Ex: EMAILSPEC "User@ORANGE.CI" → "user@orange.ci"
    # Pandas: str.lower() vectorisé
    # ------------------------------------------------------------------
    def _handle_lowercase(self, df, col, rval, rule):
        df[col] = df[col].astype(str).str.lower().str.strip().replace("none", None)
        return df

    # ------------------------------------------------------------------
    # REMOVE_SPACES
    # Suppression de tous les espaces dans la valeur.
    # Ex: IDDOCUMENTNUMBER "CI 123 456 789" → "CI123456789"
    # Pandas: str.replace(" ", "") vectorisé
    # ------------------------------------------------------------------
    def _handle_remove_spaces(self, df, col, rval, rule):
        df[col] = df[col].astype(str).str.replace(r"\s+", "", regex=True).replace("None", None)
        return df

    # ------------------------------------------------------------------
    # REGEX_VALIDATE
    # Valide le format par regex selon le type de document.
    # rval: "TYPE_CNI:^[0-9]{11}$"
    # Pandas: apply sur la ligne (accès à TYPEOFIDDOCUMENT)
    # ------------------------------------------------------------------
    def _handle_regex_validate(self, df, col, rval, rule):
        if ":" not in rval:
            return df
        type_key, pattern_str = rval.split(":", 1)
        type_key = type_key.strip()
        pattern  = re.compile(pattern_str.strip())

        def validate(row):
            doc_type = str(row.get("TYPEOFIDDOCUMENT", "")).strip()
            v        = row[col]
            if doc_type == type_key:
                if v is None or not pattern.match(str(v).strip()):
                    return None
            return v

        df[col] = df.apply(validate, axis=1)
        return df

    # ------------------------------------------------------------------
    # FIXED_LENGTH
    # ICCID = exactement 20 caractères.
    # Tronque si trop long, NULL si trop court.
    # rval: "20"
    # Pandas: apply
    # ------------------------------------------------------------------
    def _handle_fixed_length(self, df, col, rval, rule):
        expected = int(rval)

        def fix(v):
            if v is None:
                return None
            s = str(v).strip()
            if len(s) == expected:
                return s
            if len(s) > expected:
                return s[:expected]
            return None  # trop court → NULL

        df[col] = df[col].apply(fix)
        return df

    # ------------------------------------------------------------------
    # FIXED_LENGTH_NUM
    # IMEI = exactement 15 chiffres.
    # rval: "15"
    # Pandas: apply + re.sub pour extraire les chiffres
    # ------------------------------------------------------------------
    def _handle_fixed_length_num(self, df, col, rval, rule):
        expected = int(rval)

        def fix(v):
            if v is None:
                return None
            digits = re.sub(r"\D", "", str(v))
            return digits if len(digits) == expected else None

        df[col] = df[col].apply(fix)
        return df

    # ------------------------------------------------------------------
    # PREFIX_CHECK
    # Vérifie que la valeur commence par le préfixe MNC/MCC attendu.
    # rval: "61203"
    # Pandas: str.startswith vectorisé
    # ------------------------------------------------------------------
    def _handle_prefix_check(self, df, col, rval, rule):
        prefix = str(rval).strip()
        mask_invalid = ~df[col].astype(str).str.startswith(prefix)
        df.loc[mask_invalid, col] = None
        return df

    # ------------------------------------------------------------------
    # MAX_VALUE_FIELD
    # UNCLEAREDAMOUNT ne peut pas dépasser INITAMOUNT.
    # rval: "INITAMOUNT"
    # Pandas: np.minimum vectorisé sur deux colonnes numériques
    # ------------------------------------------------------------------
    def _handle_max_value_field(self, df, col, rval, rule):
        ref_col = rval.strip()
        if ref_col not in df.columns:
            logger.warning("MAX_VALUE_FIELD: colonne référence '%s' absente.", ref_col)
            return df

        col_num = pd.to_numeric(df[col],     errors="coerce")
        ref_num = pd.to_numeric(df[ref_col], errors="coerce")
        df[col] = col_num.where(col_num <= ref_num, ref_num)
        return df

    # ------------------------------------------------------------------
    # DATE_COHERENCE
    # Si DUEDATE < ISSUEDATE → DUEDATE = ISSUEDATE.
    # rval: "ISSUEDATE"
    # Pandas: pd.to_datetime + where vectorisé
    # ------------------------------------------------------------------
    def _handle_date_coherence(self, df, col, rval, rule):
        ref_col = rval.strip()
        if ref_col not in df.columns:
            logger.warning("DATE_COHERENCE: colonne référence '%s' absente.", ref_col)
            return df

        d1 = pd.to_datetime(df[col],     errors="coerce")
        d2 = pd.to_datetime(df[ref_col], errors="coerce")
        # Si d1 < d2 → d1 = d2
        df[col] = d1.where(d1 >= d2, other=d2)
        return df

    # ------------------------------------------------------------------
    # DATE_MIN_LIMIT
    # Si date < seuil → seuil.
    # rval: "2000-01-01"
    # Pandas: pd.to_datetime + clip lower
    # ------------------------------------------------------------------
    def _handle_date_min_limit(self, df, col, rval, rule):
        min_date = pd.to_datetime(rval, errors="coerce")
        df[col]  = pd.to_datetime(df[col], errors="coerce")
        df[col]  = df[col].clip(lower=min_date)
        return df

    # ------------------------------------------------------------------
    # DEFAULT_IF_NULL_SYSDATE (DN_MODDATE NULL → CURRENT_TIMESTAMP)
    # rval: "CURRENT_TIMESTAMP"
    # Pandas: fillna(pd.Timestamp.now())
    # ------------------------------------------------------------------
    def _handle_default_if_null_sysdate(self, df, col, rval, rule):
        now     = pd.Timestamp.now()
        df[col] = pd.to_datetime(df[col], errors="coerce").fillna(now)
        return df

    # ------------------------------------------------------------------
    # ROUND_DECIMAL
    # Arrondi monétaire XOF (0 décimales).
    # rval: "0"
    # Pandas: round() vectorisé
    # ------------------------------------------------------------------
    def _handle_round_decimal(self, df, col, rval, rule):
        decimals = int(rval) if str(rval).strip().lstrip("-").isdigit() else 0
        df[col]  = pd.to_numeric(df[col], errors="coerce").round(decimals)
        return df

    # ------------------------------------------------------------------
    # ABS_VALUE
    # Valeur absolue — pas de montants négatifs.
    # Pandas: abs() vectorisé
    # ------------------------------------------------------------------
    def _handle_abs_value(self, df, col, rval, rule):
        df[col] = pd.to_numeric(df[col], errors="coerce").abs()
        return df

    # ------------------------------------------------------------------
    # MAC_FORMAT
    # Adresse MAC en minuscules avec colons.
    # Ex: "AABBCCDDEEFF" → "aa:bb:cc:dd:ee:ff"
    # Pandas: apply
    # ------------------------------------------------------------------
    def _handle_mac_format(self, df, col, rval, rule):
        def format_mac(v):
            if v is None:
                return None
            s = re.sub(r"[:\-\.\s]", "", str(v)).lower()
            if len(s) == 12:
                return ":".join(s[i:i+2] for i in range(0, 12, 2))
            return v

        df[col] = df[col].apply(format_mac)
        return df

    # ------------------------------------------------------------------
    # MASK_DATA
    # Masquage de données sensibles (numéro de carte).
    # rval: "XXXX-XXXX-XXXX-####"  (# = chiffre conservé, X = masqué)
    # Pandas: apply
    # ------------------------------------------------------------------
    def _handle_mask_data(self, df, col, rval, rule):
        def mask(v):
            if v is None:
                return None
            digits  = re.sub(r"\D", "", str(v))
            pattern = rval
            result  = []
            d_idx   = len(digits) - 1
            for ch in reversed(pattern):
                if ch == "#":
                    result.append(digits[d_idx] if d_idx >= 0 else "0")
                    d_idx -= 1
                elif ch == "X":
                    result.append("X")
                else:
                    result.append(ch)
            return "".join(reversed(result))

        df[col] = df[col].apply(mask)
        return df

    # ------------------------------------------------------------------
    # UNIQUE_CHECK
    # Détection des doublons → marquage dans _migration_error_.
    # rval: "ERROR_IF_DUPLICATE"
    # Pandas: duplicated() vectorisé
    # ------------------------------------------------------------------
    def _handle_unique_check(self, df, col, rval, rule):
        if "_migration_error_" not in df.columns:
            df["_migration_error_"] = None

        dup_mask = df[col].duplicated(keep="first")
        df.loc[dup_mask, "_migration_error_"] = (
            f"DUPLICATE_{col}_rule_{rule['rule_id']}"
        )
        return df

    # ------------------------------------------------------------------
    # FOREIGN_KEY_CHECK
    # Vérifie l'existence dans la table parent (FK set chargé via SQL).
    # rval: "bscs_customer:CUSTOMER_ID"
    # Pandas: isin() vectorisé sur le set pré-chargé
    # ------------------------------------------------------------------
    def _handle_foreign_key_check(self, df, col, rval, rule):
        fk_key    = rval.strip()
        valid_ids = self._fk_sets.get(fk_key)

        if valid_ids is None:
            logger.warning("FK set '%s' non chargé — règle %s ignorée.", fk_key, rule["rule_id"])
            return df

        if "_migration_error_" not in df.columns:
            df["_migration_error_"] = None

        invalid_mask = ~df[col].isin(valid_ids)
        df.loc[invalid_mask, "_migration_error_"] = (
            f"FK_VIOLATION_{col}_not_in_{fk_key}_rule_{rule['rule_id']}"
        )
        return df

    # ------------------------------------------------------------------
    # MANDATORY_FIELD
    # Champ obligatoire — marquage si NULL.
    # rval: "REJECT_IF_NULL"
    # Pandas: isna() vectorisé
    # ------------------------------------------------------------------
    def _handle_mandatory_field(self, df, col, rval, rule):
        if "_migration_error_" not in df.columns:
            df["_migration_error_"] = None

        null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        df.loc[null_mask, "_migration_error_"] = (
            f"MANDATORY_{col}_IS_NULL_rule_{rule['rule_id']}"
        )
        return df


# ===========================================================================
# HELPERS CONNEXION
# ===========================================================================

def _source_hook() -> MySqlHook:
    return MySqlHook(mysql_conn_id="mysql_conn")

def _staging_hook() -> MySqlHook:
    return MySqlHook(mysql_conn_id="mysql_bss_staging")


def load_iteration_config(**context) -> None:
    dag_run      = context["dag_run"]
    iteration_id = (dag_run.conf or {}).get("iteration_id")
    if not iteration_id:
        raise ValueError("Paramètre 'iteration_id' manquant dans dag_run.conf.")

    sql = """
        SELECT
            cic.id,
            cic.rule_id,
            cic.row_limit,
            cic.offset_current,
            cic.iteration_id,
            cic.created_by,
            bmr.source_table,
            bmr.source_column,
            bmr.rule_type,
            bmr.default_value
        FROM correction_iteration_config cic
        INNER JOIN bss_migration_rules bmr
            ON cic.rule_id = bmr.rule_id
        WHERE cic.iteration_id = %s
        AND cic.to_execute   = 'YES'
        ORDER BY bmr.source_table, bmr.rule_id
    """
    rows = _source_hook().get_records(sql, parameters=[iteration_id])

    if not rows:
        raise ValueError(f"Aucune règle active pour iteration_id={iteration_id}.")

    row_limit      = int(rows[0][2])
    offset_current = int(rows[0][3])

    rules = [
        {
            "config_id":    row[0],
            "rule_id":      row[1],
            "row_limit":    row[2],
            "offset":       row[3],
            "iteration_id": row[4],
            "created_by":   row[5],
            "table_name":   row[6],   # source_table
            "column_name":  row[7],   # source_column
            "rule_type":    row[8],
            "rule_value":   row[9],   # default_value (peut être NULL)
        }
        for row in rows
    ]

    config = {
        "iteration_id":    int(iteration_id),
        "row_limit":       row_limit,
        "offset_current":  offset_current,
        "rules":           rules,
        "tables_involved": sorted(set(r["table_name"] for r in rules)),
    }
    context["ti"].xcom_push(key="iteration_config", value=config)
def load_fk_sets(**context) -> None:
    """
    Identifie les règles FOREIGN_KEY_CHECK et charge via SQL les sets de
    valeurs valides depuis la base source ou staging.

    SQL généré (exemple):
        SELECT DISTINCT CUSTOMER_ID
        FROM bscs_customer
        WHERE CUSTOMER_ID IS NOT NULL
    """
    config = context["ti"].xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    rules  = config["rules"]

    fk_rules = [r for r in rules if r["rule_type"] == "FOREIGN_KEY_CHECK"]
    fk_sets  = {}

    for rule in fk_rules:
        fk_ref = (rule["rule_value"] or "").strip()
        if ":" not in fk_ref:
            continue
        fk_table, fk_col = [x.strip() for x in fk_ref.split(":", 1)]

        hook = _source_hook() if fk_table.startswith("bscs_") else _staging_hook()
        sql  = f"SELECT DISTINCT `{fk_col}` FROM `{fk_table}` WHERE `{fk_col}` IS NOT NULL"
        logger.info("FK SQL: %s", sql)

        rows = hook.get_records(sql)
        fk_sets[fk_ref] = [row[0] for row in rows]
        logger.info("FK set '%s' : %d valeurs.", fk_ref, len(fk_sets[fk_ref]))

    context["ti"].xcom_push(key="fk_sets", value=fk_sets)


# ===========================================================================
# TASK 3 — etl_table  (une instance par table, dans un TaskGroup)
# ===========================================================================

def etl_table(table_name: str, **context) -> dict:
    """
    ETL complet pour une table :
      1. Extraction (SQL LIMIT/OFFSET via pandas.read_sql)
      2. Transformation (RuleEngine pandas)
      3. Chargement (pandas.to_sql vers la table corrected_*)

    SQL extraction:
        SELECT *
        FROM `<bscs_table>`
        LIMIT <row_limit> OFFSET <offset_current>

    SQL chargement:
        INSERT INTO `<corrected_table>` (<cols>) VALUES (...)
        [via pandas to_sql, mode append, chunksize=500]
    """
    ti     = context["ti"]
    config = ti.xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    fk_raw = ti.xcom_pull(key="fk_sets",          task_ids="load_fk_sets") or {}
    row_limit   = config["row_limit"]
    offset      = config["offset_current"]
    rules       = config["rules"]
    target_name = SOURCE_TO_TARGET.get(table_name)

    # ✅ Ensuite filtrer les règles pour cette table
    table_rules = [r for r in rules if r["table_name"] == table_name]
    if not table_rules:
        logger.info("[%s] Aucune règle active — table ignorée.", table_name)
        return {"table": table_name, "rows_extracted": 0, "rows_loaded": 0}

    # ----------------------------------------------------------------
    # 1. Extraction MySQL source → DataFrame pandas
    # ----------------------------------------------------------------
    src_hook    = _source_hook()
    src_engine  = src_hook.get_sqlalchemy_engine()

    sql_extract = (
        f"SELECT * FROM `{table_name}` "
        f"LIMIT {row_limit} OFFSET {offset}"
    )
    logger.info("[%s] SQL extract: %s", table_name, sql_extract)

    with src_engine.connect() as conn:
        df = pd.read_sql(sql_extract, conn)

    rows_extracted = len(df)
    logger.info("[%s] %d lignes extraites.", table_name, rows_extracted)

    if df.empty:
        return {"table": table_name, "rows_extracted": 0, "rows_loaded": 0}

    # ----------------------------------------------------------------
    # 2. Transformation via RuleEngine
    # ----------------------------------------------------------------
    table_rules = [r for r in rules if r["table_name"] == table_name]
    engine      = RuleEngine(table_rules)
    engine.set_fk_sets({k: set(v) for k, v in fk_raw.items()})

    df = engine.apply_all(df, table_name)

    # Séparer lignes valides / lignes en erreur
    if "_migration_error_" in df.columns:
        df_errors = df[df["_migration_error_"].notna()].copy()
        df_valid  = df[df["_migration_error_"].isna()].drop(columns=["_migration_error_"])
        if not df_errors.empty:
            logger.warning("[%s] %d lignes rejetées.", table_name, len(df_errors))
            _write_error_rows(df_errors, table_name, config["iteration_id"], context["run_id"])
    else:
        df_valid = df

    # ----------------------------------------------------------------
    # 3. Chargement → table corrected_* (MySQL staging)
    # ----------------------------------------------------------------
    rows_loaded = 0
    if not df_valid.empty:
        stg_engine = _staging_hook().get_sqlalchemy_engine()
        # Remplacer NaN par None (NULL MySQL)
        df_valid = df_valid.where(pd.notnull(df_valid), other=None)

        with stg_engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE `{target_name}`"))
            df_valid.to_sql(
                name      = target_name,
                con       = conn,
                if_exists = "append",
                index     = False,
                chunksize = 500,
                method    = "multi",
            )
        rows_loaded = len(df_valid)
        logger.info("[%s] → [%s] : %d lignes chargées.", table_name, target_name, rows_loaded)

    return {"table": table_name, "rows_extracted": rows_extracted, "rows_loaded": rows_loaded}

def _write_error_rows(df_errors: pd.DataFrame, table_name: str,
                      iteration_id: int, dag_run_id: str) -> None:
    hook = _staging_hook()
    sql = """
        INSERT INTO migration_errors
            (iteration_id, dag_run_id, source_table, error_reason, row_data, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    params = []
    for _, row in df_errors.iterrows():
        error_reason = str(row.get("_migration_error_", "UNKNOWN"))
        # Supprimer la colonne d'erreur avant de sérialiser en JSON
        row_data = row.drop(labels=["_migration_error_"], errors="ignore").to_json()
        # Échapper les % pour éviter les erreurs de formatting MySQL
        row_data = row_data.replace("%", "%%")
        params.append((
            iteration_id,
            dag_run_id,
            table_name,
            error_reason,
            row_data
        ))
    
    # Exécution multi-lignes
    if params:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
        cursor.close()
        logger.info("[%s] %d erreurs insérées dans migration_errors.", table_name, len(params))
def update_iteration_status(**context) -> None:
    """
    Met à jour correction_iteration_config :
      - offset_current += row_limit  (prochaine itération reprend ici)
      - dag_run_id = run ID courant

    SQL:
        UPDATE correction_iteration_config
        SET    offset_current = offset_current + %s,
               dag_run_id    = %s
        WHERE  iteration_id  = %s
          AND  to_execute    = 'YES'
    """
    config       = context["ti"].xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    row_limit    = config["row_limit"]
    iteration_id = config["iteration_id"]
    dag_run_id   = context["run_id"]

    sql = """
        UPDATE correction_iteration_config
        SET    offset_current = offset_current + %s,
               dag_run_id    = %s
        WHERE  iteration_id  = %s
          AND  to_execute    = 'YES'
    """
    _source_hook().run(sql, parameters=[row_limit, dag_run_id, iteration_id])
    logger.info(
        "Offset mis à jour — iteration_id=%s | +%s lignes | dag_run_id=%s",
        iteration_id, row_limit, dag_run_id,
    )


# ===========================================================================
# TASK 5 — generate_report
# ===========================================================================

def generate_report(**context) -> None:
    """
    Agrège les résultats XCom de chaque task ETL et écrit le rapport
    dans migration_run_summary.

    SQL:
        INSERT INTO migration_run_summary
            (iteration_id, dag_run_id, table_name,
             rows_extracted, rows_loaded, run_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    config       = context["ti"].xcom_pull(key="iteration_config", task_ids="load_iteration_config")
    iteration_id = config["iteration_id"]
    dag_run_id   = context["run_id"]

    total_extracted = 0
    total_loaded    = 0
    summary_rows    = []

    for table_name in sorted(SOURCE_TO_TARGET.keys()):
        result    = context["ti"].xcom_pull(task_ids=f"etl_tables.etl_{table_name}") or {}
        extracted = result.get("rows_extracted", 0)
        loaded    = result.get("rows_loaded",    0)
        total_extracted += extracted
        total_loaded    += loaded

        if extracted > 0:
            logger.info("  [%s] extrait=%d | chargé=%d", table_name, extracted, loaded)
            summary_rows.append((iteration_id, dag_run_id, table_name, extracted, loaded))

    logger.info(
        "=== BILAN === iteration_id=%s | extrait=%d | chargé=%d",
        iteration_id, total_extracted, total_loaded,
    )

    if summary_rows:
        hook = _staging_hook()
        sql = """
            INSERT INTO migration_run_summary
                (iteration_id, dag_run_id, table_name,
                 rows_extracted, rows_loaded, run_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, summary_rows)  # ← insertion multi-lignes
        conn.commit()
        cursor.close()
        logger.info("Rapport inséré avec succès pour %d tables.", len(summary_rows))
default_args = {
    "owner":            "migration_team",
    "depends_on_past":  False,
    "retries":          1,
    "email_on_failure": False,
    
}

with DAG(
    dag_id            = "bss_migration_etl",
    description       = "ETL migration BSS — BSCS (MySQL source) → corrected_* (staging)",
    schedule = None,          # déclenchement manuel uniquement
    start_date        = datetime(2024,1,1),
    catchup           = False,
    default_args      = default_args,
    tags              = ["bss", "migration", "etl"],
    params            = {"iteration_id": 1},
) as dag:

    # -----------------------------------------------------------------------
    # T1 — Charger la config de l'itération + les règles actives
    # -----------------------------------------------------------------------
    t_load_config = PythonOperator(
        task_id         = "load_iteration_config",
        python_callable = load_iteration_config,
    )

    # -----------------------------------------------------------------------
    # T2 — Charger les FK sets via SQL (avant les transformations)
    # -----------------------------------------------------------------------
    t_load_fk = PythonOperator(
        task_id         = "load_fk_sets",
        python_callable = load_fk_sets,
    )

    # -----------------------------------------------------------------------
    # T3 — TaskGroup ETL : une task par table source connue
    #      Les tables sans règles actives retournent rows=0 immédiatement.
    # -----------------------------------------------------------------------
    with TaskGroup(group_id="etl_tables") as tg_etl:
        for tbl in sorted(SOURCE_TO_TARGET.keys()):
            PythonOperator(
                task_id         = f"etl_{tbl}",
                python_callable = etl_table,
                op_kwargs       = {"table_name": tbl},
                
            )

    # -----------------------------------------------------------------------
    # T4 — Mettre à jour l'offset dans correction_iteration_config
    # -----------------------------------------------------------------------
    t_update_offset = PythonOperator(
        task_id         = "update_iteration_status",
        python_callable = update_iteration_status,
    )

    t_report = PythonOperator(
        task_id         = "generate_report",
        python_callable = generate_report,
    )
    t_load_config >> t_load_fk >> tg_etl >> t_update_offset >> t_report