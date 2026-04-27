from airflow import DAG
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import pymysql
import json
import logging
from collections import defaultdict

default_args = {
    "owner": "data_export",
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": 5,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="export_full_error_rows",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Exporte les lignes complètes des tables sources vers bscs_error_details"
)

BATCH_SIZE        = 500
RETENTION_DAYS    = 7       # Nombre de jours d'historique conservés dans bscs_error_details


# ==============================================================================
# CONNEXION
# ==============================================================================

def get_conn_info(conn_id="mysql_conn"):
    hook = MySqlHook(mysql_conn_id=conn_id)
    c    = hook.get_connection(conn_id)
    return {
        "host":     c.host,
        "user":     c.login,
        "password": c.password,
        "database": c.schema,
        "port":     int(c.port or 3306),
    }


def get_table_primary_key(cursor, table_name):
    try:
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = %s
              AND COLUMN_KEY   = 'PRI'
            LIMIT 1
        """, (table_name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Erreur récupération PK pour {table_name}: {e}")
        return None


def get_table_columns(cursor, table_name):
    try:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = %s
            ORDER BY ORDINAL_POSITION
        """, (table_name,))
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"Erreur récupération colonnes pour {table_name}: {e}")
        return []


# ==============================================================================
# INITIALISATION DU SCHÉMA EXPORT (idempotent)
# ==============================================================================

def ensure_export_schema(**context):
    conn_info = get_conn_info()
    conn      = pymysql.connect(**conn_info)
    cursor    = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bscs_error_details (
            id                INT AUTO_INCREMENT PRIMARY KEY,
            source_run_id     VARCHAR(200),
            table_name        VARCHAR(200),
            row_pk_value      VARCHAR(200),
            full_row_data     MEDIUMTEXT,
            error_categories  TEXT,
            column_errors     TEXT,
            export_date       DATETIME,
            correction_status VARCHAR(50) DEFAULT 'PENDING',
            INDEX idx_source_run_id   (source_run_id),
            INDEX idx_table_name      (table_name),
            INDEX idx_correction_status (correction_status),
            INDEX idx_export_date     (export_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # Vue KPI export pour dashboard
    cursor.execute("""
        CREATE OR REPLACE VIEW v_export_summary AS
        SELECT
            source_run_id,
            table_name,
            COUNT(*)                                                AS nb_rows_exported,
            COUNT(CASE WHEN correction_status = 'PENDING'   THEN 1 END) AS nb_pending,
            COUNT(CASE WHEN correction_status = 'CORRECTED' THEN 1 END) AS nb_corrected,
            COUNT(CASE WHEN correction_status = 'IGNORED'   THEN 1 END) AS nb_ignored,
            MIN(export_date)                                        AS first_export,
            MAX(export_date)                                        AS last_export
        FROM bscs_error_details
        GROUP BY source_run_id, table_name
    """)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Schéma export et vues initialisés.")


# ==============================================================================
# RÉCUPÉRATION DES LIGNES PROBLÉMATIQUES — filtrées par source_run_id
# ==============================================================================

def get_problematic_rows_grouped(**context):
    dag_run       = context["dag_run"]
    source_run_id = (dag_run.conf.get("source_run_id") if dag_run.conf else None)

    if not source_run_id:
        # Exécution planifiée sans conf → prendre le run le plus récent en SUCCESS
        mysqlhook     = MySqlHook(mysql_conn_id="mysql_conn")
        latest        = mysqlhook.get_first("""
            SELECT run_id FROM bscs_quality_run
            WHERE status = 'SUCCESS'
            ORDER BY execution_date DESC
            LIMIT 1
        """)
        source_run_id = latest[0] if latest else None
        if not source_run_id:
            logging.warning("Aucun run SUCCESS trouvé — export annulé.")
            context["ti"].xcom_push(key="tables_data",    value={})
            context["ti"].xcom_push(key="source_run_id",  value=None)
            return
        logging.info(f"Exécution planifiée : source_run_id déduit = {source_run_id}")
    else:
        logging.info(f"source_run_id reçu depuis conf : {source_run_id}")

    context["ti"].xcom_push(key="source_run_id", value=source_run_id)

    mysqlhook = MySqlHook(mysql_conn_id="mysql_conn")

    # CORRECTION : filtrer UNIQUEMENT les lignes du run source
    rows = mysqlhook.get_records("""
        SELECT
            table_name,
            row_pk_value,
            GROUP_CONCAT(DISTINCT column_name                     ORDER BY column_name) AS columns_in_error,
            GROUP_CONCAT(DISTINCT rule                            ORDER BY rule)        AS rules,
            GROUP_CONCAT(DISTINCT error_category                  ORDER BY error_category) AS error_categories,
            MIN(detection_date) AS detection_date
        FROM bscs_problematic_rows
        WHERE run_id = %s
        GROUP BY table_name, row_pk_value
        ORDER BY table_name, row_pk_value
    """, parameters=(source_run_id,))

    if not rows:
        logging.info(f"Aucune ligne problématique pour run_id={source_run_id}")
        context["ti"].xcom_push(key="tables_data", value={})
        return

    tables_data = defaultdict(list)
    for row in rows:
        tables_data[row[0]].append({
            "pk_value":         row[1],
            "columns_in_error": row[2].split(",") if row[2] else [],
            "rules":            row[3].split(",") if row[3] else [],
            "error_categories": row[4].split(",") if row[4] else [],
            "detection_date":   row[5],
        })

    context["ti"].xcom_push(key="tables_data", value=dict(tables_data))
    logging.info(f"{len(tables_data)} tables à exporter pour run_id={source_run_id}")
    for table, items in tables_data.items():
        logging.info(f"  - {table} : {len(items)} lignes problématiques")


# ==============================================================================
# EXTRACTION DES LIGNES COMPLÈTES
# ==============================================================================

def extract_full_rows_for_table(table_name, pk_column, all_columns, rows_info, conn_info):
    conn          = None
    cursor        = None
    extracted_rows = []

    try:
        conn   = pymysql.connect(**conn_info)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        pk_values    = list(set([info["pk_value"] for info in rows_info]))
        pk_index     = {info["pk_value"]: info for info in rows_info}
        columns_list = [f"`{col[0]}`" for col in all_columns]

        for i in range(0, len(pk_values), BATCH_SIZE):
            batch        = pk_values[i: i + BATCH_SIZE]
            placeholders = ",".join(["%s"] * len(batch))
            query        = (
                f"SELECT {', '.join(columns_list)} "
                f"FROM `{table_name}` "
                f"WHERE `{pk_column}` IN ({placeholders})"
            )
            cursor.execute(query, batch)
            rows = cursor.fetchall()

            for row in rows:
                pk_value = str(row.get(pk_column))
                row_info = pk_index.get(pk_value)
                if not row_info:
                    continue

                row_json = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row_json[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(value, (int, float)):
                        row_json[key] = value
                    else:
                        row_json[key] = str(value) if value is not None else None

                column_errors = {}
                for col in row_info["columns_in_error"]:
                    col = col.strip()
                    column_errors[col] = {
                        "value": row_json.get(col),
                        "rules": [r for r in row_info["rules"] if col.lower() in r.lower()],
                    }

                extracted_rows.append({
                    "table_name":      table_name,
                    "row_pk_value":    pk_value,
                    "full_row_data":   json.dumps(row_json, ensure_ascii=False),
                    "error_categories":json.dumps(sorted(set(c.strip() for c in row_info["error_categories"]))),
                    "column_errors":   json.dumps(column_errors, ensure_ascii=False),
                    "detection_date":  row_info["detection_date"],
                })

        logging.info(f"  {len(extracted_rows)} lignes extraites pour {table_name}")

    except Exception as e:
        logging.error(f"Erreur extraction pour {table_name}: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return extracted_rows


def extract_all_full_rows(**context):
    tables_data = context["ti"].xcom_pull(key="tables_data",   task_ids="get_problematic_rows_grouped")

    if not tables_data:
        logging.info("Aucune donnée à extraire.")
        context["ti"].xcom_push(key="full_rows_data", value=[])
        return

    conn_info    = get_conn_info()
    all_full_rows = []

    for table_name, rows_info in tables_data.items():
        logging.info(f"\n{'='*50}\nTraitement : {table_name}\n{'='*50}")
        try:
            conn   = pymysql.connect(**conn_info)
            cursor = conn.cursor()

            pk_column  = get_table_primary_key(cursor, table_name)
            if not pk_column:
                logging.warning(f"Table {table_name} sans clé primaire — ignorée.")
                cursor.close()
                conn.close()
                continue

            all_columns = get_table_columns(cursor, table_name)
            if not all_columns:
                logging.warning(f"Table {table_name} sans colonnes — ignorée.")
                cursor.close()
                conn.close()
                continue

            cursor.close()
            conn.close()

            logging.info(f"  PK: {pk_column} | {len(rows_info)} IDs à extraire")
            rows_data = extract_full_rows_for_table(
                table_name, pk_column, all_columns, rows_info, conn_info
            )
            all_full_rows.extend(rows_data)
            logging.info(f"Total pour {table_name} : {len(rows_data)} lignes")

        except Exception as e:
            logging.error(f"Erreur traitement table {table_name}: {e}")
            continue

    context["ti"].xcom_push(key="full_rows_data", value=all_full_rows)
    logging.info(f"\nTotal général : {len(all_full_rows)} lignes extraites")


# ==============================================================================
# SAUVEGARDE — SANS TRUNCATE, avec rétention glissante et purge sécurisée
# ==============================================================================

def save_full_rows_to_table(**context):
    full_rows_data = context["ti"].xcom_pull(key="full_rows_data",  task_ids="extract_all_full_rows")
    source_run_id  = context["ti"].xcom_pull(key="source_run_id",   task_ids="get_problematic_rows_grouped")

    if not full_rows_data:
        logging.info("Aucune ligne à sauvegarder.")
        return

    conn_info = get_conn_info()
    conn      = pymysql.connect(**conn_info)
    cursor    = conn.cursor()

    try:
        # ------------------------------------------------------------------
        # ANTI-DUPLICATION : supprimer uniquement les lignes de ce run source
        # Pas de TRUNCATE → l'historique des anciens runs est conservé
        # ------------------------------------------------------------------
        if source_run_id:
            cursor.execute(
                "DELETE FROM bscs_error_details WHERE source_run_id = %s",
                (source_run_id,)
            )
            conn.commit()
            logging.info(f"Lignes précédentes du run {source_run_id} supprimées.")

        # PURGE ANCIENNE RÉTENTION : supprimer les exports plus vieux que RETENTION_DAYS
        cursor.execute(
            "DELETE FROM bscs_error_details WHERE export_date < DATE_SUB(NOW(), INTERVAL %s DAY)",
            (RETENTION_DAYS,)
        )
        purged = cursor.rowcount
        conn.commit()
        logging.info(f"Purge rétention : {purged} lignes supprimées (> {RETENTION_DAYS} jours).")

        # ------------------------------------------------------------------
        # INSERT des nouvelles données
        # ------------------------------------------------------------------
        insert_sql = """
            INSERT INTO bscs_error_details
                (source_run_id, table_name, row_pk_value, full_row_data,
                 error_categories, column_errors, export_date, correction_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        inserted  = 0
        now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for row in full_rows_data:
            cursor.execute(insert_sql, (
                source_run_id,
                row["table_name"],
                row["row_pk_value"],
                row["full_row_data"],
                row["error_categories"],
                row["column_errors"],
                now_str,
                "PENDING",
            ))
            inserted += 1
            if inserted % 500 == 0:
                conn.commit()
                logging.info(f"  {inserted} lignes insérées...")

        conn.commit()
        logging.info(f"{inserted} lignes sauvegardées dans bscs_error_details.")

        # Résumé par table
        cursor.execute("""
            SELECT table_name, COUNT(*) as cnt
            FROM bscs_error_details
            WHERE source_run_id = %s
            GROUP BY table_name
            ORDER BY cnt DESC
        """, (source_run_id,))
        logging.info("Résumé par table :")
        for row in cursor.fetchall():
            logging.info(f"  - {row[0]} : {row[1]} lignes")

    except Exception as e:
        logging.error(f"Erreur sauvegarde export : {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ==============================================================================
# RÉSUMÉ EXPORT — KPIs enrichis
# ==============================================================================

def generate_export_summary(**context):
    full_rows_data = context["ti"].xcom_pull(key="full_rows_data",  task_ids="extract_all_full_rows")
    source_run_id  = context["ti"].xcom_pull(key="source_run_id",   task_ids="get_problematic_rows_grouped")

    if not full_rows_data:
        logging.info("Aucune donnée pour le résumé.")
        return

    logging.info("\n" + "=" * 60)
    logging.info("RÉSUMÉ DE L'EXPORT")
    logging.info("=" * 60)
    logging.info(f"Source run_id   : {source_run_id}")
    logging.info(f"Date d'export   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Total lignes    : {len(full_rows_data)}")
    logging.info(f"Tables uniques  : {len(set(r['table_name'] for r in full_rows_data))}")

    categories_count  = defaultdict(int)
    tables_count      = defaultdict(int)
    for row in full_rows_data:
        tables_count[row["table_name"]] += 1
        try:
            categories = json.loads(row["error_categories"])
        except Exception:
            categories = []
        for cat in categories:
            categories_count[cat.strip()] += 1

    logging.info("\nPar catégorie d'erreur :")
    for cat, count in sorted(categories_count.items(), key=lambda x: x[1], reverse=True):
        pct = round(count / len(full_rows_data) * 100, 1)
        logging.info(f"  - {cat:<35} : {count:>6} lignes ({pct}%)")

    logging.info("\nPar table (top 10) :")
    for tbl, count in sorted(tables_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        logging.info(f"  - {tbl:<35} : {count:>6} lignes")

    # Requête KPI depuis la base pour le résumé final
    try:
        conn_info = get_conn_info()
        conn      = pymysql.connect(**conn_info)
        cursor    = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*)                                                           AS total_exported,
                COUNT(CASE WHEN correction_status = 'PENDING'   THEN 1 END)       AS pending,
                COUNT(CASE WHEN correction_status = 'CORRECTED' THEN 1 END)       AS corrected,
                COUNT(CASE WHEN correction_status = 'IGNORED'   THEN 1 END)       AS ignored
            FROM bscs_error_details
            WHERE source_run_id = %s
        """, (source_run_id,))
        stats = cursor.fetchone()
        if stats:
            logging.info(
                f"\nStatut corrections : "
                f"PENDING={stats[1]} | CORRECTED={stats[2]} | IGNORED={stats[3]}"
            )
        cursor.close()
        conn.close()
    except Exception as e:
        logging.warning(f"Impossible de récupérer les stats de correction : {e}")

    logging.info("=" * 60)


# ==============================================================================
# DÉFINITION DU DAG
# ==============================================================================

with dag:
    t0 = PythonOperator(
        task_id="ensure_export_schema",
        python_callable=ensure_export_schema,
    )
    t1 = PythonOperator(
        task_id="get_problematic_rows_grouped",
        python_callable=get_problematic_rows_grouped,
    )
    t2 = PythonOperator(
        task_id="extract_all_full_rows",
        python_callable=extract_all_full_rows,
    )
    t3 = PythonOperator(
        task_id="save_full_rows_to_table",
        python_callable=save_full_rows_to_table,
    )
    t4 = PythonOperator(
        task_id="generate_export_summary",
        python_callable=generate_export_summary,
    )

    t0 >> t1 >> t2 >> t3 >> t4