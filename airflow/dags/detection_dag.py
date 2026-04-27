from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    "owner": "eyaaa",
    "start_date": datetime(2026, 2, 9),
}

dag = DAG(
    dag_id="validation_donnees",
    default_args=default_args,
    schedule=None,
    catchup=False
)

def insert_error(mysql_hook, id_findoc, column, error_type, value):
    print(f" Erreur  - ID:{id_findoc}, Column:{column}, Type:{error_type}, Value:{value}")
    
    mysql_hook.run("""
        INSERT INTO bscs_findocs_errors
        (ID_FINDOC, COLUMN_NAME, ERROR_TYPE, ERROR_VALUE)
        VALUES (%s, %s, %s, %s)
    """, parameters=(id_findoc, column, error_type, str(value)))


def validate_data():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    ref_currencies=mysql_hook.get_records("select code_iso from map_currency")
    valid_currencies={row[0] for row in ref_currencies}
    records = mysql_hook.get_records("SELECT * FROM BSCS_FINDOCS")
    print(f" {len(records)} documents analysés")

    total_errors = 0

    for row in records:
        (
            BILLING_ACCOUNT_ID, COMPLAINT, CURRENCY, CURRENCY_INIT,
            CURRENCY_UNCLEARED, CUSTOMER_ID, DOCTYPE, DOCTYPE_DET,
            DUEDATE, EXCHANGERATE, EXTREF, ID_FINDOC,
            INITAMOUNT, ISSUEDATE, NETINITAMOUNT,
            REFERENCE, UNCLEAREDAMOUNT
        ) = row

        #  COMPLAINT doit être Y ou N
        if COMPLAINT not in ('Y', 'N'):
            insert_error(mysql_hook, ID_FINDOC, "COMPLAINT",
                         "Valeur invalide (doit être Y ou N)", COMPLAINT)
            total_errors += 1
        if CURRENCY is None or CURRENCY.strip() not  in valid_currencies:
            insert_error(mysql_hook,ID_FINDOC,"CURRENCY","devise invalide",CURRENCY)
            total_errors +=1
        

        #DOCTYPE CR → montant négatif
        if DOCTYPE == "CR" and INITAMOUNT > 0:
            insert_error(mysql_hook, ID_FINDOC, "INITAMOUNT",
                         "CR doit être négatif", INITAMOUNT)
            total_errors += 1

        #DOCTYPE IN → montant positif
        if DOCTYPE == "IN" and INITAMOUNT < 0:
            insert_error(mysql_hook, ID_FINDOC, "INITAMOUNT",
                         "IN doit être positif", INITAMOUNT)
            total_errors += 1

        #  UNCLEAREDAMOUNT <= INITAMOUNT
        if UNCLEAREDAMOUNT is not None and INITAMOUNT is not None:
            if UNCLEAREDAMOUNT > abs(INITAMOUNT):
                insert_error(mysql_hook, ID_FINDOC, "UNCLEAREDAMOUNT",
                             "Montant incohérent", UNCLEAREDAMOUNT)
                total_errors += 1

        # DUEDATE >= ISSUEDATE
        if DUEDATE and ISSUEDATE and DUEDATE < ISSUEDATE:
            insert_error(mysql_hook, ID_FINDOC, "DUEDATE",
                         "Date échéance < date émission", DUEDATE)
            total_errors += 1

    
        if EXCHANGERATE is None or EXCHANGERATE <= 0:
            insert_error(mysql_hook, ID_FINDOC, "EXCHANGERATE",
                         "Taux de change invalide", EXCHANGERATE)
            total_errors += 1

    print("="*40)
    print(f" Total erreurs détectées : {total_errors}")
    print("="*40)

    return total_errors


task_validate = PythonOperator(
    task_id="validate_bscs_data",
    python_callable=validate_data,
    dag=dag
)
