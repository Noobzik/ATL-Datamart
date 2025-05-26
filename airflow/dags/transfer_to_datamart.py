from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

# Path to your SQL files (now inside the dags/sql directory)
CREATE_DATAMART_SQL = 'sql/datamart.sql'
TRANSFER_SQL = 'sql/transfer_to_datamart.sql'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='ATL-Transfer-To-Datamart',
    description='Ensure datamart exists and transfer data from warehouse to datamart',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
)

create_datamart = SQLExecuteQueryOperator(
    task_id='create_datamart',
    conn_id='datamart_postgres',  # This must be set up in Airflow Connections
    sql=CREATE_DATAMART_SQL,
    autocommit=True,
    dag=dag,
)

transfer_to_datamart = SQLExecuteQueryOperator(
    task_id='transfer_to_datamart',
    conn_id='datamart_postgres',
    sql=TRANSFER_SQL,
    autocommit=True,
    dag=dag,
)

create_datamart >> transfer_to_datamart
