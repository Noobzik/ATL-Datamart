from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

# Database config
db_config = {
    "dbms_engine": "postgresql",
    "dbms_username": "admin",
    "dbms_password": "admin",
    "dbms_ip": "data-mart",
    "dbms_port": "5432",
    "dbms_database": "nyc_datamart",
}

# Function to execute SQL file
def execute_sql_file():
    sql_file_path = '/opt/airflow/dags/insertion.sql'  # Adjust path as needed
    with open(sql_file_path, 'r') as file:
        sql = file.read()

    conn = psycopg2.connect(
        host=db_config['dbms_ip'],
        port=db_config['dbms_port'],
        dbname=db_config['dbms_database'],
        user=db_config['dbms_username'],
        password=db_config['dbms_password']
    )
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    conn.close()

# DAG definition
with DAG(
    dag_id='warehouse_to_datamart',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['manual_sql'],
) as dag:

    run_sql = PythonOperator(
        task_id='run_sql_file',
        python_callable=execute_sql_file
    )
