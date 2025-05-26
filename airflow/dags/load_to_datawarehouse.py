from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine, text
import pendulum
from airflow.utils.dates import days_ago
from airflow import DAG
import logging

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

BUCKET = 'my-bucket'
MINIO_KWARGS = dict(
    endpoint="minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)
DB_URL = "postgresql://admin:admin@data-warehouse:5432/nyc_warehouse"
STATE_TABLE = 'etl_state'

def get_month_filename():
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    return f"yellow_tripdata_{month}.parquet"

def get_state(engine, filename):
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                filename TEXT PRIMARY KEY,
                last_row_group INTEGER,
                last_batch INTEGER,
                total_rows INTEGER
            )
        """))
        result = conn.execute(text(f"SELECT last_row_group, last_batch, total_rows FROM {STATE_TABLE} WHERE filename=:filename"), {"filename": filename})
        row = result.fetchone()
        if row:
            return row[0], row[1], row[2]
        else:
            return -1, -1, 0

def set_state(engine, filename, last_row_group, last_batch, total_rows):
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {STATE_TABLE} (filename, last_row_group, last_batch, total_rows)
            VALUES (:filename, :last_row_group, :last_batch, :total_rows)
            ON CONFLICT (filename) DO UPDATE SET
                last_row_group=EXCLUDED.last_row_group,
                last_batch=EXCLUDED.last_batch,
                total_rows=EXCLUDED.total_rows
        """), {
            "filename": filename,
            "last_row_group": last_row_group,
            "last_batch": last_batch,
            "total_rows": total_rows
        })

def reset_state(engine, filename):
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {STATE_TABLE} WHERE filename=:filename"), {"filename": filename})

def load_to_datawarehouse(**kwargs):
    import pyarrow.parquet as pq
    import psutil
    client = Minio(**MINIO_KWARGS)
    filename = get_month_filename()
    logging.info(f"[Airflow] Starting load of {filename} from Minio bucket {BUCKET}")
    logging.info(f"[Airflow] Available memory before download: {psutil.virtual_memory().available / 1024**2:.2f} MB")

    engine = create_engine(DB_URL)
    # Option to reset state
    reset_state_flag = kwargs.get('params', {}).get('reset_state', False)
    if reset_state_flag:
        reset_state(engine, filename)
        logging.info(f"[Airflow] State for {filename} reset in DB.")

    # Load state from DB
    last_row_group, last_batch, total_rows = get_state(engine, filename)
    if last_row_group >= 0 or last_batch >= 0:
        logging.info(f"[Airflow] Resuming from row group {last_row_group+1}, batch {last_batch+1}, total_rows={total_rows}")
    else:
        last_row_group = -1
        last_batch = -1
        total_rows = 0

    # Download parquet from Minio
    try:
        logging.info(f"[Airflow] Attempting to get object {filename} from bucket {BUCKET}")
        data = client.get_object(BUCKET, filename)
        logging.info(f"[Airflow] Successfully got object {filename} from Minio")
        buffer = BytesIO(data.read())
        logging.info(f"[Airflow] Read bytes from Minio object, size: {buffer.getbuffer().nbytes} bytes")
    except Exception as e:
        logging.error(f"[Airflow] Error downloading file from Minio: {e}")
        raise
    try:
        parquet_file = pq.ParquetFile(buffer)
        for i in range(parquet_file.num_row_groups):
            if i <= last_row_group:
                continue
            logging.info(f"[Airflow] Reading row group {i+1}/{parquet_file.num_row_groups}")
            table = parquet_file.read_row_group(i)
            logging.info(f"[Airflow] Row group {i+1} num_rows: {table.num_rows}")
            for batch_idx, batch in enumerate(table.to_batches(max_chunksize=1000)):
                if i == last_row_group and batch_idx <= last_batch:
                    continue
                df = batch.to_pandas()
                df.columns = map(str.lower, df.columns)
                logging.info(f"[Airflow] Inserting batch of {len(df):,} rows from row group {i+1}, batch {batch_idx+1}")
                df.to_sql('nyc_raw', engine, index=False, if_exists='append', method='multi', chunksize=1000)
                total_rows += len(df)
                set_state(engine, filename, i, batch_idx, total_rows)
                logging.info(f"[Airflow] Inserted {len(df):,} rows from batch {batch_idx+1} in row group {i+1}, total_rows={total_rows}")
            logging.info(f"[Airflow] Available memory after row group {i+1}: {psutil.virtual_memory().available / 1024**2:.2f} MB")
        logging.info(f"[Airflow] Successfully loaded {total_rows:,} rows from {filename} into nyc_raw table.")
        # Report total lines inserted
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM nyc_raw"))
            count = result.scalar()
            logging.info(f"[Airflow] Total lines in nyc_raw after load: {count:,}")
        # Remove state after successful completion
        reset_state(engine, filename)
        logging.info(f"[Airflow] State for {filename} removed after successful completion.")
    except Exception as e:
        logging.error(f"[Airflow] Error processing parquet file in batches: {e}")
        raise

with DAG(
    dag_id='ATL-Load-To-DataWarehouse',
    dag_display_name='ATL Load To DataWarehouse',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='load_to_datawarehouse',
        python_callable=load_to_datawarehouse
    )
