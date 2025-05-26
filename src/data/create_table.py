import sys
from pathlib import Path
from io import BytesIO
import psycopg2
from minio import Minio
import pandas as pd
from rich import print

def create_table_from_dataframe(conn, dataframe: pd.DataFrame, table_name: str) -> None:
    """Creates the table if it doesn't exist based on DataFrame schema"""
    type_map = {
        'int64': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP'
    }

    columns = ['"rowid" SERIAL PRIMARY KEY']
    for column_name, dtype in dataframe.dtypes.items():
        sql_type = type_map.get(str(dtype), 'TEXT')
        columns.append(f'"{column_name}" {sql_type}')

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns)}
    )
    """

    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()

def main() -> None:
    # MinIO configuration
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "my-bucket"

    try:
        # Get first parquet file
        objects = list(client.list_objects(bucket))
        if not objects:
            print("[red]No files found in bucket")
            return 1

        # Read first file
        first_file = objects[0]
        print(f"[cyan]Using file {first_file.object_name} for table creation")

        data = client.get_object(bucket, first_file.object_name)
        buffer = BytesIO(data.read())
        parquet_df = pd.read_parquet(buffer, engine='pyarrow')
        parquet_df.columns = map(str.lower, parquet_df.columns)

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="nyc_warehouse",
            user="admin",
            password="admin",
            host="localhost",
            port="15432"
        )

        # Create table
        create_table_from_dataframe(conn, parquet_df, 'nyc_raw')
        print("[green]Table created successfully")
        conn.close()

    except Exception as e:
        print(f"[red]Error: {e}")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
