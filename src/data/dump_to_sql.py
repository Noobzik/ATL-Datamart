from minio import Minio
import pandas as pd
import os
from sqlalchemy import create_engine

minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

bucket_name = "taxi-data"
object_name = "yellow_tripdata_2025-03.parquet"
local_parquet_path = os.path.join("data", "interim", object_name)

minio_client.fget_object(bucket_name, object_name, local_parquet_path)
print(f"Téléchargé : {object_name}")

df = pd.read_parquet(local_parquet_path)
print(f"Lignes : {len(df)}")

db_user = "postgres"
db_password = "root"
db_host = "localhost"
db_port = "5432"
db_name = "data_warehouse"

engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

table_name = "taxi_data"
df.to_sql(table_name, engine, if_exists='replace', index=False)
print("Données insérées dans PostgreSQL.")
