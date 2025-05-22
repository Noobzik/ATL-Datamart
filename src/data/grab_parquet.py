from minio import Minio
import os

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

bucket_name = "taxi-data"
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)

local_file = "C:/Users/jolib/ATL-Datamart/data/raw/yellow_tripdata_2025-03.parquet"
object_name = os.path.basename(local_file)

# Upload vers MinIO
client.fput_object(bucket_name, object_name, local_file)
print(f"Fichier {object_name} envoyé vers MinIO.")
