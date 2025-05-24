from minio import Minio
import os
from pathlib import Path

client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "taxi-data"
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)

project_root = Path(__file__).resolve().parent.parent.parent
local_file = project_root / "data" / "raw" / "yellow_tripdata_2025-03.parquet"
object_name = os.path.basename(local_file)

client.fput_object(bucket_name, object_name, local_file)
print(f"Fichier {object_name} envoyé vers MinIO.")
