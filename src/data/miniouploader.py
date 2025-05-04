# file_uploader.py MinIO Python SDK example
import sys
from minio import Minio
from minio.error import S3Error

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio("localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123",
    )

    # The file to upload, change this path if needed
    source_file = "C:/Users/Mehdi/Desktop/DSI/ATL-Datamart-Mehdi/data/raw/okkkt"

    # The destination bucket and filename on the MinIO server
    bucket_name = "ny.taxis"
    destination_file = "my-test-file.parquet"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )


if __name__ == '__main__':
    sys.exit(main())