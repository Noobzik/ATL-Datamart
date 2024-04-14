from minio import Minio
from minio.error import S3Error
import Constants

import urllib.request

# import pandas as pd
import sys
import os

folder = "../../data/raw"
os.makedirs(folder, exist_ok=True)


def main():
    grab_data()
    write_data_minio()


def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi.

    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    print("### START OF DATA DOWNLOADING ###")

    month = [1, 2]

    for year in range(2023, 2024):
        for m in month:
            try:
                file_name = f"yellow_tripdata_{year}-{m}.parquet"
                file_path = os.path.join(folder, file_name)
                print(f"🔌 Start downloading for {file_name}")

                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{str(m).zfill(2)}.parquet"
                print(f"🎯 URL targeted: {url}")
                response = urllib.request.urlopen(url)
                with open(file_path, "wb") as f:
                    f.write(response.read())
                print(f"✨ Downloaded data to {file_path}")
                print("---")
            except urllib.request.HTTPError as e:
                print(e)
                break
    print("### END OF DATA DOWNLOADING ###")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """

    print("### START OF MINIO UPLOAD ###")

    parquet_files = [
        f
        for f in os.listdir(folder)
        if f.lower().endswith(".parquet") and os.path.isfile(os.path.join(folder, f))
    ]

    for parquet_file in parquet_files:
        try:
            client = Minio(
                "localhost:9000",
                secure=False,
                access_key="minio",
                secret_key="minio123",
            )
            bucket: str = Constants.BUCKET_NAME
            found = client.bucket_exists(bucket)
            if not found:
                client.make_bucket(bucket)
                print("Created bucket", bucket)
            else:
                print("Bucket " + bucket + " existe déjà")

            src = os.path.join(folder, parquet_file)

            client.fput_object(bucket, parquet_file, src)
            print(
                parquet_file,
                "successfully uploaded as object",
                src,
                "to bucket",
                bucket,
            )
        except S3Error as e:
            print("Error", e)
    print("### END OF MINIO UPLOAD ###")


if __name__ == "__main__":
    sys.exit(main())
