from minio import Minio
import urllib.request
import sys

def main():
    grab_data()
    write_data_minio()


def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi.

    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    print("Downloading files")
    for i in range(1, 11):
        if i < 10:
            i = "0" + str(i)
        else:
            i = str(i)

        print("Downloading file yellow_tripdata_2024-" + i + ".parquet")
        request = urllib.request.urlopen("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-" + i + ".parquet")
        with open("data/raw/yellow_tripdata_2024-" + i + ".parquet", "wb") as f:
            f.write(request.read())

    print("All files have been downloaded")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "my-bucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

    print("Uploading files to Minio")
    for i in range(1, 11):
        if i < 10:
            i = "0" + str(i)
        else:
            i = str(i)

        print("Uploading file yellow_tripdata_2024-" + i + ".parquet")
        client.fput_object(bucket, "yellow_tripdata_2024-" + i + ".parquet", "data/raw/yellow_tripdata_2024-" + i + ".parquet")

    print("All files have been uploaded")


if __name__ == '__main__':
    sys.exit(main())
