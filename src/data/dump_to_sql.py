import gc
import io
import sys

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
from minio.error import S3Error


def get_minio_client():
    return Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            print("✅ Connexion à PostgreSQL réussie !")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
            return True
    except Exception as e:
        print(f"❌ Erreur de connexion ou d’insertion PostgreSQL : {e}")
        return False


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main():
    bucket = "nyc-yellow-taxi"
    client = get_minio_client()

    try:
        objects = client.list_objects(bucket)
        for obj in objects:
            if obj.object_name.endswith(".parquet"):
                print(f"📦 Lecture de {obj.object_name} depuis Minio...")

                response = client.get_object(bucket, obj.object_name)
                data = response.read()  # Lire tout en mémoire
                df = pd.read_parquet(io.BytesIO(data))

                clean_column_name(df)

                if not write_data_postgres(df):
                    del df
                    gc.collect()
                    return

                del df
                gc.collect()
    except S3Error as e:
        print(f"❌ Erreur Minio : {e}")


if __name__ == '__main__':
    sys.exit(main())
