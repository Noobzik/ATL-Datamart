import gc
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from sqlalchemy import create_engine, text


import pandas as pd
from sqlalchemy import create_engine, inspect
from minio import Minio
from minio.error import S3Error

@contextmanager
def secure_tempfile(suffix=None):
    """Gestion robuste des fichiers temporaires"""
    temp_path = tempfile.mktemp(suffix=suffix)
    try:
        yield temp_path
    finally:
        if os.path.exists(temp_path):
            for _ in range(3):
                try:
                    os.remove(temp_path)
                    break
                except PermissionError:
                    time.sleep(0.1)

def create_database_if_not_exists(db_config):
    """Crée la base de données si elle n'existe pas"""
    admin_engine = create_engine(
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/postgres"
    )
    try:
        with admin_engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            result = conn.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname='{db_config['dbms_database']}'")
            )
            if not result.scalar():
                conn.execute(text(f"CREATE DATABASE {db_config['dbms_database']}"))
                print(f"✅ Base de données {db_config['dbms_database']} créée avec succès")
    except Exception as e:
        print(f"❌ Erreur création base : {e}")
        raise
    finally:
        admin_engine.dispose()

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

    create_database_if_not_exists(db_config)

    engine = create_engine(
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )

    try:
        with engine.connect() as conn:
            print("✅ Connexion à PostgreSQL réussie.")
            inspector = inspect(engine)
            if not inspector.has_table(db_config['dbms_table']):
                dataframe.head(0).to_sql(
                    db_config['dbms_table'],
                    engine,
                    index=False,
                    if_exists='fail'
                )
                print(f"✅ Table {db_config['dbms_table']} créée avec succès")

            dataframe.to_sql(
                db_config['dbms_table'],
                engine,
                index=False,
                if_exists='append',
                chunksize=10000
            )
            return True
    except Exception as e:
        print(f"❌ Erreur de connexion ou d'insertion : {e}")
        return False
    finally:
        if engine:
            engine.dispose()

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def process_parquet_file(minio_client, bucket_name, obj):
    """Traite un fichier Parquet individuel"""
    print(f"📥 Téléchargement de {obj.object_name} depuis MinIO...")

    with secure_tempfile(suffix=".parquet") as temp_file_path:
        try:
            minio_client.fget_object(bucket_name, obj.object_name, temp_file_path)

            try:
                parquet_df = pd.read_parquet(temp_file_path, engine='pyarrow')
                clean_column_name(parquet_df)

                if not write_data_postgres(parquet_df):
                    print(f"⚠️ Échec insertion pour {obj.object_name}")
                    return False

                del parquet_df
                gc.collect()
                return True

            except Exception as e:
                print(f"❌ Erreur lecture Parquet {obj.object_name}: {e}")
                return False

        except Exception as e:
            print(f"❌ Erreur traitement {obj.object_name}: {e}")
            return False

def main() -> None:
    start_time = time.time()

    minio_client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "nyc-taxi"
    success_count = 0
    error_count = 0

    try:
        objects = list(minio_client.list_objects(bucket_name, recursive=True))
        parquet_files = [obj for obj in objects if obj.object_name.endswith(".parquet")]
        total_files = len(parquet_files)
        print(f"🔍 {total_files} fichiers Parquet à traiter...")

        for i, obj in enumerate(parquet_files, 1):
            print(f"\n📂 Fichier {i}/{total_files}: {obj.object_name}")
            if process_parquet_file(minio_client, bucket_name, obj):
                success_count += 1
            else:
                error_count += 1

    except S3Error as e:
        print(f"❌ Erreur MinIO: {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        error_count += 1
    finally:
        elapsed_time = time.time() - start_time
        print(f"\n✅ {success_count} fichiers traités avec succès")
        print(f"❌ {error_count} fichiers en échec")
        print(f"⏱️ Traitement terminé en {elapsed_time:.2f} secondes")

if __name__ == '__main__':
    sys.exit(main())
