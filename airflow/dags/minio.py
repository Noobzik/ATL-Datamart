# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error
import psycopg2
import pandas as pd
import gc
import sys
from sqlalchemy import create_engine



def download_parquet(**kwargs):
    folder_path: str = 'data/raw'
    os.makedirs(folder_path, exist_ok=True)
    # Construct the relative path to the folder
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    month: str = pendulum.now().subtract(months=4).format('YYYY-MM')
    try:
        print(f"Downloading data for {month}...")
        print(f"URL:"+url+filename+'_'+month+extension)
        urllib.request.urlretrieve(url+filename+'_'+month+extension,folder_path+'/'+ filename +'_' + month + extension)
        print(folder_path+'/'+ filename +'_' + month + extension)
        #print la taille du ficiher
        print(os.path.getsize(folder_path+'/'+ filename +'_' + month + extension))
        print("Data downloaded successfully!")

    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

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
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
            print("Data dumped successfully!")

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success

def dump_to_sql(**kwargs):
    folder_path: str = 'data/raw' 
    parquet_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.parquet')]
    if not parquet_files:
        print("No Parquet files found in the specified directory.")
        return
    
    parquet_file = parquet_files[0]
    print(f"Processing {folder_path}/{parquet_file} for data cleaning and dumping to SQL database")
    print("Reading the parquet file...")
    parquet_df :pd.DataFrame = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')
    clean_column_name(parquet_df)
    print("Data cleaned successfully!")
    if not write_data_postgres(parquet_df):
        del parquet_df
        gc.collect()
        return
    del parquet_df
    gc.collect()

# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = 'taxis-parquet'
    month: str = pendulum.now().subtract(months=4).format('YYYY-MM')
    print(client.list_buckets())
    # Check if the bucket exists
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print(f"Bucket {bucket} already exists")

    # recupère le fichier present dans data/raw
    file = os.listdir(r'data/raw')[0]
    # Upload the file to Minio
    client.fput_object(bucket, file, r'data/raw/' + file)
    print(f"{file} uploaded successfully!")
    # On supprime le fichié récement téléchargés, pour éviter la redondance. On suppose qu'en arrivant ici, l'ajout est
    # bien réalisé
    os.remove(r'data/raw/yellow_tripdata_' + month + ".parquet")
    print("File deleted successfully!")


def insertion_clean_data_to_table():
    db_config = {
        "host": "localhost",
        "port": 15432,
        "database": "nyc_warehouse",
        "user": "postgres",
        "password": "admin",
    }
    # Connexion à la base de données
    try:
        connection = psycopg2.connect(**db_config)
    except psycopg2.OperationalError as e:
        print(f"Échec de la connexion à la base de données : {e}")
        exit()

    # Création d'un curseur
    with connection.cursor() as cur:
        try:
            print("Insertion des données dans la base de données...")
            with open("insert_data.sql", "r") as f:
                sql_script = f.read()
                #mettre start_date en heure,min,sec, ms
                begin_date = pendulum.now().subtract(months=4).format('YYYY-MM-DD') + " 00:00:00.000"

                cur.execute(sql_script, (begin_date, begin_date, begin_date, begin_date))
        except psycopg2.Error as e:
            print(f"Échec de l'exécution du script SQL : {e}")
            connection.rollback()
        else:
            connection.commit()
            print("Data inserer !")

    # Fermeture de la connexion
    connection.close()

###############################################
with DAG(dag_id='Grab_NYC_Data_to_Minio',
    # Define the schedule interval to run the DAG Mont

         start_date=pendulum.today('UTC').subtract(months=4),
         schedule_interval=None,
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='dump_to_sql',
        provide_context=True,
        python_callable=dump_to_sql
    )
    t3 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
    
###############################################

###############################################
# first upload the file, then read the other file.
t1 >> t2 >> t3
###############################################
