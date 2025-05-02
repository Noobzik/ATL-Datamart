# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import os
import urllib.error

def download_parquet(**kwargs):
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    # faut prendre en condsideration la disponibilité du fichier du moi dernier
    # Nombre de mois à essayer en arrière
    max_attempts = 3
    success = False
    
    # Essayer de télécharger les fichiers pour les mois précédents en utilisant max_attempts
    for i in range(2, 2 + max_attempts):
        try:
            month: str = pendulum.now().subtract(months=i).format('YYYY-MM')
            file_url = url + filename + "_" + month + extension
            local_path = filename + "_" + month + extension
            
            print(f"Tentative de téléchargement du fichier pour {month}...")
            request.urlretrieve(file_url, local_path)
            
            # succes du téléchargement
            print(f"Téléchargement réussi pour {month}")
            
            # stockage du mois téléchargé dans XCom pour le communiquer au t2 
            kwargs['ti'].xcom_push(key='downloaded_month', value=month)
            
            success = True
            break 
            
        except urllib.error.URLError as e:
            print(f"Échec du téléchargement pour {month}: {str(e)}")
            # continuer a la prochaine itération si le téléchargement échoue
    
    
    if not success:
        raise RuntimeError(f"Failed to download parquet files for the last {max_attempts} months")

# Python Function
def upload_file(**kwargs):
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = 'rawnyc'
    
    # recuperation depuis  XCom
    ti = kwargs['ti']
    month = ti.xcom_pull(task_ids='download_parquet', key='downloaded_month')
    
    if not month:
        raise ValueError("No month information available from download task")
    
    print(f"Processing month: {month}")
    print(client.list_buckets())
    
    # Vérifier si le bucket existe, sinon le créer
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    
    file_name = f"yellow_tripdata_{month}.parquet"
    
    client.fput_object(
        bucket_name=bucket,
        object_name=file_name,
        file_path=file_name)
    
    # Supprimer le fichier local après l'upload
    os.remove(os.path.join("./", file_name))

###############################################
with DAG(dag_id='grab_nyc_data_to_minio',
         start_date=days_ago(1),
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
    task_id='upload_file_task',
    provide_context=True,
    python_callable=upload_file
)
###############################################  

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################
