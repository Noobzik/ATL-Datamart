import os.path
import urllib.request
import datetime
from minio import Minio

def main():
    grab_data_range()
    grab_latest_month()
    write_data_minio()


def grab_data_range() -> None:
    """Grab the data from New York Yellow Taxi for January 2023 to August 2023"""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    # URL des données des taxis de NYC
    data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023'
    # Répertoire pour récupérer les données
    save_dir = '../../data/raw'
    os.makedirs(save_dir, exist_ok=True)
    # Téléchargement
    for month in range(1, 9):  # Récupérer de janvier à août
        month_str = str(month).zfill(2)  # Formatage du mois sur deux chiffres avec un zéro devant si nécessaire
        file_url = f'{data_url}-{month_str}.parquet'
        file_path = os.path.join(folder_path, f'2023-{month_str}-tripdata.parquet')
        try:
            urllib.request.urlretrieve(file_url, file_path)
            print(f'Downloaded: {file_path}')
        except urllib.error.HTTPError as e:
            print(f"Error downloading {file_url}: {e}")
def grab_latest_month() -> None:
    """Grab the data from New York Yellow Taxi for the latest available month"""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    # Récupération de l'année actuelle
    current_year = datetime.datetime.now().year
    # URL des données des taxis de NYC pour l'année actuelle
    data_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}'
    # Répertoire pour récupérer les données
    save_dir = os.path.join(script_dir, '..', '..', 'data', 'raw')
    os.makedirs(save_dir, exist_ok=True)
    # Récupération du mois actuel
    current_month = datetime.datetime.now().month
    current_month_str = str(current_month).zfill(2)  # Formatage du mois sur deux chiffres avec un zéro devant si nécessaire
    # Téléchargement du mois actuel
    file_url = f'{data_url}-{current_month_str}.parquet'
    file_path = os.path.join(folder_path, f'{current_year}-{current_month_str}-tripdata.parquet')
    try:
        urllib.request.urlretrieve(file_url, file_path)
        print(f'Downloaded latest month data: {file_path}')
    except urllib.error.HTTPError as e:
        print(f"Error downloading {file_url}: {e}")
def write_data_minio():
    """
    This method puts all downloaded files into Minio
    """
    # Créez une instance Minio avec les détails de connexion
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket_name = "nycwarehouse"  # Nom du bucket Minio

    # Vérifiez si le bucket existe, sinon, créez-le
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print("Le bucket existe déjà")

    # Parcourez les fichiers dans le dossier de destination
    for filename in os.listdir('../../data/raw'):
        # Assurez-vous que le fichier est un fichier (pas un répertoire) avant de l'envoyer à Minio
        if os.path.isfile(os.path.join('../../data/raw', filename)):
            file_path = os.path.join('../../data/raw', filename)
            # Téléversez le fichier dans Minio
            object_name = os.path.basename(file_path)
            client.fput_object(bucket_name, object_name, file_path)
            print(f"Fichier téléchargé dans Minio : {object_name}")

if __name__ == '__main__':
    main()