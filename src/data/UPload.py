"""Script d'upload de fichiers vers un stockage MinIO pour le projet NYC Taxi"""

import sys
from minio import Minio
from minio.error import S3Error

def configure_minio_client():
    """Configure et retourne le client MinIO"""
    return Minio(
        endpoint="localhost:9000",
        access_key="minio",      # À remplacer par des variables d'environnement
        secret_key="minio123",   # À sécuriser en production
        secure=False             # HTTPS désactivé en dev
    )

def upload_file_to_minio(client, source_path, bucket_name, object_name):
    """Gère l'upload d'un fichier vers MinIO"""
    try:
        # Vérification/Création du bucket
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} créé")
        
        # Upload du fichier
        client.fput_object(
            bucket_name, 
            object_name, 
            source_path
        )
        print(f"Fichier {source_path} uploadé sous {object_name}")
        return True
    
    except S3Error as e:
        print(f"Erreur MinIO: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Erreur inattendue: {e}", file=sys.stderr)
        return False

def main():
    # Configuration
    client = configure_minio_client()
    source_file = "./data/raw/taxi_data.parquet"  # Chemin relatif recommandé
    bucket_name = "nyc-taxi"                     # Nom standardisé
    object_name = "raw/taxi_data.parquet"        # Structure de dossiers
    
    # Execution
    success = upload_file_to_minio(
        client, 
        source_file, 
        bucket_name, 
        object_name
    )
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())