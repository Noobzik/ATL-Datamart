from minio import Minio

def get_minio_client():
    """
    Crée et retourne un client Minio
    """
    return Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )