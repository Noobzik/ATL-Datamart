import os
import sys
import tempfile
import logging
from contextlib import contextmanager
from functools import partial
from multiprocessing import Pool, cpu_count

import pendulum
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.url import URL
from minio import Minio

# Configuration centrale avec valeurs par défaut pour le dev local
DB_CONFIG = {
    'drivername': os.getenv('DB_DRIVER', 'postgresql'),
    'username': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASS', 'admin'),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     os.getenv('DB_PORT', '15432'),  # Port non standard pour éviter les conflits
    'database': os.getenv('DB_NAME', 'nyc_warehouse'),
}

# Optimisation des performances
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '5000'))  # Taille idéale pour INSERT batch
NUM_WORKERS = int(os.getenv('NUM_WORKERS', str(cpu_count())))  # Utilisation complète du CPU

@contextmanager
def temp_file(suffix=''):
    """Gestion sécurisée des fichiers temporaires avec suppression garantie"""
    path = tempfile.mktemp(suffix=suffix)
    try:
        yield path
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

def ensure_database_exists():
    """Crée la BDD si inexistante en se connectant d'abord à postgres"""
    admin_url = URL(**{**DB_CONFIG, 'database': 'postgres'})
    engine = create_engine(admin_url)

    try:
        with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {'db': DB_CONFIG['database']}
            ).scalar()
            if not exists:
                conn.execute(text(f"CREATE DATABASE {DB_CONFIG['database']!r}"))
                logger.info("Nouvelle base créée: %s", DB_CONFIG['database'])
    finally:
        engine.dispose()  # Toujours libérer les connexions

def process_file(object_name: str):
    """Pipeline complet pour un fichier: téléchargement → lecture → insertion"""
    client = Minio(**MINIO_CONFIG)
    with temp_file('.parquet') as path:
        try:
            logger.info("Traitement de %s", object_name)
            download_parquet(client, object_name, path)
            
            # Lecture itérative pour économiser la mémoire
            df_iter = pd.read_parquet(path, engine='pyarrow', chunksize=CHUNK_SIZE)
            engine = get_engine()

            # Schéma auto-détecté si table inexistante
            if not inspect(engine).has_table(TABLE_NAME):
                first_chunk = next(df_iter)
                clean_columns(first_chunk).head(0).to_sql(TABLE_NAME, engine, index=False)
                df_iter = pd.read_parquet(path, engine='pyarrow', chunksize=CHUNK_SIZE)  # Reset iterator

            for chunk in df_iter:
                ingest_chunk(clean_columns(chunk), engine)

            return True
        except Exception as e:
            logger.error("Échec sur %s: %s", object_name, str(e))
            return False
        finally:
            engine.dispose()

def main():
    """Workflow principal avec parallélisation et suivi des performances"""
    start_time = pendulum.now()
    ensure_database_exists()
    
    client = Minio(**MINIO_CONFIG)
    parquet_files = list_parquet_objects(client)
    logger.info("%d fichiers Parquet à traiter", len(parquet_files))

    # Pool de workers pour le traitement parallèle
    with Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_file, parquet_files)

    # Rapport final avec métriques
    success_count = sum(results)
    logger.info(
        "Terminé: %d succès | %d échecs | Durée: %s",
        success_count,
        len(parquet_files) - success_count,
        pendulum.now() - start_time
    )

if __name__ == '__main__':
    sys.exit(main())