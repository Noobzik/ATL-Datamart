import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError

def create_database():
    try:
        # Connexion au serveur PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="admin",
            host="localhost",
            port="15432",
        )
        conn.autocommit = True
        print("Connexion au serveur PostgreSQL réussie.")

        # Vérification de l'existence de la base de données
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'nyc_datamart';")
        exists = cur.fetchone()
        cur.close()

        if not exists:
            # Création de la base de données si elle n'existe pas
            cur = conn.cursor()
            cur.execute(sql.SQL('CREATE DATABASE nyc_datamart;'))
            cur.close()
            print("Base de données 'nyc_datamart' créée avec succès.")
        else:
            print("La base de données 'nyc_datamart' existe déjà.")

    except OperationalError as e:
        print(f"Error: {e}")

    finally:
        if conn is not None:
            conn.close()

def create_table():
    try:
        # Utilisation de la base de données nyc_datamart
        conn = psycopg2.connect(
            dbname="nyc_datamart",
            user="admin",
            password="admin",
            host="localhost",
            port="15432",
        )
        conn.autocommit = True
        print("Connexion à la base de données 'nyc_datamart' réussie.")

        # Exécution du script SQL de création de tables
        cur = conn.cursor()
        with open('./models/creation.sql', 'r') as sql_file:
            cur.execute(sql_file.read())
        cur.close()
        print("Tables créées avec succès.")

    except OperationalError as e:
        print(f"Error: {e}")

    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    # Création de la base de données et des tables
    create_database()
    create_table()

