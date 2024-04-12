import argparse

import psycopg2
import pandas as pd


def insert_data_postgres(start_date) -> bool:
    """
    Crée des tables dans la base de données et insère les données du dataframe
    Récupère les lignes de nyx_raw et insère chaque colonne dans une table spécifique
    """

    # Configuration de la connexion à la base de données
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
            success: bool = True
            print("Insertion des données dans la base de données...")
            with open("insert_data.sql", "r") as f:
                sql_script = f.read()
                #mettre start_date en heure,min,sec, ms
                begin_date = start_date + " 00:00:00.000"

                cur.execute(sql_script, (begin_date, begin_date, begin_date, begin_date))
        except psycopg2.Error as e:
            print(f"Échec de l'exécution du script SQL : {e}")
            connection.rollback()
            success: bool = False
        else:
            connection.commit()
            print("Data inserer !")
            success: bool = True

    # Fermeture de la connexion
    connection.close()
    return success


def main(start_date: str):
    """
    Take a start date and create a table in the database
    Parameters:
        - start_date (str) : The start date to create the table
    """
    insert_data_postgres(start_date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Description de votre script main2')
    parser.add_argument('start_date', type=str, help='Description du premier paramètre')
    args = parser.parse_args()
    main(args.start_date)
