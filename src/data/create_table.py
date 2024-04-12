import argparse

import psycopg2
import pandas as pd


def create_table_postgres(dataframe: pd.DataFrame) -> bool:
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
            print("Création des tables dans la base de données...")
            with open("create_table.sql", "r") as f:
                sql_script = f.read()
                cur.execute(sql_script)
        except psycopg2.Error as e:
            print(f"Échec de l'exécution du script SQL : {e}")
            connection.rollback()
            success: bool = False
        else:
            connection.commit()
            print("Tables créées avec succès !")
            success: bool = True

    # Fermeture de la connexion
    connection.close()
    return success


def main():
    """
    Take a start date and create a table in the database
    Parameters:
        - start_date (str) : The start date to create the table
    """
    create_table_postgres(pd.DataFrame())
    print(f"Table created !")


if __name__ == '__main__':
    main()
