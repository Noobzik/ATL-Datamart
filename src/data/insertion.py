import csv
import psycopg2

# Récupére les données CSV de location et les insère dans la base de données PostgreSQL
def get_data_from_csv():
    try:
        connection = psycopg2.connect(
            dbname="nyc_datamart",
            user="admin",
            password="admin",
            host="localhost",
            port="15432"
        )
        cursor = connection.cursor()
        print("Connexion à la base de données réussie.")

        # Ouverture du fichier CSV
        with open('./data/raw/taxi_zone_lookup.csv', newline='', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            print("Lecture du fichier CSV en cours...")
            next(csvreader)  # Ignorer la première ligne (en-têtes)

            # Réinitialiser la séquence pour que les nouveaux IDs commencent à 1
            cursor.execute("ALTER SEQUENCE dim_service_zone_id_seq RESTART WITH 1;")
            cursor.execute("ALTER SEQUENCE dim_zone_id_seq RESTART WITH 1;")
            cursor.execute("ALTER SEQUENCE dim_location_id_seq RESTART WITH 1;")

            # Parcourir le fichier CSV
            for row in csvreader:
                ## Insérer les données dans la table dim_service_zone
                # Nettoyer la valeur de service_zone en supprimant les espaces blancs
                service_zone = row[3].strip().lower()

                # Vérifier si la valeur de service_zone existe déjà dans la table dim_service_zone
                cursor.execute("SELECT id FROM dim_service_zone WHERE service_zone = %s;", (service_zone,))
                existing_service_zone = cursor.fetchone()

                if existing_service_zone is None:
                    # Si la valeur n'existe pas, l'insérer dans la table dim_service_zone
                    cursor.execute("INSERT INTO dim_service_zone (service_zone) VALUES (%s) RETURNING id;", (service_zone,))
                    service_zone_id = cursor.fetchone()[0]
                    print(f"Donnée '{service_zone}' insérée dans dim_service_zone.")
                else:
                    # Si la valeur existe, récupérer l'ID correspondant
                    service_zone_id = existing_service_zone[0]

                ## Insérer les données dans la table dim_zone
                # Nettoyer la valeur de zone en supprimant les espaces blancs
                zone = row[2].strip().lower()

                # Vérifier que zone n'est pas égal à "N/A"
                if zone != "N/A":
                    # Vérifier si la valeur de zone existe déjà dans la table dim_zone
                    cursor.execute("SELECT id FROM dim_zone WHERE zone = %s AND service_zone_id = %s;", (zone, service_zone_id))
                    existing_zone = cursor.fetchone()

                    if existing_zone is None:
                        # Si la valeur n'existe pas, l'insérer dans la table dim_zone
                        cursor.execute("INSERT INTO dim_zone (zone, service_zone_id) VALUES (%s, %s) RETURNING id;", (zone, service_zone_id))
                        zone_id = cursor.fetchone()[0]
                        print(f"Donnée '{zone}' insérée dans dim_zone.")
                    else:
                        # Si la valeur existe, récupérer l'ID correspondant
                        zone_id = existing_zone[0]

                    # Vérifier si la valeur de borough existe déjà dans la table dim_location pour la zone_id correspondante
                    borough = row[1].strip().lower()

                    # Vérifier que borough n'est pas égal à "N/A"
                    if borough != "N/A":
                        cursor.execute("SELECT COUNT(*) FROM dim_location WHERE borough = %s AND zone_id = %s;", (borough, zone_id))
                        existing_location_count = cursor.fetchone()[0]

                        if existing_location_count == 0:
                            # Si la valeur n'existe pas pour cette zone_id, l'insérer dans la table dim_location
                            cursor.execute("INSERT INTO dim_location (borough, zone_id) VALUES (%s, %s) RETURNING id;", (borough, zone_id))
                            print(f"Donnée '{borough}' insérée dans dim_location.")
                        else:
                            print(f"Donnée '{borough}' pour la zone {zone} existe déjà dans dim_location pour la zone_id {zone_id}.")
        connection.commit()
        print("Données insérées avec succès dans dim_service_zone, dim_zone et dim_location !")

    except psycopg2.Error as e:
        print("Erreur lors de la connexion à la base de données PostgreSQL ou lors de l'insertion des données :", e)

    finally:
        if connection is not None:
            connection.close()

# def insert_data_from_warehouse():



if __name__ == '__main__':
    #Récupére les données CSV et les insère dans la base de données PostgreSQL
    get_data_from_csv()