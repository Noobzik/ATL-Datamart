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
            for row in csvreader:
                # Insérer les données dans la table dim_service_zone
                service_zone = row[3]
                cursor.execute("INSERT INTO dim_service_zone (service_zone) VALUES (%s) ON CONFLICT DO NOTHING;", (service_zone,))
                print(f"Donnée '{service_zone}' insérée dans dim_service_zone.")

                # Récupérer l'ID de la service zone insérée ou existante
                cursor.execute("SELECT id FROM dim_service_zone WHERE service_zone = %s;", (service_zone,))
                service_zone_id = cursor.fetchone()[0]

                # Insérer les données dans la table dim_zone
                zone = row[2]
                cursor.execute("INSERT INTO dim_zone (zone, service_zone_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (zone, service_zone_id))
                print(f"Donnée '{zone}' insérée dans dim_zone.")

                # Récupérer l'ID de la zone insérée ou existante
                cursor.execute("SELECT id FROM dim_zone WHERE zone = %s AND service_zone_id = %s;", (zone, service_zone_id))
                zone_id = cursor.fetchone()[0]

                # Insérer les données dans la table dim_location
                borough = row[1]
                cursor.execute("INSERT INTO dim_location (borough, zone_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (borough, zone_id))
                print(f"Donnée '{borough}' insérée dans dim_location.")
                
        connection.commit()
        print("Données insérées avec succès dans dim_service_zone, dim_zone et dim_location !")

    except psycopg2.Error as e:
        print("Erreur lors de la connexion à la base de données PostgreSQL ou lors de l'insertion des données :", e)

    finally:
        if connection is not None:
            connection.close()
if __name__ == '__main__':
    #Récupére les données CSV et les insère dans la base de données PostgreSQL
    get_data_from_csv()