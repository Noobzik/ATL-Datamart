#!/bin/bash

# Nom du script SQL
SQL_FILE="transfer_to_datamart.sql"
CONTAINER_NAME="data-mart"
DB_NAME="nyc_datamart"
DB_USER="admin"

echo "➡️  Copie du script SQL vers le conteneur $CONTAINER_NAME..."
docker compose cp "$SQL_FILE" "$CONTAINER_NAME":/tmp/$SQL_FILE

echo "🚀 Exécution du script dans le conteneur $CONTAINER_NAME..."
docker compose exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -f /tmp/$SQL_FILE

if [ $? -eq 0 ]; then
    echo "✅ Données transférées avec succès de data-warehouse vers data-mart."
else
    echo "❌ Une erreur est survenue lors du transfert."
fi
