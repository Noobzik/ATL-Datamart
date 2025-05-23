-- Étape 1 : Activer l'extension postgres_fdw
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Étape 2 : Supprimer le serveur distant s’il existe déjà
DROP SERVER IF EXISTS warehouse_server CASCADE;

-- Étape 3 : Créer le serveur distant vers le data warehouse
CREATE SERVER warehouse_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'data-warehouse', port '5432', dbname 'nyc_warehouse');

-- Étape 4 : Créer le mapping utilisateur pour se connecter
CREATE USER MAPPING FOR admin
  SERVER warehouse_server
  OPTIONS (user 'admin', password 'admin');
