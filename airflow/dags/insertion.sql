-- insertion.sql
DELETE FROM fact_trips CASCADE;
DELETE FROM dim_location CASCADE;
DELETE FROM dim_payment CASCADE;
DELETE FROM dim_ratecode CASCADE;
DELETE FROM dim_vendor CASCADE;

-- Installer l'extension postgres_fdw
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS data_warehouse_server CASCADE;
-- Créer un serveur pour la connexion au data warehouse
CREATE SERVER data_warehouse_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'data-warehouse', dbname 'nyc_warehouse', port '5432');

-- Créer un mapping d'utilisateur
CREATE USER MAPPING FOR postgres
SERVER data_warehouse_server
OPTIONS (user 'postgres', password 'admin');

-- Créer un schéma pour les tables distantes
CREATE SCHEMA IF NOT EXISTS data_warehouse_schema;

-- Importer la table distante nyc_raw
IMPORT FOREIGN SCHEMA public
FROM SERVER data_warehouse_server
INTO data_warehouse_schema;

-- Insérer les données dans les tables dimensionnelles
INSERT INTO dim_vendor (vendorid, vendor_name, contact_info)
SELECT DISTINCT vendorid, 'Vendor ' || vendorid, 'Contact ' || vendorid
FROM data_warehouse_schema.nyc_raw;

INSERT INTO dim_ratecode (ratecodeid, ratecode_description, is_flat_rate)
SELECT DISTINCT ratecodeid, 'Rate Code ' || ratecodeid, FALSE
FROM data_warehouse_schema.nyc_raw
WHERE ratecodeid IS NOT NULL;

INSERT INTO dim_payment (payment_type, is_electronic, payment_description)
SELECT DISTINCT payment_type, payment_type = 1, 'Payment Type ' || payment_type
FROM data_warehouse_schema.nyc_raw;

INSERT INTO dim_location (locationid, borough, service_zone, zone)
SELECT DISTINCT pulocationid, 'Borough ' || pulocationid, 'Service Zone ' || pulocationid, 'Zone ' || pulocationid
FROM data_warehouse_schema.nyc_raw
UNION
SELECT DISTINCT dolocationid, 'Borough ' || dolocationid, 'Service Zone ' || dolocationid, 'Zone ' || dolocationid
FROM data_warehouse_schema.nyc_raw;

-- Insérer les données dans la table de faits
INSERT INTO fact_trips (
    passenger_count, trip_distance, store_and_fwd_flag, pulocationid, dolocationid,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount,
    congestion_surcharge, airport_fee, improvement_surcharge, locationid, locationid_1,
    payment_type, ratecodeid, vendorid
)
SELECT
    passenger_count, trip_distance, store_and_fwd_flag, pulocationid, dolocationid,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount,
    congestion_surcharge, airport_fee, improvement_surcharge, pulocationid, dolocationid,
    payment_type, ratecodeid, vendorid
FROM data_warehouse_schema.nyc_raw;
