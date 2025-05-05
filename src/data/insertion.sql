BEGIN;

-- Activation de l’extension FDW si elle n’est pas encore installée
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Définition du serveur distant contenant les données sources
CREATE SERVER IF NOT EXISTS nyc_warehouse
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'atl-datamart-data-warehouse-1',
    port '5432',
    dbname 'nyc_warehouse',
    sslmode 'prefer'
);

-- Déclaration des identifiants d'accès pour l'utilisateur local
CREATE USER MAPPING IF NOT EXISTS FOR postgres
SERVER nyc_warehouse
OPTIONS (
    user 'postgres',
    password 'admin'
);

-- Connexion à la table distante contenant les courses brutes
CREATE FOREIGN TABLE IF NOT EXISTS remote_nyc_raw (
    vendorid integer,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance numeric,
    ratecodeid integer,
    store_and_fwd_flag text,
    pulocationid integer,
    dolocationid integer,
    payment_type integer,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    improvement_surcharge numeric,
    total_amount numeric,
    congestion_surcharge numeric,
    airport_fee numeric
)
SERVER nyc_warehouse
OPTIONS (
    schema_name 'public',
    table_name 'nyc_raw',
    updatable 'false'
);

-- Chargement temporaire des zones officielles (source externe TLC NYC)
CREATE TEMPORARY TABLE temp_zone_mapping (
    location_id INTEGER PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    borough_name VARCHAR(50) NOT NULL
) ON COMMIT DROP;

COPY temp_zone_mapping(location_id, zone_name, borough_name)
FROM PROGRAM 'curl -s "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv" | cut -d, -f1,3,2'
WITH (FORMAT csv, HEADER true);

-- Insertion dans les dimensions

-- Boroughs
INSERT INTO dim_borough (name)
SELECT DISTINCT borough_name FROM temp_zone_mapping
ON CONFLICT (name) DO NOTHING;

-- Zones
INSERT INTO dim_zone (zone_id, name, borough_id)
SELECT 
    t.location_id,
    t.zone_name,
    b.borough_id
FROM temp_zone_mapping t
JOIN dim_borough b ON t.borough_name = b.name
ON CONFLICT (zone_id) DO NOTHING;

-- Vendeurs
INSERT INTO dim_vendor (vendor_name)
SELECT DISTINCT
    CASE
        WHEN vendorid = 1 THEN 'Creative Mobile Technologies'
        WHEN vendorid = 2 THEN 'VeriFone Inc.'
        ELSE 'Unknown'
    END
FROM remote_nyc_raw
ON CONFLICT (vendor_name) DO NOTHING;

-- Moyens de paiement
INSERT INTO dim_payment (payment_type, rate_code)
SELECT DISTINCT
    CASE
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
        ELSE 'Other'
    END,
    CASE
        WHEN ratecodeid = 1 THEN 'Standard rate'
        WHEN ratecodeid = 2 THEN 'JFK'
        WHEN ratecodeid = 3 THEN 'Newark'
        WHEN ratecodeid = 4 THEN 'Nassau or Westchester'
        WHEN ratecodeid = 5 THEN 'Negotiated fare'
        WHEN ratecodeid = 6 THEN 'Group ride'
        ELSE 'Unknown'
    END
FROM remote_nyc_raw
ON CONFLICT (payment_type, rate_code) DO NOTHING;

-- Temps
INSERT INTO dim_time (
    pickup_datetime, pickup_date, pickup_time,
    hour_of_day, day_of_week, is_weekend, quarter, year
)
SELECT DISTINCT
    tpep_pickup_datetime,
    tpep_pickup_datetime::date,
    tpep_pickup_datetime::time,
    EXTRACT(HOUR FROM tpep_pickup_datetime),
    TRIM(TO_CHAR(tpep_pickup_datetime, 'Day')),
    EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6),
    EXTRACT(QUARTER FROM tpep_pickup_datetime),
    EXTRACT(YEAR FROM tpep_pickup_datetime)
FROM remote_nyc_raw
ON CONFLICT (pickup_datetime) DO NOTHING;

-- Localisations (zones de départ/arrivée + distance)
INSERT INTO dim_location (pickup_zone_id, dropoff_zone_id, distance_miles)
SELECT DISTINCT
    r.pulocationid,
    r.dolocationid,
    r.trip_distance
FROM remote_nyc_raw r
WHERE EXISTS (SELECT 1 FROM dim_zone WHERE zone_id = r.pulocationid)
  AND EXISTS (SELECT 1 FROM dim_zone WHERE zone_id = r.dolocationid)
ON CONFLICT (pickup_zone_id, dropoff_zone_id, distance_miles) DO NOTHING;

-- Remplissage de la table de faits
INSERT INTO fact_trips (
    trip_id, time_id, location_id, payment_id, vendor_id,
    passenger_count, fare_amount, tip_amount, total_amount,
    congestion_surcharge, airport_fee
)
SELECT
    md5(
        r.tpep_pickup_datetime::text ||
        r.pulocationid::text ||
        r.dolocationid::text
    )::uuid,
    t.time_id,
    l.location_id,
    p.payment_id,
    v.vendor_id,
    r.passenger_count,
    r.fare_amount,
    r.tip_amount,
    r.total_amount,
    COALESCE(r.congestion_surcharge, 0),
    COALESCE(r.airport_fee, 0)
FROM remote_nyc_raw r
JOIN dim_time t ON r.tpep_pickup_datetime = t.pickup_datetime
JOIN dim_location l ON r.pulocationid = l.pickup_zone_id
                   AND r.dolocationid = l.dropoff_zone_id
JOIN dim_payment p ON (
    CASE r.payment_type
        WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge' WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END = p.payment_type AND
    CASE r.ratecodeid
        WHEN 1 THEN 'Standard rate' WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark' WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare' WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END = p.rate_code
)
JOIN dim_vendor v ON (
    CASE r.vendorid
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc.'
        ELSE 'Unknown'
    END = v.vendor_name
);

COMMIT;
