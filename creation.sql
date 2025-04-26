-- ========================
-- 1. Suppression des tables existantes (clean reset)
-- ========================

DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;
DROP TABLE IF EXISTS dim_payment_type CASCADE;
DROP TABLE IF EXISTS dim_rate_code CASCADE;
DROP TABLE IF EXISTS dim_datetime CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;

-- ========================
-- 2. Création des dimensions enrichies
-- ========================

CREATE TABLE dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name TEXT
);

CREATE TABLE dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_description TEXT
);

CREATE TABLE dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_description TEXT
);

CREATE TABLE dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    full_datetime TIMESTAMP UNIQUE,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);

-- ========================
-- 3. Création de la table de faits
-- ========================

CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,

    vendor_id INT REFERENCES dim_vendor(vendor_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
    pickup_datetime_id INT REFERENCES dim_datetime(datetime_id),
    dropoff_datetime_id INT REFERENCES dim_datetime(datetime_id),
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),

    passenger_count SMALLINT,
    trip_distance FLOAT,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    congestion_surcharge NUMERIC,
    airport_fee NUMERIC,
    total_amount NUMERIC,

    store_and_fwd_flag CHAR(1)
);
