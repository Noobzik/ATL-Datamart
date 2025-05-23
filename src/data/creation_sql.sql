-- Création des dimensions

CREATE TABLE dim_passenger_count (
    id_passenger_count SERIAL PRIMARY KEY,
    passenger_count DOUBLE PRECISION UNIQUE
);

CREATE TABLE dim_vendor (
    id_vendor SERIAL PRIMARY KEY,
    vendorid INT UNIQUE
);

CREATE TABLE dim_payment_type (
    id_payment_type SERIAL PRIMARY KEY,
    payment_type BIGINT UNIQUE
);

CREATE TABLE dim_rate_code (
    id_rate_code SERIAL PRIMARY KEY,
    ratecodeid DOUBLE PRECISION UNIQUE
);

CREATE TABLE dim_store_and_fwd_flag (
    id_flag SERIAL PRIMARY KEY,
    flag_value TEXT UNIQUE
);

CREATE TABLE dim_datetime (
    id_datetime SERIAL PRIMARY KEY,
    datetime TIMESTAMP UNIQUE,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    second INT
);

-- Création de la table de faits

CREATE TABLE fact_course (
    id_course SERIAL PRIMARY KEY,
    pickup_datetime_id INT REFERENCES dim_datetime(id_datetime),
    dropoff_datetime_id INT REFERENCES dim_datetime(id_datetime),
    passenger_count_id INT REFERENCES dim_passenger_count(id_passenger_count),
    vendor_id INT REFERENCES dim_vendor(id_vendor),
    payment_type_id INT REFERENCES dim_payment_type(id_payment_type),
    rate_code_id INT REFERENCES dim_rate_code(id_rate_code),
    store_and_fwd_flag_id INT REFERENCES dim_store_and_fwd_flag(id_flag),
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    pulocationid INT,
    dolocationid INT
);
