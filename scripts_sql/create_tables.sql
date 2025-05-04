-- Création de la table dim_time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP
);

-- Création de la table dim_vendor
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id INTEGER PRIMARY KEY
);

-- Création de la table fact_trips
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    fare_amount FLOAT,
    total_amount FLOAT,
    FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id)
);
