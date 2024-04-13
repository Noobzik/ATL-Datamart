/**BDD DATAMART*/

-- Créer les tables de dimensions
CREATE TABLE IF NOT EXISTS dim_datetime (
    id SERIAL PRIMARY KEY,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    id SERIAL PRIMARY KEY ,
    name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    id SERIAL PRIMARY KEY ,
    payment_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_rate_code (
    id SERIAL PRIMARY KEY,
    zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_service_zone (
    id SERIAL PRIMARY KEY ,
    service_zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_zone (
    id SERIAL PRIMARY KEY ,
    zone VARCHAR(50),
    service_zone_id INT,
    CONSTRAINT fk_service_zone_id FOREIGN KEY (service_zone_id) REFERENCES dim_service_zone(id)
);

CREATE TABLE IF NOT EXISTS dim_location (
    id SERIAL PRIMARY KEY ,
    borough VARCHAR(50),
    zone_id INT,
    CONSTRAINT fk_zone_id FOREIGN KEY (zone_id) REFERENCES dim_zone(id)
);

CREATE TABLE IF NOT EXISTS dim_taximeter_engagement_zones_dimension (
    id SERIAL PRIMARY KEY ,
    pu_location TIMESTAMP,
    po_location TIMESTAMP,
    location_id INT,
    CONSTRAINT fk_location_id FOREIGN KEY (location_id) REFERENCES dim_location(id)
);

-- Créer la table de faits
CREATE TABLE IF NOT EXISTS fact_taxi_trip (
    id SERIAL PRIMARY KEY ,
    passenger_count FLOAT(53),
    trip_distance FLOAT(53),
    store_and_fwd_flag VARCHAR(1) CHECK(UPPER(store_and_fwd_flag) IN ('Y', 'N')),   
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    vendor_id INT,
    engagement_datetime_id INT,
    payment_type_id INT,
    rate_code_id INT,
    taximeter_engagement_zones_id INT,
    --Contraite de clé étrangère
    CONSTRAINT fk_vendor_id FOREIGN KEY (vendor_id) REFERENCES dim_vendor(id),
    CONSTRAINT fk_engagement_datetime_id FOREIGN KEY (engagement_datetime_id) REFERENCES dim_datetime(id),
    CONSTRAINT fk_payment_type_id FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(id),
    CONSTRAINT fk_rate_code_id FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code(id),
    CONSTRAINT fk_taximeter_engagement_zones_id FOREIGN KEY (taximeter_engagement_zones_id) REFERENCES dim_taximeter_engagement_zones_dimension(id)
);
