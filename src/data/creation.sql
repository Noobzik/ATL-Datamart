-- Création de la table de faits
CREATE TABLE fact_rides (
    id SERIAL PRIMARY KEY,
    vendorid INT,
    ratecodeid INT,
    payment_type INT,
    pulocationid INT,
    dolocationid INT,
    passenger_count FLOAT,
    trip_distance FLOAT,
    store_and_fwd_flag CHAR(1),
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    tip_amount FLOAT,
    total_amount FLOAT
);

-- Création des tables de dimensions
CREATE TABLE dim_vendor_sf (
    vendorid INT PRIMARY KEY,
    description VARCHAR
);

CREATE TABLE dim_location_sf (
    locationid INT PRIMARY KEY,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR
);

CREATE TABLE dim_ratecode_sf (
    ratecodeid INT PRIMARY KEY,
    description VARCHAR
);

CREATE TABLE dim_payment_sf (
    id SERIAL,
    payment_type INT UNIQUE,
    description VARCHAR,
    PRIMARY KEY (id)
);

-- Définition des contraintes de clé étrangère
ALTER TABLE fact_rides ADD CONSTRAINT fk_vendorid FOREIGN KEY (vendorid) REFERENCES dim_vendor_sf(vendorid);
ALTER TABLE fact_rides ADD CONSTRAINT fk_pulocationid FOREIGN KEY (pulocationid) REFERENCES dim_location_sf(locationid);
ALTER TABLE fact_rides ADD CONSTRAINT fk_dolocationid FOREIGN KEY (dolocationid) REFERENCES dim_location_sf(locationid);
ALTER TABLE fact_rides ADD CONSTRAINT fk_ratecodeid FOREIGN KEY (ratecodeid) REFERENCES dim_ratecode_sf(ratecodeid);
ALTER TABLE fact_rides ADD CONSTRAINT fk_payment_type FOREIGN KEY (payment_type) REFERENCES dim_payment_sf(payment_type);

INSERT INTO dim_payment_sf (payment_type, description) VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip');

INSERT INTO dim_ratecode_sf (ratecodeid, description) VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride');

INSERT INTO dim_vendor_sf (vendorid, description) VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc.');