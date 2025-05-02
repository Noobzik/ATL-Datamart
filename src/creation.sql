-- Dimension Temps
CREATE TABLE dim_temps (
    temps_id SERIAL PRIMARY KEY,
    date DATE,
    heure INTEGER,
    jour_semaine INTEGER,
    mois INTEGER,
    annee INTEGER
);

-- Dimension Géographique
CREATE TABLE dim_zone (
    zone_id INTEGER PRIMARY KEY,  
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    pu_location_id INTEGER REFERENCES dim_zone(zone_id),
    do_location_id INTEGER REFERENCES dim_zone(zone_id)
);

-- Dimension Paiement
CREATE TABLE dim_paiement (
    paiement_id SERIAL PRIMARY KEY,
    payment_type INTEGER,
    payment_desc VARCHAR(50)
);

-- Dimension Tarif
CREATE TABLE dim_tarif (
    tarif_id SERIAL PRIMARY KEY,
    rate_code_id INTEGER,
    rate_code_desc VARCHAR(100)
);

-- Dimension Vendor
CREATE TABLE dim_vendor (
    vendor_id SERIAL PRIMARY KEY,
    vendor_code INTEGER,
    vendor_name VARCHAR(100)
);

-- Table de faits
CREATE TABLE fact_courses (
    course_id SERIAL PRIMARY KEY,
    temps_id INTEGER REFERENCES dim_temps(temps_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    paiement_id INTEGER REFERENCES dim_paiement(paiement_id),
    tarif_id INTEGER REFERENCES dim_tarif(tarif_id),
    vendor_id INTEGER REFERENCES dim_vendor(vendor_id),
    
    -- Mesures
    passenger_count INTEGER,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    
    -- Indicateur
    store_and_fwd_flag VARCHAR(1)
);

-- Données de référence
INSERT INTO dim_paiement (payment_type, payment_desc) VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip');

INSERT INTO dim_tarif (rate_code_id, rate_code_desc) VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Null/unknown');

INSERT INTO dim_vendor (vendor_code, vendor_name) VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'Curb Mobility, LLC'),
    (6, 'Myle Technologies Inc'),
    (7, 'Helix');

-- Table temporaire pour l'import du CSV
CREATE TABLE temp_taxi_zones (
    LocationID INTEGER,
    Borough VARCHAR(50),
    Zone VARCHAR(100),
    service_zone VARCHAR(50)
);