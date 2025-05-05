-- TP3 - Création du modèle de données pour l’analyse (Datamart NYC)
-- Structure en étoile avec séparation entre dimensions et faits
-- Base dédiée : nyc_datamart (port PostgreSQL : 15435)

-- Nettoyage préalable pour éviter les conflits lors des ré-exécutions
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_payment CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;
DROP TABLE IF EXISTS dim_zone CASCADE;
DROP TABLE IF EXISTS dim_borough CASCADE;

-- ===== Création des tables de dimensions =====

-- Table temps : permet l’analyse par date, heure, jour, week-end, etc.
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    pickup_date DATE NOT NULL,
    pickup_time TIME NOT NULL,
    hour_of_day INTEGER NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week VARCHAR(9) NOT NULL CHECK (day_of_week IN ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')),
    is_weekend BOOLEAN NOT NULL,
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INTEGER NOT NULL,
    CONSTRAINT uq_pickup_datetime UNIQUE (pickup_datetime)
);

-- Table des arrondissements (boroughs) de NYC
CREATE TABLE dim_borough (
    borough_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

-- Table des zones associées aux arrondissements
CREATE TABLE dim_zone (
    zone_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    borough_id INTEGER REFERENCES dim_borough(borough_id),
    CONSTRAINT uq_zone_name UNIQUE (name, borough_id)
);

-- Table regroupant les trajets entre zones, avec distance
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    pickup_zone_id INTEGER REFERENCES dim_zone(zone_id) NOT NULL,
    dropoff_zone_id INTEGER REFERENCES dim_zone(zone_id) NOT NULL,
    distance_miles DECIMAL(8,2) NOT NULL CHECK (distance_miles >= 0),
    CONSTRAINT uq_location_combo UNIQUE (pickup_zone_id, dropoff_zone_id, distance_miles)
);

-- Table contenant les moyens de paiement et types de tarifs
CREATE TABLE dim_payment (
    payment_id SERIAL PRIMARY KEY,
    payment_type VARCHAR(50) NOT NULL,
    rate_code VARCHAR(50) NOT NULL,
    CONSTRAINT uq_payment_combo UNIQUE (payment_type, rate_code)
);

-- Table des compagnies de taxi (avec validité dans le temps si historique)
CREATE TABLE dim_vendor (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(50) NOT NULL UNIQUE,
    valid_from DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT NULL
);

-- ===== Table de faits principale : les trajets =====

CREATE TABLE fact_trips (
    trip_id UUID PRIMARY KEY,
    time_id INTEGER REFERENCES dim_time(time_id) NOT NULL,
    location_id INTEGER REFERENCES dim_location(location_id) NOT NULL,
    payment_id INTEGER REFERENCES dim_payment(payment_id) NOT NULL,
    vendor_id INTEGER REFERENCES dim_vendor(vendor_id) NOT NULL,
    passenger_count INTEGER NOT NULL CHECK (passenger_count BETWEEN 1 AND 9),
    fare_amount DECIMAL(10,2) NOT NULL CHECK (fare_amount >= 0),
    tip_amount DECIMAL(10,2) NOT NULL CHECK (tip_amount >= 0),
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    congestion_surcharge DECIMAL(10,2) DEFAULT 0.00 CHECK (congestion_surcharge >= 0),
    airport_fee DECIMAL(10,2) DEFAULT 0.00 CHECK (airport_fee >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ===== Ajout d’index pour améliorer les performances des analyses =====

CREATE INDEX idx_fact_trips_time ON fact_trips(time_id);
CREATE INDEX idx_fact_trips_location ON fact_trips(location_id);
CREATE INDEX idx_fact_trips_vendor ON fact_trips(vendor_id);
CREATE INDEX idx_fact_trips_payment ON fact_trips(payment_id);
CREATE INDEX idx_fact_trips_amounts ON fact_trips(fare_amount, tip_amount, total_amount);

-- Index combinés pour les requêtes fréquentes
CREATE INDEX idx_fact_trips_time_location ON fact_trips(time_id, location_id);
CREATE INDEX idx_fact_trips_vendor_time ON fact_trips(vendor_id, time_id);

-- ===== Documentation du modèle avec des commentaires métier =====

COMMENT ON TABLE fact_trips IS 'Table centrale regroupant les trajets de taxis new-yorkais';
COMMENT ON COLUMN fact_trips.congestion_surcharge IS 'Montant ajouté pour la circulation dense à NYC';
COMMENT ON COLUMN dim_zone.name IS 'Nom officiel de la zone géographique tel que défini par la NYC TLC';

-- ===== Vue matérialisée pour agrégations pré-calculées (optionnel) =====

CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT 
    t.pickup_date,
    z.name AS pickup_zone,
    SUM(f.fare_amount) AS daily_revenue,
    COUNT(*) AS trip_count
FROM fact_trips f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_zone z ON l.pickup_zone_id = z.zone_id
GROUP BY t.pickup_date, z.name;

CREATE UNIQUE INDEX idx_mv_daily_revenue ON mv_daily_revenue(pickup_date, pickup_zone);
