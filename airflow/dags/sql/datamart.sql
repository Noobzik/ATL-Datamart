-- ====================================
-- DIMENSION TABLES
-- ====================================

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendorid INTEGER PRIMARY KEY,
    vendor_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type INTEGER PRIMARY KEY,
    payment_description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_ratecode (
    ratecodeid INTEGER PRIMARY KEY,
    rate_description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    locationid INTEGER PRIMARY KEY,
    borough TEXT NOT NULL,
    zone TEXT NOT NULL,
    service_zone TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    datetime TIMESTAMP PRIMARY KEY,
    year INTEGER NOT NULL CHECK (year >= 2000),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    hour INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    weekday TEXT NOT NULL CHECK (weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
);

CREATE TABLE IF NOT EXISTS dim_trip_flag (
    store_and_fwd_flag CHAR(1) PRIMARY KEY CHECK (store_and_fwd_flag IN ('Y', 'N')),
    flag_description TEXT NOT NULL
);
-- ====================================
-- FACT TABLE
-- ====================================

CREATE TABLE IF NOT EXISTS fact_trip (
    rowid BIGSERIAL PRIMARY KEY,
    vendorid INTEGER NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count SMALLINT NOT NULL CHECK (passenger_count >= 0),
    trip_distance NUMERIC(8, 3) NOT NULL CHECK (trip_distance >= 0),
    ratecodeid INTEGER,
    store_and_fwd_flag CHAR(1),
    pulocationid INTEGER NOT NULL,
    dolocationid INTEGER NOT NULL,
    payment_type INTEGER,
    fare_amount NUMERIC(10, 2) NOT NULL CHECK (fare_amount >= 0),
    extra NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (extra >= 0),
    mta_tax NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (mta_tax >= 0),
    tip_amount NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (tip_amount >= 0),
    tolls_amount NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (tolls_amount >= 0),
    improvement_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (improvement_surcharge >= 0),
    total_amount NUMERIC(10, 2) NOT NULL CHECK (total_amount >= 0),
    congestion_surcharge NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (congestion_surcharge >= 0),
    airport_fee NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (airport_fee >= 0),
    cbd_congestion_fee NUMERIC(10, 2) NOT NULL DEFAULT 0 CHECK (cbd_congestion_fee >= 0),
    FOREIGN KEY (vendorid) REFERENCES dim_vendor(vendorid) ON DELETE SET NULL,
    FOREIGN KEY (ratecodeid) REFERENCES dim_ratecode(ratecodeid) ON DELETE SET NULL,
    FOREIGN KEY (payment_type) REFERENCES dim_payment_type(payment_type) ON DELETE SET NULL,
    FOREIGN KEY (pulocationid) REFERENCES dim_location(locationid) ON DELETE SET NULL,
    FOREIGN KEY (dolocationid) REFERENCES dim_location(locationid) ON DELETE SET NULL,
    FOREIGN KEY (pickup_datetime) REFERENCES dim_time(datetime) ON DELETE CASCADE,
    FOREIGN KEY (dropoff_datetime) REFERENCES dim_time(datetime) ON DELETE CASCADE,
    FOREIGN KEY (store_and_fwd_flag) REFERENCES dim_trip_flag(store_and_fwd_flag) ON DELETE SET NULL
);
-- ====================================
-- INDEXES FOR PERFORMANCE
-- ====================================

CREATE INDEX idx_trip_pickup_time ON fact_trip(pickup_datetime);
CREATE INDEX idx_trip_dropoff_time ON fact_trip(dropoff_datetime);
CREATE INDEX idx_trip_locations ON fact_trip(pulocationid, dolocationid);
CREATE INDEX idx_trip_vendor ON fact_trip(vendorid);
CREATE INDEX idx_trip_payment ON fact_trip(payment_type);
CREATE INDEX idx_trip_ratecode ON fact_trip(ratecodeid);
