-- ========================
-- 1. Configuration de postgres_fdw pour accéder au Data Warehouse
-- ========================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS warehouse_server CASCADE;
CREATE SERVER warehouse_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'host.docker.internal', port '15432', dbname 'nyc_warehouse');

CREATE USER MAPPING FOR postgres
    SERVER warehouse_server
    OPTIONS (user 'postgres', password 'admin');

IMPORT FOREIGN SCHEMA public
    LIMIT TO (nyc_raw)
    FROM SERVER warehouse_server
    INTO public;

-- ========================
-- 2. Insertion dans les dimensions enrichies
-- ========================


INSERT INTO dim_vendor (vendor_id, vendor_name)
VALUES
    (1, 'Creative Mobile Technologies (CMT)'),
    (2, 'VeriFone Inc. (VTS)')
ON CONFLICT DO NOTHING;

INSERT INTO dim_payment_type (payment_type_id, payment_description)
VALUES
    (1, 'Credit Card'),
    (2, 'Cash'),
    (3, 'No Charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided Trip')
ON CONFLICT DO NOTHING;

INSERT INTO dim_rate_code (rate_code_id, rate_description)
VALUES
    (1, 'Standard Rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated Fare'),
    (6, 'Group Ride')
ON CONFLICT DO NOTHING;



-- ========================
-- 3. Insertion dans dim_datetime (pickup et dropoff) avec LIMIT
-- ========================

INSERT INTO dim_datetime (full_datetime, year, month, day, hour, minute)
SELECT DISTINCT
    tpep_pickup_datetime,
    EXTRACT(YEAR FROM tpep_pickup_datetime),
    EXTRACT(MONTH FROM tpep_pickup_datetime),
    EXTRACT(DAY FROM tpep_pickup_datetime),
    EXTRACT(HOUR FROM tpep_pickup_datetime),
    EXTRACT(MINUTE FROM tpep_pickup_datetime)
FROM nyc_raw
WHERE tpep_pickup_datetime IS NOT NULL
LIMIT 10000
ON CONFLICT DO NOTHING;

INSERT INTO dim_datetime (full_datetime, year, month, day, hour, minute)
SELECT DISTINCT
    tpep_dropoff_datetime,
    EXTRACT(YEAR FROM tpep_dropoff_datetime),
    EXTRACT(MONTH FROM tpep_dropoff_datetime),
    EXTRACT(DAY FROM tpep_dropoff_datetime),
    EXTRACT(HOUR FROM tpep_dropoff_datetime),
    EXTRACT(MINUTE FROM tpep_dropoff_datetime)
FROM nyc_raw
WHERE tpep_dropoff_datetime IS NOT NULL
LIMIT 10000
ON CONFLICT DO NOTHING;

-- ========================
-- 4. Insertion dans fact_trips avec jointures (limité à 10000 lignes)
-- ========================

INSERT INTO fact_trips (
    vendor_id, payment_type_id, rate_code_id,
    pickup_datetime_id, dropoff_datetime_id,
    pickup_location_id, dropoff_location_id,
    passenger_count, trip_distance, fare_amount, extra,
    mta_tax, tip_amount, tolls_amount, improvement_surcharge,
    congestion_surcharge, airport_fee, total_amount,
    store_and_fwd_flag
)
SELECT
    v.vendor_id,
    p.payment_type_id,
    rcode.rate_code_id,
    dp.datetime_id,
    dd.datetime_id,
    pl.location_id,
    dl.location_id,
    r.passenger_count,
    r.trip_distance,
    r.fare_amount,
    r.extra,
    r.mta_tax,
    r.tip_amount,
    r.tolls_amount,
    r.improvement_surcharge,
    r.congestion_surcharge,
    r.airport_fee,
    r.total_amount,
    r.store_and_fwd_flag
FROM nyc_raw r
JOIN dim_vendor v ON v.vendor_id = r.vendorid
JOIN dim_payment_type p ON p.payment_type_id = r.payment_type
JOIN dim_rate_code rcode ON rcode.rate_code_id = r.ratecodeid
JOIN dim_datetime dp ON dp.full_datetime = date_trunc('minute', r.tpep_pickup_datetime)
JOIN dim_datetime dd ON dd.full_datetime = date_trunc('minute', r.tpep_dropoff_datetime)
JOIN dim_location pl ON pl.location_id = r.pulocationid
JOIN dim_location dl ON dl.location_id = r.dolocationid
LIMIT 10000;