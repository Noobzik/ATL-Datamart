-- 1. Enable FDW
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 2. Create foreign server to connect to data-warehouse
DROP SERVER IF EXISTS warehouse_server CASCADE;
CREATE SERVER warehouse_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'data-warehouse', dbname 'nyc_warehouse', port '5432');

-- 3. Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
    SERVER warehouse_server
    OPTIONS (user 'admin', password 'admin');

-- 4. Import foreign schema from data-warehouse
IMPORT FOREIGN SCHEMA public
    LIMIT TO (nyc_raw)
    FROM SERVER warehouse_server
    INTO public;

-- 5. Now insert data with explicit type casts and fallback values

-- dim_vendor
INSERT INTO dim_vendor (vendorid, vendor_name)
SELECT DISTINCT
    vendorid::int,
    CASE vendorid
        WHEN '1' THEN 'Creative Mobile Technologies'
        WHEN '2' THEN 'VeriFone Inc.'
        ELSE 'Unknown Vendor'
    END
FROM nyc_raw
WHERE vendorid IS NOT NULL
ON CONFLICT (vendorid) DO NOTHING;

-- dim_payment_type
INSERT INTO dim_payment_type (payment_type, payment_description)
SELECT DISTINCT
    payment_type::int,
    CASE payment_type
        WHEN '1' THEN 'Credit card'
        WHEN '2' THEN 'Cash'
        WHEN '3' THEN 'No charge'
        WHEN '4' THEN 'Dispute'
        WHEN '5' THEN 'Unknown'
        WHEN '6' THEN 'Voided trip'
        ELSE 'Other'
    END
FROM nyc_raw
WHERE payment_type IS NOT NULL
ON CONFLICT (payment_type) DO NOTHING;

-- dim_ratecode
INSERT INTO dim_ratecode (ratecodeid, rate_description)
SELECT DISTINCT
    ratecodeid::int,
    CASE ratecodeid
        WHEN '1' THEN 'Standard rate'
        WHEN '2' THEN 'JFK'
        WHEN '3' THEN 'Newark'
        WHEN '4' THEN 'Nassau/Westchester'
        WHEN '5' THEN 'Negotiated fare'
        WHEN '6' THEN 'Group ride'
        ELSE 'Other'
    END
FROM nyc_raw
WHERE ratecodeid IS NOT NULL
ON CONFLICT (ratecodeid) DO NOTHING;

-- dim_trip_flag
INSERT INTO dim_trip_flag (store_and_fwd_flag, flag_description)
SELECT DISTINCT
    store_and_fwd_flag,
    CASE store_and_fwd_flag
        WHEN 'Y' THEN 'Stored and forwarded'
        WHEN 'N' THEN 'Not stored'
        ELSE 'Unknown'
    END
FROM nyc_raw
WHERE store_and_fwd_flag IS NOT NULL
ON CONFLICT (store_and_fwd_flag) DO NOTHING;

-- dim_time (pickup and dropoff)
INSERT INTO dim_time (datetime, year, month, day, hour, weekday)
SELECT DISTINCT
    tpep_pickup_datetime::timestamp,
    EXTRACT(YEAR FROM tpep_pickup_datetime::timestamp),
    EXTRACT(MONTH FROM tpep_pickup_datetime::timestamp),
    EXTRACT(DAY FROM tpep_pickup_datetime::timestamp),
    EXTRACT(HOUR FROM tpep_pickup_datetime::timestamp),
    TO_CHAR(tpep_pickup_datetime::timestamp, 'Day')
FROM nyc_raw
WHERE tpep_pickup_datetime IS NOT NULL
ON CONFLICT (datetime) DO NOTHING;

INSERT INTO dim_time (datetime, year, month, day, hour, weekday)
SELECT DISTINCT
    tpep_dropoff_datetime::timestamp,
    EXTRACT(YEAR FROM tpep_dropoff_datetime::timestamp),
    EXTRACT(MONTH FROM tpep_dropoff_datetime::timestamp),
    EXTRACT(DAY FROM tpep_dropoff_datetime::timestamp),
    EXTRACT(HOUR FROM tpep_dropoff_datetime::timestamp),
    TO_CHAR(tpep_dropoff_datetime::timestamp, 'Day')
FROM nyc_raw
WHERE tpep_dropoff_datetime IS NOT NULL
ON CONFLICT (datetime) DO NOTHING;

-- dim_location
INSERT INTO dim_location (locationid)
SELECT DISTINCT pulocationid::int
FROM nyc_raw
WHERE pulocationid IS NOT NULL
ON CONFLICT (locationid) DO NOTHING;

INSERT INTO dim_location (locationid)
SELECT DISTINCT dolocationid::int
FROM nyc_raw
WHERE dolocationid IS NOT NULL
ON CONFLICT (locationid) DO NOTHING;

-- fact_trip
INSERT INTO fact_trip (
    vendorid, pickup_datetime, dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid,
    dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, total_amount, congestion_surcharge,
    airport_fee, cbd_congestion_fee
)
SELECT
    vendorid::int,
    tpep_pickup_datetime::timestamp,
    tpep_dropoff_datetime::timestamp,
    passenger_count::int,
    trip_distance::float,
    ratecodeid::int,
    store_and_fwd_flag,
    pulocationid::int,
    dolocationid::int,
    payment_type::int,
    fare_amount::numeric,
    extra::numeric,
    mta_tax::numeric,
    tip_amount::numeric,
    tolls_amount::numeric,
    improvement_surcharge::numeric,
    total_amount::numeric,
    congestion_surcharge::numeric,
    airport_fee::numeric,
    cbd_congestion_fee::numeric
FROM nyc_raw
WHERE vendorid IS NOT NULL
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;
