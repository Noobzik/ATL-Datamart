-- insertion.sql

BEGIN;

INSERT INTO dim_vendor (vendor_id, vendor_name)
SELECT DISTINCT VendorID
FROM trips, 
ON CONFLICT (vendor_id) DO NOTHING;

-- dim_rate_code
INSERT INTO dim_rate_code (rate_code_id, rate_code_name)
SELECT DISTINCT RatecodeID
FROM trips,
ON CONFLICT (rate_code_id) DO NOTHING;

-- dim_payment_type
INSERT INTO dim_payment_type (payment_type_id, payment_type_name)
SELECT DISTINCT payment_type,
       CASE payment_type
           WHEN 1 THEN 'Credit card'
           WHEN 2 THEN 'Cash'
           WHEN 3 THEN 'No charge'
           WHEN 4 THEN 'Dispute'
           WHEN 5 THEN 'Unknown'
           WHEN 6 THEN 'Voided trip'
           ELSE 'Invalid payment'
       END
FROM trips
ON CONFLICT (payment_type_id) DO NOTHING;

-- dim_time
INSERT INTO dim_time (time_id, hour, day, month, year, day_of_week, is_weekend)
SELECT 
    tpep_pickup_datetime,
    EXTRACT(HOUR FROM tpep_pickup_datetime),
    EXTRACT(DAY FROM tpep_pickup_datetime),
    EXTRACT(MONTH FROM tpep_pickup_datetime),
    EXTRACT(YEAR FROM tpep_pickup_datetime),
    EXTRACT(DOW FROM tpep_pickup_datetime),
    EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6)
FROM trips
ON CONFLICT (time_id) DO NOTHING;

-- dim_time
INSERT INTO dim_time (time_id, hour, day, month, year, day_of_week, is_weekend)
SELECT 
    tpep_dropoff_datetime,
    EXTRACT(HOUR FROM tpep_dropoff_datetime),
    EXTRACT(DAY FROM tpep_dropoff_datetime),
    EXTRACT(MONTH FROM tpep_dropoff_datetime),
    EXTRACT(YEAR FROM tpep_dropoff_datetime),
    EXTRACT(DOW FROM tpep_dropoff_datetime),
    EXTRACT(DOW FROM tpep_dropoff_datetime) IN (0, 6)
FROM trips
ON CONFLICT (time_id) DO NOTHING;

-- =============================================

INSERT INTO fact_trips (
    vendor_id,
    pickup_time_id,
    dropoff_time_id,
    passenger_count,
    trip_distance,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    store_and_fwd_flag
)
SELECT 
    t.VendorID,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.RatecodeID,
    t.PULocationID,
    t.DOLocationID,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.Airport_fee,
    t.store_and_fwd_flag
FROM trips t;


-- derived duration
UPDATE fact_trips
SET trip_duration_seconds = EXTRACT(EPOCH FROM (dropoff_time_id - pickup_time_id));

-- speed in MPH 
UPDATE fact_trips
SET speed_mph = CASE 
    WHEN trip_duration_seconds > 0 THEN (trip_distance / (trip_duration_seconds / 3600))
    ELSE NULL 
END;

COMMIT;

-- 4. Data validation queries (optional)
-- =============================================

-- Check for records that didn't get proper dimension references
SELECT COUNT(*) AS orphaned_records
FROM fact_trips
WHERE pickup_location_id NOT IN (SELECT location_id FROM dim_location)
   OR dropoff_location_id NOT IN (SELECT location_id FROM dim_location);

-- Compare record counts
SELECT 
    (SELECT COUNT(*) FROM trips) AS source_count,
    (SELECT COUNT(*) FROM fact_trips) AS target_count;