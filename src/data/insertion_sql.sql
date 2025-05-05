-- Insertion des dimensions avec affichage

INSERT INTO dim_passenger_count (passenger_count)
SELECT DISTINCT passenger_count FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_passenger_count - Total :' AS info, COUNT(*) FROM dim_passenger_count;

INSERT INTO dim_vendor (vendorid)
SELECT DISTINCT vendorid FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_vendor - Total :' AS info, COUNT(*) FROM dim_vendor;

INSERT INTO dim_payment_type (payment_type)
SELECT DISTINCT payment_type FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_payment_type - Total :' AS info, COUNT(*) FROM dim_payment_type;

INSERT INTO dim_rate_code (ratecodeid)
SELECT DISTINCT ratecodeid FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_rate_code - Total :' AS info, COUNT(*) FROM dim_rate_code;

INSERT INTO dim_store_and_fwd_flag (flag_value)
SELECT DISTINCT store_and_fwd_flag FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_store_and_fwd_flag - Total :' AS info, COUNT(*) FROM dim_store_and_fwd_flag;

INSERT INTO dim_datetime (datetime, year, month, day, hour, minute, second)
SELECT DISTINCT tpep_pickup_datetime,
    EXTRACT(YEAR FROM tpep_pickup_datetime),
    EXTRACT(MONTH FROM tpep_pickup_datetime),
    EXTRACT(DAY FROM tpep_pickup_datetime),
    EXTRACT(HOUR FROM tpep_pickup_datetime),
    EXTRACT(MINUTE FROM tpep_pickup_datetime),
    EXTRACT(SECOND FROM tpep_pickup_datetime)
FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_datetime (pickup) - Total :' AS info, COUNT(*) FROM dim_datetime;

INSERT INTO dim_datetime (datetime, year, month, day, hour, minute, second)
SELECT DISTINCT tpep_dropoff_datetime,
    EXTRACT(YEAR FROM tpep_dropoff_datetime),
    EXTRACT(MONTH FROM tpep_dropoff_datetime),
    EXTRACT(DAY FROM tpep_dropoff_datetime),
    EXTRACT(HOUR FROM tpep_dropoff_datetime),
    EXTRACT(MINUTE FROM tpep_dropoff_datetime),
    EXTRACT(SECOND FROM tpep_dropoff_datetime)
FROM nyc_raw
ON CONFLICT DO NOTHING;

SELECT 'dim_datetime (total pickup + dropoff) - Total :' AS info, COUNT(*) FROM dim_datetime;
