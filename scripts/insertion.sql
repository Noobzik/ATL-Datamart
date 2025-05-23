-- Remplissage de la dimension du temps
INSERT INTO dim_time (pickup_date, pickup_hour)
SELECT DISTINCT 
    DATE(tpep_pickup_datetime) AS pickup_date,
    EXTRACT(HOUR FROM tpep_pickup_datetime)::INT AS pickup_hour
FROM nyc_raw;

-- Remplissage de la dimension vendor
INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT VendorID
FROM nyc_raw;

-- Remplissage de la dimension paiement
INSERT INTO dim_payment (payment_type)
SELECT DISTINCT payment_type
FROM nyc_raw;

-- Remplissage de la table de faits
INSERT INTO f_trips (
    vendor_id,
    time_id,
    payment_id,
    passenger_count,
    trip_distance,
    total_amount
)
SELECT
    dim_vendor.id AS vendor_id,
    dim_time.id AS time_id,
    dim_payment.id AS payment_id,
    nyc_raw.passenger_count,
    nyc_raw.trip_distance,
    nyc_raw.total_amount
FROM nyc_raw
JOIN dim_vendor ON dim_vendor.vendor_id = nyc_raw.VendorID
JOIN dim_time 
    ON dim_time.pickup_date = DATE(nyc_raw.tpep_pickup_datetime)
   AND dim_time.pickup_hour = EXTRACT(HOUR FROM nyc_raw.tpep_pickup_datetime)::INT
JOIN dim_payment ON dim_payment.payment_type = nyc_raw.payment_type;
