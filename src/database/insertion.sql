INSERT INTO time_dimension (pickup_datetime, dropoff_datetime, trip_distance)
SELECT DISTINCT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance FROM nyc_raw;

INSERT INTO payment_dimension (payment_type, congestion_surcharge, fare_amount, tolls_amount, tip_amount, extra, airport_fee, mta_tax, total_amount)
SELECT DISTINCT payment_type, congestion_surcharge, fare_amount, tolls_amount, tip_amount, extra, airport_fee, mta_tax, total_amount FROM nyc_raw;

INSERT INTO taxi_activity (
    vendorid,
    time_id,
    payment_id,
    passenger_count,
    trip_distance,
    improvement_surcharge
)
SELECT
    nyc_raw.vendorid,
    time_dimension.time_id,
    payment_dimension.payment_id,
    nyc_raw.passenger_count,
    nyc_raw.trip_distance,
    nyc_raw.improvement_surcharge
FROM nyc_raw
JOIN time_dimension ON time_dimension.trip_distance = nyc_raw.trip_distance
JOIN payment_dimension ON payment_dimension.fare_amount = nyc_raw.fare_amount
                        AND payment_dimension.total_amount = nyc_raw.total_amount;
