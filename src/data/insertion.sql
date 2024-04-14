CREATE EXTENSION IF NOT EXISTS dblink;

INSERT INTO fact_rides(
    vendorid, ratecodeid, payment_type, pulocationid, dolocationid, passenger_count, trip_distance, store_and_fwd_flag, fare_amount, extra, mta_tax, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount, total_amount
) 
SELECT 
    vendorid, ratecodeid, payment_type, pulocationid, dolocationid, passenger_count, trip_distance, store_and_fwd_flag, fare_amount, extra, mta_tax, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount, total_amount 
FROM 
    dblink(
        'dbname=nyc_warehouse user=postgres password=admin', 
        'SELECT vendorid, ratecodeid, payment_type, pulocationid, dolocationid, passenger_count, trip_distance, store_and_fwd_flag, fare_amount, extra, mta_tax, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount, total_amount 
        FROM nyc_raw 
        WHERE (vendorid = 1 OR vendorid = 2 AND vendorid IS NOT NULL)
        AND (ratecodeid BETWEEN 1 AND 6 AND ratecodeid IS NOT NULL)
        AND (payment_type BETWEEN 1 AND 6 AND payment_type IS NOT NULL)
        AND ((pulocationid BETWEEN 1 AND 263 AND trip_distance BETWEEN 1 AND 100) OR (pulocationid = 265 AND trip_distance BETWEEN 1 AND 235) AND pulocationid IS NOT NULL)
        AND ((dolocationid BETWEEN 1 AND 263 AND trip_distance BETWEEN 1 AND 100) OR (dolocationid = 265 AND trip_distance BETWEEN 1 AND 235) AND dolocationid IS NOT NULL)
        AND (passenger_count BETWEEN 1 AND 4 AND passenger_count IS NOT NULL)
        AND (trip_distance IS NOT NULL)
        AND (store_and_fwd_flag IS NOT NULL)
        AND (fare_amount IS NOT NULL)
        AND (extra BETWEEN 0.5 AND 1 AND extra IS NOT NULL)
        AND (mta_tax = 0.5)
        AND (tolls_amount IS NOT NULL AND tolls_amount BETWEEN 0 AND 50)
        AND (improvement_surcharge IS NOT NULL)
        AND (congestion_surcharge BETWEEN 0 AND 2.5 AND congestion_surcharge IS NOT NULL)
        AND (airport_fee = 1.25 AND (pulocationid = 138 OR pulocationid = 132) OR (airport_fee = 0 AND pulocationid != 138 AND pulocationid != 132) AND airport_fee IS NOT NULL)
        AND (tpep_pickup_datetime BETWEEN ''2023-01-01 00:00:00'' AND ''2023-08-31 23:59:59'' AND tpep_pickup_datetime IS NOT NULL)
        AND (tpep_dropoff_datetime BETWEEN ''2023-01-01 00:00:00'' AND ''2023-08-31 23:59:59'' AND tpep_dropoff_datetime IS NOT NULL)
        AND (tip_amount IS NOT NULL)
        AND (total_amount IS NOT NULL)'
    ) AS t(
        vendorid INT, ratecodeid INT, payment_type INT, pulocationid INT, dolocationid INT, passenger_count FLOAT, trip_distance FLOAT, store_and_fwd_flag VARCHAR, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, congestion_surcharge FLOAT, airport_fee FLOAT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, tip_amount FLOAT, total_amount FLOAT
    );
