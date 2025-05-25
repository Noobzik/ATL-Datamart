INSERT INTO dim_vendor (vendor_id, vendor_name)
SELECT DISTINCT VendorID, 
       CASE VendorID
           WHEN 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN 2 THEN 'Curb Mobility, LLC'
           WHEN 6 THEN 'Myle Technologies Inc'
           WHEN 7 THEN 'Helix'
       END
FROM foreign_table;

INSERT INTO dim_rate_code (rate_code_id, rate_description)
SELECT DISTINCT RatecodeID,
       CASE RatecodeID
           WHEN 1 THEN 'Standard rate'
           WHEN 2 THEN 'JFK'
           WHEN 3 THEN 'Newark'
           WHEN 4 THEN 'Nassau or Westchester'
           WHEN 5 THEN 'Negotiated fare'
           WHEN 6 THEN 'Group ride'
           WHEN 99 THEN 'Null/unknown'
       END
FROM foreign_table WHERE RatecodeID IS NOT NULL;;

INSERT INTO dim_payment_type (payment_type_id, payment_description)
SELECT DISTINCT payment_type,
       CASE payment_type
           WHEN 0 THEN 'Flex Fare trip'
           WHEN 1 THEN 'Credit card'
           WHEN 2 THEN 'Cash'
           WHEN 3 THEN 'No charge'
           WHEN 4 THEN 'Dispute'
           WHEN 5 THEN 'Unknown'
           WHEN 6 THEN 'Voided trip'
       END
FROM foreign_table;

INSERT INTO dim_location (location_id, location_description)
SELECT DISTINCT PULocationID, CONCAT('Zone ', PULocationID) FROM foreign_table
UNION
SELECT DISTINCT DOLocationID, CONCAT('Zone ', DOLocationID) FROM foreign_table;

-- Remplir la dimension temps
INSERT INTO dim_datetime (pickup_datetime, dropoff_datetime)
SELECT DISTINCT
    tpep_pickup_datetime,
    tpep_dropoff_datetime
FROM foreign_table;

-- Remplir la table de faits
INSERT INTO fact_trips (
    vendor_id, rate_code_id, payment_type_id, pickup_location_id, dropoff_location_id, datetime_id,
    passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    store_and_fwd_flag
)
SELECT
    yt.VendorID,
    yt.RatecodeID,
    yt.payment_type,
    yt.PULocationID,
    yt.DOLocationID,
    d.datetime_id,
    yt.passenger_count,
    yt.trip_distance,
    yt.fare_amount,
    yt.extra,
    yt.mta_tax,
    yt.tip_amount,
    yt.tolls_amount,
    yt.improvement_surcharge,
    yt.total_amount,
    yt.congestion_surcharge,
    yt.airport_fee,
    yt.store_and_fwd_flag
FROM foreign_table yt
JOIN dim_datetime d ON yt.tpep_pickup_datetime = d.pickup_datetime AND yt.tpep_dropoff_datetime = d.dropoff_datetime;