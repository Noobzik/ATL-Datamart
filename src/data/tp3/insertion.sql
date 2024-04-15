INSERT INTO
    dim_date (
        date, day_of_week, month, year, hour, minute
    )
SELECT DISTINCT
    CAST(datetime AS DATE) AS date,
    TO_CHAR (datetime, 'Day') AS day_of_week,
    TO_CHAR (datetime, 'Month') AS month,
    EXTRACT(
        YEAR
        FROM datetime
    ) AS year,
    EXTRACT(
        HOUR
        FROM datetime
    ) AS hour,
    EXTRACT(
        MINUTE
        FROM datetime
    ) AS minute
FROM (
        SELECT tpep_pickup_datetime AS datetime
        FROM nyc
        UNION
        SELECT tpep_dropoff_datetime AS datetime
        FROM nyc
    ) AS datetimes ON CONFLICT (date, hour, minute) DO NOTHING;

INSERT INTO
    dim_location (location_key)
SELECT DISTINCT
    location_id
FROM (
        SELECT pulocationid AS location_id
        FROM nyc
        UNION
        SELECT dolocationid AS location_id
        FROM nyc
    ) AS location_ids
WHERE
    location_id IS NOT NULL ON CONFLICT (location_key) DO NOTHING;

INSERT INTO
    dim_payment (payment_key)
SELECT DISTINCT
    payment_type
FROM nyc
WHERE
    payment_type IS NOT NULL ON CONFLICT (payment_key) DO NOTHING;

INSERT INTO
    fact_rides (
        vendor_id, pickup_datetime_key, dropoff_datetime_key, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pickup_location_key, dropoff_location_key, payment_key, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
    )
SELECT
    n.vendorid,
    pd.date_key AS pickup_datetime_key,
    dd.date_key AS dropoff_datetime_key,
    n.passenger_count,
    n.trip_distance,
    n.ratecodeid,
    n.store_and_fwd_flag,
    n.pulocationid AS pickup_location_key,
    n.dolocationid AS dropoff_location_key,
    n.payment_type AS payment_key,
    n.fare_amount,
    n.extra,
    n.mta_tax,
    n.tip_amount,
    n.tolls_amount,
    n.improvement_surcharge,
    n.total_amount,
    n.congestion_surcharge,
    n.airport_fee
FROM
    nyc n
    JOIN dim_date pd ON pd.date = CAST(
        n.tpep_pickup_datetime AS DATE
    )
    AND pd.hour = EXTRACT(
        HOUR
        FROM n.tpep_pickup_datetime
    )
    AND pd.minute = EXTRACT(
        MINUTE
        FROM n.tpep_pickup_datetime
    )
    JOIN dim_date dd ON dd.date = CAST(
        n.tpep_dropoff_datetime AS DATE
    )
    AND dd.hour = EXTRACT(
        HOUR
        FROM n.tpep_dropoff_datetime
    )
    AND dd.minute = EXTRACT(
        MINUTE
        FROM n.tpep_dropoff_datetime
    )
    JOIN dim_location pl ON pl.location_key = n.pulocationid
    JOIN dim_location dl ON dl.location_key = n.dolocationid
    JOIN dim_payment p ON p.payment_key = n.payment_type;