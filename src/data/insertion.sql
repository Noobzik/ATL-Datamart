INSERT INTO
    dim_vendor (vendor_id, name)
    VALUES 
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'VeriFone Inc.'),
        (6, 'Other');

 INSERT INTO
    dim_rate_code (rate_code_id, description) 
    VALUES 
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride'),
        (99, 'Other');

-- INSERT INTO
--     dim_store_and_fwd_flag (store_and_fwd_flag_id, description)
--     VALUES 
--         (1, 'Y (store and forward trip)'),
--         (2, 'N (not a store and forward trip)');

INSERT INTO
    dim_payment_type (payment_type_id, description)
    VALUES 
        (0, 'Other'),
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip');

-- Connect to the warehouse database
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS warehouse_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    host 'localhost', port '5432', dbname 'nyc_warehouse'
);

CREATE USER MAPPING IF NOT EXISTS FOR current_user SERVER warehouse_server OPTIONS (
    user 'postgres', password 'admin'
);

CREATE FOREIGN TABLE IF NOT EXISTS nyc_raw_fdw (
    vendorid INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    ratecodeid INT,
    store_and_fwd_flag VARCHAR(1),
    pulocationid INT,
    dolocationid INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
) SERVER warehouse_server OPTIONS (schema_name 'public', table_name 'nyc_raw');

INSERT INTO 
    fact_trip (
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge, 
        airport_fee
    )
    SELECT
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        CASE store_and_fwd_flag
            WHEN 'Y' THEN TRUE
            ELSE FALSE
        END AS store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
FROM nyc_raw_fdw;

INSERT INTO 
    fact_trip_without_outliers (
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge, 
        airport_fee
    )
    SELECT
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        CASE store_and_fwd_flag
            WHEN 'Y' THEN TRUE
            ELSE FALSE
        END AS store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
FROM nyc_raw_fdw
WHERE
    tpep_pickup_datetime >= '2023-01-01' AND
    tpep_dropoff_datetime >= '2023-01-01' AND
    ratecodeid IN (1, 2, 3, 4, 5, 6) AND
    payment_type IN (1, 2, 3, 4, 5, 6) AND
    trip_distance > 0 AND
    fare_amount > 0 AND
    total_amount > 0;


