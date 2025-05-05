INSERT INTO fact_course (
    pickup_datetime_id,
    dropoff_datetime_id,
    passenger_count_id,
    vendor_id,
    payment_type_id,
    rate_code_id,
    store_and_fwd_flag_id,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    pulocationid,
    dolocationid
)
SELECT
    dt_pickup.id_datetime AS pickup_datetime_id,
    dt_dropoff.id_datetime AS dropoff_datetime_id,
    pc.id_passenger_count AS passenger_count_id,
    v.id_vendor AS vendor_id,
    pt.id_payment_type AS payment_type_id,
    rc.id_rate_code AS rate_code_id,
    sf.id_flag AS store_and_fwd_flag_id,
    n.trip_distance,
    n.fare_amount,
    n.extra,
    n.mta_tax,
    n.tip_amount,
    n.tolls_amount,
    n.improvement_surcharge,
    n.total_amount,
    n.congestion_surcharge,
    n.airport_fee,
    n.pulocationid,
    n.dolocationid
FROM nyc_raw n
JOIN dim_datetime dt_pickup 
    ON dt_pickup.datetime = n.tpep_pickup_datetime
JOIN dim_datetime dt_dropoff 
    ON dt_dropoff.datetime = n.tpep_dropoff_datetime
JOIN dim_passenger_count pc 
    ON pc.passenger_count = n.passenger_count
JOIN dim_vendor v 
    ON v.vendorid = n.vendorid
JOIN dim_payment_type pt 
    ON pt.payment_type = n.payment_type
JOIN dim_rate_code rc 
    ON rc.ratecodeid = n.ratecodeid
JOIN dim_store_and_fwd_flag sf 
    ON sf.flag_value = n.store_and_fwd_flag;
