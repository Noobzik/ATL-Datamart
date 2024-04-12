
INSERT INTO nyc_warehouse.public.triptpe (
    tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, distance,
    tips, fare_amount, tolls_amount, total_amounts, airport_fee, extra_charges,
    mta_tax, rate_id, fwd_flag, congestion_surcharge, improvement_surcharge
)
SELECT
    tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
    tip_amount, fare_amount, tolls_amount, total_amount,COALESCE(NULLIF(airport_fee, ''), '0')::double precision, extra,
    mta_tax, ratecodeid, store_and_fwd_flag, congestion_surcharge, improvement_surcharge
FROM
    nyc_warehouse.public.nyc_raw WHERE ratecodeid IS NOT NULL  AND ratecodeid >=0 AND ratecodeid <= 5 and  tpep_pickup_datetime >=  %s;

INSERT INTO nyc_warehouse.public.dolocation (id, taxi_zone_name)
SELECT DISTINCT dolocationid, CONCAT('Zone', dolocationid)
FROM nyc_warehouse.public.nyc_raw n
WHERE tpep_pickup_datetime >= %s
  AND NOT EXISTS (
    SELECT 1
    FROM nyc_warehouse.public.dolocation d
    WHERE d.id = n.dolocationid
);
INSERT INTO nyc_warehouse.public.pulocation (id, taxi_zone_name)
SELECT DISTINCT pulocationid, CONCAT('Zone', pulocationid)
FROM nyc_warehouse.public.nyc_raw n
WHERE tpep_pickup_datetime >= %s
  AND NOT EXISTS (
    SELECT 1
    FROM nyc_warehouse.public.pulocation p
    WHERE p.id = n.pulocationid
);

INSERT INTO nyc_warehouse.public.taxifact ( vendor_id, payment_id, trip_id, dolocation_id, pulocation_id)
SELECT
    vendorid, payment_type,
    (SELECT id FROM nyc_warehouse.public.triptpe limit 1)
    , dolocationid, pulocationid
FROM
    nyc_warehouse.public.nyc_raw where payment_type >= 1 and payment_type <= 6
       and vendorid >= 1 and vendorid <= 2 and tpep_pickup_datetime >=  %s;

/* sugère moi une requete afin d'analyser les données */
SELECT
    tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, distance,
    tips, fare_amount, tolls_amount, total_amounts, airport_fee, extra_charges,
    mta_tax, rate_id, fwd_flag, congestion_surcharge, improvement_surcharge
FROM
    nyc_warehouse.public.triptpe
WHERE
    tpep_pickup_datetime >= %s
