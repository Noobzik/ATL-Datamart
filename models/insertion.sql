/** BDD DATAMART*/

-- Insertion des données dans les tables de dimensions depuis la base warehouse vers la base datamart

-- Insertion des données dans la table dim_datetime
INSERT INTO nyc_datamart.public.dim_datetime (tpep_pickup_datetime, tpep_dropoff_datetime)
SELECT tpep_pickup_datetime, tpep_dropoff_datetime
FROM nyc_warehouse.nyc_raw;

--TODO check vendor name
-- Insertion des données dans la table dim_vendor
INSERT INTO nyc_datamart.public.dim_vendor (name)
VALUES('Creative Mobile Technologies, LLC'), ('VeriFone Inc.');

-- Insertion des données dans la table dim_payment_type
INSERT INTO nyc_datamart.public.dim_payment_type (payment_type)
VALUES('Standard Rate'), ('JFK'), ('Newark'), ('Nasssau or Westchester'), ('Negociated fare'), ('Group ride');

-- Insertion des données dans la table dim_rate_code
INSERT INTO nyc_datamart.public.dim_rate_code (zone)
VALUES ('Credit card'), ('Cash'), ('No charge'), ('Dispute'), ('Unknown'), ('Group ride');


-- Insertion des données dans la table dim_taximeter_engagement_zones_dimension
-- Insertion des données dans la table dim_taximeter_engagement_zones_dimension
INSERT INTO nyc_datamart.public.dim_taximeter_engagement_zones_dimension (pu_location, po_location, location_id)
SELECT pu_location, po_location, dl.id
FROM nyc_warehouse.nyc_raw nr
JOIN nyc_datamart.public.dim_location dl ON (nr.borough = dl.borough AND nr.zone = dl.zone);

-- Insertion des données dans la table fact_taxi_trip
INSERT INTO nyc_datamart.public.fact_taxi_trip (passenger_count, trip_distance, store_and_fwd_flag, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, vendor_id, engagement_datetime_id, payment_type_id, rate_code_id, taximeter_engagement_zones_id)
SELECT passenger_count, trip_distance, store_and_fwd_flag, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, dv.id AS vendor_id, dd.id AS engagement_datetime_id, dpt.id AS payment_type_id, drc.id AS rate_code_id, dt.id AS taximeter_engagement_zones_id
FROM nyc_warehouse.nyc_raw nr
JOIN nyc_datamart.public.dim_vendor dv ON (nr.vendor_name = dv.name)
JOIN nyc_datamart.public.dim_datetime dd ON (nr.tpep_pickup_datetime = dd.tpep_pickup_datetime AND nr.tpep_dropoff_datetime = dd.tpep_dropoff_datetime)
JOIN nyc_datamart.public.dim_payment_type dpt ON (nr.payment_type = dpt.payment_type)
JOIN nyc_datamart.public.dim_rate_code drc ON (nr.rate_code_id = drc.zone)
JOIN nyc_datamart.public.dim_taximeter_engagement_zones_dimension dt ON (nr.pu_location = dt.pu_location AND nr.po_location = dt.po_location);