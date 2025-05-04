-- Insertion dans dim_time
INSERT INTO dim_time (pickup_datetime, dropoff_datetime)
SELECT pickup_datetime, dropoff_datetime
FROM dblink('dbname=nyc_warehouse user=postgres password=admin host=host.docker.internal port=15432',
            'SELECT pickup_datetime, dropoff_datetime FROM yellow_tripdata_2024_10')
AS t(pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP);

-- Insertion dans dim_vendor
INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT vendor_id
FROM dblink('dbname=nyc_warehouse user=postgres password=admin host=host.docker.internal port=15432',
            'SELECT vendor_id FROM yellow_tripdata_2024_10')
AS t(vendor_id INTEGER);

-- Insertion dans fact_trips
INSERT INTO fact_trips (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, total_amount)
SELECT vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, total_amount
FROM dblink('dbname=nyc_warehouse user=postgres password=admin host=host.docker.internal port=15432',
            'SELECT vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, total_amount
             FROM yellow_tripdata_2024_10')
AS t(vendor_id INTEGER, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance FLOAT, fare_amount FLOAT, total_amount FLOAT);
