CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY, date DATE NOT NULL, day_of_week VARCHAR(10) NOT NULL, month VARCHAR(10) NOT NULL, year INT NOT NULL, hour INT NOT NULL, minute INT NOT NULL, UNIQUE (date, hour, minute) -- Adding a unique constraint
);

CREATE TABLE dim_location (location_key INT PRIMARY KEY);

CREATE TABLE dim_payment (payment_key INT PRIMARY KEY);

CREATE TABLE fact_rides (
    ride_id SERIAL PRIMARY KEY, vendor_id INT, pickup_datetime_key INT REFERENCES dim_date (date_key), dropoff_datetime_key INT REFERENCES dim_date (date_key), passenger_count INT, trip_distance FLOAT, ratecode_id INT, store_and_fwd_flag CHAR(1), pickup_location_key INT REFERENCES dim_location (location_key), dropoff_location_key INT REFERENCES dim_location (location_key), payment_key INT REFERENCES dim_payment (payment_key), fare_amount NUMERIC(10, 2), extra NUMERIC(10, 2), mta_tax NUMERIC(10, 2), tip_amount NUMERIC(10, 2), tolls_amount NUMERIC(10, 2), improvement_surcharge NUMERIC(10, 2), total_amount NUMERIC(10, 2), congestion_surcharge NUMERIC(10, 2), airport_fee NUMERIC(10, 2)
);

CREATE INDEX idx_pickup_date ON fact_rides (pickup_datetime_key);

CREATE INDEX idx_dropoff_date ON fact_rides (dropoff_datetime_key);

CREATE INDEX idx_pickup_location ON fact_rides (pickup_location_key);

CREATE INDEX idx_dropoff_location ON fact_rides (dropoff_location_key);

CREATE INDEX idx_payment_type ON fact_rides (payment_key);