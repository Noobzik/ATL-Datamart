-- Dimension : Vendor
CREATE TABLE dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name TEXT
);

-- Dimension : Rate Code
CREATE TABLE dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_code_description TEXT
);

-- Dimension : Location
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    latitude FLOAT,
    longitude FLOAT
);

-- Dimension : Payment Type
CREATE TABLE dim_payment (
    payment_type_id INT PRIMARY KEY,
    payment_description TEXT
);

-- Dimension : Time
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT
);

-- Table factuelle
CREATE TABLE rides_fact (
    ride_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    vendor_id INT REFERENCES dim_vendor(vendor_id),
    rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    payment_type_id INT REFERENCES dim_payment(payment_type_id)
);
