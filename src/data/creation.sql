-- Flocon model for the data

-- Dimension Tables
-- Vendor
DROP TABLE IF EXISTS dim_vendor CASCADE;
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Location
DROP TABLE IF EXISTS dim_location CASCADE;
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    borough VARCHAR(255),
    zone VARCHAR(255),
    service_zone VARCHAR(255)
);

-- Rate Code
DROP TABLE IF EXISTS dim_rate_code CASCADE;
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    description VARCHAR(255)
);

-- Store and Forward Flag
-- CREATE TABLE IF NOT EXISTS dim_store_and_fwd_flag (
--     store_and_fwd_flag_id VARCHAR(1) PRIMARY KEY,
--     description VARCHAR(255)
-- );

-- Payment
DROP TABLE IF EXISTS dim_payment_type CASCADE;
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    description VARCHAR(255)
);

-- Fact Table
DROP TABLE IF EXISTS fact_trip CASCADE;
CREATE TABLE IF NOT EXISTS fact_trip (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag BOOLEAN, -- I choose to use a boolean instead of 'Y' or 'N' to simplify the model
    pulocationid INT,
    dolocationid INT,
    payment_type_id INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    CONSTRAINT fk_vendor_id FOREIGN KEY (vendor_id) REFERENCES dim_vendor (vendor_id),
    CONSTRAINT fk_rate_code_id FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code (rate_code_id),
    -- CONSTRAINT fk_store_and_fwd_flag_id FOREIGN KEY (store_and_fwd_flag) REFERENCES dim_store_and_fwd_flag (store_and_fwd_flag_id),
    CONSTRAINT fk_pulocationid FOREIGN KEY (pulocationid) REFERENCES dim_location (location_id),
    CONSTRAINT fk_dolocationid FOREIGN KEY (dolocationid) REFERENCES dim_location (location_id),
    CONSTRAINT fk_payment_type_id FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type (payment_type_id)
);

-- 
DROP TABLE IF EXISTS fact_trip_without_outliers CASCADE;

CREATE TABLE IF NOT EXISTS fact_trip_without_outliers (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag BOOLEAN, -- I choose to use a boolean instead of 'Y' or 'N' to simplify the model
    pulocationid INT,
    dolocationid INT,
    payment_type_id INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    CONSTRAINT fk_vendor_id FOREIGN KEY (vendor_id) REFERENCES dim_vendor (vendor_id),
    CONSTRAINT fk_rate_code_id FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code (rate_code_id),
    -- CONSTRAINT fk_store_and_fwd_flag_id FOREIGN KEY (store_and_fwd_flag) REFERENCES dim_store_and_fwd_flag (store_and_fwd_flag_id),
    CONSTRAINT fk_pulocationid FOREIGN KEY (pulocationid) REFERENCES dim_location (location_id),
    CONSTRAINT fk_dolocationid FOREIGN KEY (dolocationid) REFERENCES dim_location (location_id),
    CONSTRAINT fk_payment_type_id FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type (payment_type_id)
);


-- To respect the OLAP model, we can create the following tables, but we will not use them in the project to simplify the model
-- Trip Date
-- CREATE TABLE IF NOT EXISTS dim_year(
--     year_id SERIAL PRIMARY KEY,
--     year INT
-- );

-- CREATE TABLE IF NOT EXISTS dim_month (
--     month_id SERIAL PRIMARY KEY,
--     month INT,
--     year_id INT REFERENCES dim_year (year)
-- );

-- CREATE TABLE IF NOT EXISTS dim_day (
--     day_id SERIAL PRIMARY KEY,
--     day INT,
--     month_id INT REFERENCES dim_month (month_id)
-- );

-- CREATE TABLE IF NOT EXISTS dim_trip_datetime (
--     trip_datetime_id SERIAL PRIMARY KEY,
--     trip_time TIME,
--     trip_day INT
-- );

-- Location
-- CREATE TABLE IF NOT EXISTS dim_service_zone (
--     service_zone_id INT PRIMARY KEY,
--     name VARCHAR(255)
-- );

-- CREATE TABLE IF NOT EXISTS dim_zone (
--     zone_id SERIAL PRIMARY KEY,
--     name VARCHAR(255),
--     service_zone_id INT REFERENCES dim_service_zone (service_zone_id)
-- );

-- CREATE TABLE IF NOT EXISTS dim_borough (
--     borough_id INT PRIMARY KEY,
--     name VARCHAR(255)
-- );

-- CREATE TABLE IF NOT EXISTS dim_location(
--     location_id SERIAL PRIMARY KEY,
--     borough_id INT REFERENCES dim_borough (borough_id),
--     zone_id INT INT REFERENCES dim_zone (zone_id),
-- );