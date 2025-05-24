CREATE TABLE dim_time (
    id SERIAL PRIMARY KEY,
    pickup_date DATE,
    pickup_hour INT
);

CREATE TABLE dim_vendor (
    id SERIAL PRIMARY KEY,
    vendor_id INT
);

CREATE TABLE dim_payment (
    id SERIAL PRIMARY KEY,
    payment_type BIGINT
);

CREATE TABLE f_trips (
    id SERIAL PRIMARY KEY,
    vendor_id INT REFERENCES dim_vendor(id),
    time_id INT REFERENCES dim_time(id),
    payment_id INT REFERENCES dim_payment(id),
    passenger_count FLOAT,
    trip_distance FLOAT,
    total_amount FLOAT
);
