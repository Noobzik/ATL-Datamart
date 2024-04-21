DROP TABLE IF EXISTS time_dimension CASCADE;
DROP TABLE IF EXISTS payment_dimension CASCADE;
DROP TABLE IF EXISTS taxi_activity CASCADE;


CREATE TABLE time_dimension (
    time_id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_distance FLOAT
);

CREATE TABLE payment_dimension (
    payment_id SERIAL PRIMARY KEY,
    payment_type VARCHAR(50),
    fare_amount FLOAT,
    congestion_surcharge FLOAT,
    tolls_amount FLOAT,
    tip_amount FLOAT,
    extra FLOAT,
    airport_fee FLOAT,
    mta_tax FLOAT,
    total_amount FLOAT
);

CREATE TABLE taxi_activity (
    activity_id SERIAL PRIMARY KEY,
    vendorid INTEGER,
    time_id INTEGER,
    payment_id INTEGER,
    passenger_count INTEGER,
    trip_distance FLOAT,
    improvement_surcharge FLOAT,
    FOREIGN KEY (time_id) REFERENCES time_dimension(time_id),
    FOREIGN KEY (payment_id) REFERENCES payment_dimension(payment_id)
);
