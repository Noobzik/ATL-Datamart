DROP TABLE IF EXISTS TaxiFact;
DROP TABLE IF EXISTS Vendor;
DROP TABLE IF EXISTS Payment;
DROP TABLE IF EXISTS TripTpe;
DROP TABLE IF EXISTS Rate;
DROP TABLE IF EXISTS DOLocation;
DROP TABLE IF EXISTS PULocation;
DROP TABLE IF EXISTS Fwd_flag;



CREATE TABLE Rate (
                      id SERIAL PRIMARY KEY,
                      rate TEXT
);


CREATE TABLE PULocation (
                            id SERIAL PRIMARY KEY,
                            taxi_zone_name TEXT
);

CREATE TABLE DOLocation (
                            id SERIAL PRIMARY KEY,
                            taxi_zone_name TEXT
);

CREATE TABLE TripTpe (
                         id SERIAL PRIMARY KEY,
                         tpep_pickup_datetime TIMESTAMP,
                         tpep_dropoff_datetime TIMESTAMP,
                         passenger_count INT,
                         distance FLOAT,
                         tips FLOAT,
                         fare_amount FLOAT,
                         tolls_amount FLOAT,
                         total_amounts FLOAT,
                         airport_fee FLOAT,
                         Extra_charges TEXT,
                         rate_id INT,
                         fwd_flag TEXT,
                         congestion_surcharge FLOAT,
                         improvement_surcharge FLOAT,
                         mta_tax FLOAT,
                         FOREIGN KEY (rate_id) REFERENCES Rate (id)
);


CREATE TABLE Payment (
                         id SERIAL PRIMARY KEY ,
                         payment_type TEXT
);

CREATE TABLE Vendor (
                        id SERIAL PRIMARY KEY,
                        vendor_name TEXT
);

CREATE TABLE TaxiFact (
                          id SERIAL PRIMARY KEY,
                          vendor_id INT,
                          payment_id INT,
                          trip_id INT,
                          dolocation_id INT,
                          pulocation_id INT,
                          FOREIGN KEY (dolocation_id) REFERENCES DOLocation (id),
                          FOREIGN KEY (pulocation_id) REFERENCES PULocation (id),
                          FOREIGN KEY (vendor_id) REFERENCES Vendor (id),
                          FOREIGN KEY (payment_id) REFERENCES Payment (id),
                          FOREIGN KEY (trip_id) REFERENCES TripTpe (id)
);


INSERT INTO nyc_warehouse.public.rate (id,rate) VALUES
                                                    (0,'Standard rate'),
                                                    (1,'JFK'),
                                                    (2,'Newark'),
                                                    (3,'Nassau or Westchester'),
                                                    (4,'Negotiated fare'),
                                                    (5,'Group ride')
;

INSERT INTO nyc_warehouse.public.payment (payment_type) VALUES ('Credit card'),('Cash'),('No charge'),('Dispute'),('Unknown'),('Voided trip');

INSERT INTO nyc_warehouse.public.vendor (vendor_name) VALUES ('Creative Mobile Technologies, LLC'),(' VeriFone Inc.');


