CREATE TABLE dim_vendor(
   vendorid INT,
   vendor_name VARCHAR(50),
   contact_info VARCHAR(50),
   PRIMARY KEY(vendorid)
);

CREATE TABLE dim_ratecode(
   ratecodeid INT,
   ratecode_description TEXT,
   is_flat_rate BOOLEAN,
   PRIMARY KEY(ratecodeid)
);

CREATE TABLE dim_payment(
   payment_type INT,
   is_electronic BOOLEAN,
   payment_description TEXT,
   PRIMARY KEY(payment_type)
);

CREATE TABLE dim_location(
   locationid INT,
   borough VARCHAR(50),
   service_zone VARCHAR(100),
   zone VARCHAR(100),
   PRIMARY KEY(locationid)
);

CREATE TABLE fact_trips(
   Id_fact_trips SERIAL,
   passenger_count INT,
   trip_distance DECIMAL(15,2),
   store_and_fwd_flag TEXT,
   pulocationid INT,
   dolocationid INT,
   fare_amount DECIMAL(15,2),
   extra DECIMAL(15,2),
   mta_tax DECIMAL(15,2),
   tip_amount DECIMAL(15,2),
   tolls_amount DECIMAL(15,2),
   total_amount DECIMAL(15,2),
   congestion_surcharge DECIMAL(15,2),
   airport_fee DECIMAL(15,2),
   improvement_surcharge DECIMAL(15,2),
   locationid INT,
   locationid_1 INT,
   payment_type INT,
   ratecodeid INT,
   vendorid INT,
   PRIMARY KEY(Id_fact_trips),
   FOREIGN KEY(locationid) REFERENCES dim_location(locationid),
   FOREIGN KEY(locationid_1) REFERENCES dim_location(locationid),
   FOREIGN KEY(payment_type) REFERENCES dim_payment(payment_type),
   FOREIGN KEY(ratecodeid) REFERENCES dim_ratecode(ratecodeid),
   FOREIGN KEY(vendorid) REFERENCES dim_vendor(vendorid)
);
