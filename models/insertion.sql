/** BDD DATAMART*/

-- Insertion des données dans les tables de dimensions depuis la base warehouse vers la base datamart

-- Insertion des données dans la table dim_datetime
INSERT INTO nyc_datamart.dim_datetime (tpep_pickup_datetime, tpep_dropoff_datetime)
SELECT DISTINCT tpep_pickup_datetime, tpep_dropoff_datetime
FROM nyc_warehouse.taxi_trips;

--TODO check vendor name
-- Insertion des données dans la table dim_vendor
INSERT INTO nyc_datamart.dim_vendor (name)
VALUES('Creative Mobile Technologies, LLC'), ('VeriFone Inc.'),;

-- Insertion des données dans la table dim_payment_type
INSERT INTO nyc_datamart.dim_payment_type (payment_type)
VALUES('Standard Rate'), ('JFK'), ('Newark'), ('Nasssau or Westchester'), ('Negociated fare'), ('Group ride');

-- Insertion des données dans la table dim_rate_code
INSERT INTO nyc_datamart.dim_rate_code (zone)
VALUES ('Credit card'), ('Cash'), ('No charge'), ('Dispute'), ('Unknown'), ('Group ride');
