docker-compose up | docker compose up
pip install -r requirements.txt
python src/data/grab_parquet.py
docker exec -it atl-datamart_data-warehouse_1 psql -U admin
admin=# CREATE DATABASE nyc_warehouse;
docker exec -it atl-datamart_data-mart_1 psql -U admin
admin=# CREATE DATABASE nyc_datamart;
admin=# CREATE EXTENSION IF NOT EXISTS postgres_fdw;
admin=# CREATE SERVER source_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'atl-datamart_data-warehouse_1', dbname 'nyc_warehouse', port '5432');
admin=# CREATE USER MAPPING FOR admin SERVER source_server OPTIONS (user 'admin', password 'admin');
admin=# CREATE FOREIGN TABLE foreign_table ( vendorid int4 NULL, tpep_pickup_datetime timestamp NULL, tpep_dropoff_datetime timestamp NULL, passenger_count float8 NULL, trip_distance float8 NULL, ratecodeid float8 NULL, store_and_fwd_flag text NULL, pulocationid int4 NULL, dolocationid int4 NULL, payment_type int8 NULL, fare_amount float8 NULL, extra float8 NULL, mta_tax float8 NULL, tip_amount float8 NULL, tolls_amount float8 NULL, improvement_surcharge float8 NULL, total_amount float8 NULL, congestion_surcharge float8 NULL, airport_fee float8 NULL ) SERVER source_server OPTIONS (schema_name 'public', table_name 'nyc_raw');
python src/data/dump_to_sql.py