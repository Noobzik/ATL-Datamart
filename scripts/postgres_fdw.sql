CREATE SERVER dw_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5432', dbname 'data_warehouse');

CREATE USER MAPPING FOR postgres
  SERVER dw_server
  OPTIONS (user 'postgres', password 'admin');
