/**BDD DATAMART*/

-- Créer les tables de dimensions dans la base warehouse pour etre copier dans la base datamart par la suite

CREATE TABLE IF NOT EXISTS dim_service_zone (
    id SERIAL PRIMARY KEY ,
    service_zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_zone (
    id SERIAL PRIMARY KEY ,
    zone VARCHAR(50),
    service_zone_id INT,
    CONSTRAINT fk_service_zone_id FOREIGN KEY (service_zone_id) REFERENCES dim_service_zone(id)
);

CREATE TABLE IF NOT EXISTS dim_location (
    id SERIAL PRIMARY KEY ,
    borough VARCHAR(50),
    zone_id INT,
    CONSTRAINT fk_zone_id FOREIGN KEY (zone_id) REFERENCES dim_zone(id)
);
