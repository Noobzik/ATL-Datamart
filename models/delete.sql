--Suppresion de la dimension lcoation et de ses tables associées
DELETE FROM dim_location;
-- Avant de supprimer les enregistrements dans dim_zone, vous devez d'abord supprimer les enregistrements dans dim_location qui y font référence
DELETE FROM dim_location;

-- Ensuite, vous pouvez supprimer les enregistrements dans dim_zone
DELETE FROM dim_zone;
-- Avant de supprimer les enregistrements dans dim_service_zone, vous devez d'abord supprimer les enregistrements dans dim_zone qui y font référence
DELETE FROM dim_zone;

-- Ensuite, vous pouvez supprimer les enregistrements dans dim_service_zone
DELETE FROM dim_service_zone;
