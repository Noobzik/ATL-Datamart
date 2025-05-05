-- Vue : top_zones_view
CREATE OR REPLACE VIEW top_zones_view AS
SELECT dz.name AS pickup_zone, COUNT(*) AS total_trips
FROM fact_trips ft
JOIN dim_location dl ON ft.location_id = dl.location_id
JOIN dim_zone dz ON dl.pickup_zone_id = dz.zone_id
GROUP BY dz.name
ORDER BY total_trips DESC;

-- Vue : revenue_per_month_view
CREATE OR REPLACE VIEW revenue_per_month_view AS
SELECT 
    EXTRACT(YEAR FROM dt.pickup_date) AS year,
    EXTRACT(MONTH FROM dt.pickup_date) AS month,
    SUM(ft.total_amount) AS revenue
FROM fact_trips ft
JOIN dim_time dt ON ft.time_id = dt.time_id
GROUP BY year, month
ORDER BY year, month;
