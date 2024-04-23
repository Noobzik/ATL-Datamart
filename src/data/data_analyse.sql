/* analyser la répartition des voyages en fonction des jours de la semaine */
SELECT p.payment_type, COUNT(tf.id) AS total_trips
FROM TaxiFact tf
         JOIN Payment p ON tf.payment_id = p.id
GROUP BY p.payment_type;

/* analyser la répartition des voyages en fonction des zones de ramassage (PULocation) et des zones de dépose (DOLocation) */
SELECT pu.taxi_zone_name AS pickup_location, "do".taxi_zone_name AS dropoff_location, COUNT(tf.id) AS total_trips FROM TaxiFact tf JOIN PULocation pu ON tf.pulocation_id = pu.id JOIN DOLocation "do" ON tf.dolocation_id = "do".id GROUP BY pu.taxi_zone_name, "do".taxi_zone_name ORDER BY total_trips DESC;

/*tendances de prix des trajets en fonction du jour de la semaine : */

SELECT
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    AVG(fare_amount) AS average_fare
FROM
    TripTpe
GROUP BY
    day_of_week
ORDER BY
    day_of_week;

/* distance parcourue et du montant total payé */
SELECT
    CASE
        WHEN distance < 5 THEN 'Short distance (< 5 miles)'
        WHEN distance >= 5 AND distance < 10 THEN 'Medium distance (5-10 miles)'
        ELSE 'Long distance (>= 10 miles)'
        END AS distance_category,
    AVG(total_amounts) AS average_total_amount
FROM
    TripTpe
GROUP BY
    CASE
        WHEN distance < 5 THEN 'Short distance (< 5 miles)'
        WHEN distance >= 5 AND distance < 10 THEN 'Medium distance (5-10 miles)'
        ELSE 'Long distance (>= 10 miles)'
        END, distance
ORDER BY
    CASE
        WHEN distance < 5 THEN 1
        WHEN distance >= 5 AND distance < 10 THEN 2
        ELSE 3
        END;

/* analyser les variations de pourboires en fonction du jour de la semaine */

SELECT
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    AVG(tips) AS average_tip_amount
FROM
    TripTpe
GROUP BY
    day_of_week
ORDER BY
    day_of_week;


/*  la distance pourrait être d'analyser la distribution des distances parcourues dans vos données. Vous pouvez le faire en regroupant les trajets par intervalles de distance et en comptant le nombre de trajets dans chaque intervalle*/
SELECT
    CASE
        WHEN distance < 5 THEN '0-5 miles'
        WHEN distance >= 5 AND distance < 10 THEN '5-10 miles'
        WHEN distance >= 10 AND distance < 15 THEN '10-15 miles'
        ELSE '15+ miles'
        END AS distance_range,
    COUNT(*) AS num_trips
FROM
    TripTpe
GROUP BY
    CASE
        WHEN distance < 5 THEN '0-5 miles'
        WHEN distance >= 5 AND distance < 10 THEN '5-10 miles'
        WHEN distance >= 10 AND distance < 15 THEN '10-15 miles'
        ELSE '15+ miles'
        END
ORDER BY
    distance_range;


SELECT
    distance
FROM
    TripTpe limit 1009010;

