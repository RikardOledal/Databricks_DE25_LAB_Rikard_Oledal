USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REPLACE VIEW marathos.gold.mart_time AS
SELECT 
    e.event_name,
    e.event_country_name,
    r.result_time_h AS time,
    e.event_type,
    d.year,
    d.date AS date,
    a.athlete_gender,
    a.athlete_country_name,
    r.athlete_age,
    r.athlete_age_category,
    r.result_distance_km,
    r.result_speed_km_h
FROM fct_results r
JOIN dim_event e ON r.event_id = e.event_id
JOIN dim_athlete a ON r.athlete_id = a.athlete_id
JOIN dim_date d ON r.date_id = d.date_id
WHERE e.event_type = 'Time';