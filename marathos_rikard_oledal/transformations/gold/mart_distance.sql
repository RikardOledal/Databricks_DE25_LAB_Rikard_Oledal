USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REPLACE VIEW marathos.gold.mart_distance AS
SELECT 
    e.event_name AS event,
    e.event_country_name AS event_country,
    r.result_distance_km AS event_distance,
    e.event_type,
    d.year AS event_year,
    d.month AS event_month,
    d.weekday AS event_weekday,
    d.date AS date,
    a.athlete_gender,
    a.athlete_country_name AS athlete_country,
    r.athlete_age,
    r.athlete_age_category,
    r.result_time_h AS athlete_time_h,
    r.result_speed_km_h AS athlete_speed_km_h
FROM fct_results r
JOIN dim_event e ON r.event_id = e.event_id
JOIN dim_athlete a ON r.athlete_id = a.athlete_id
JOIN dim_date d ON r.date_id = d.date_id
WHERE e.event_type = 'Distance';