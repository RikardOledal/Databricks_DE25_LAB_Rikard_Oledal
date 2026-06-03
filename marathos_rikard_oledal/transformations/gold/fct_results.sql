CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
    COMMENT "Fact table - gold layer" AS
SELECT
    result_id,
    event_id,
    athlete_id,
    athlete_age,
    athlete_age_category,
    duration_h AS result_time_h,
    distance_km AS result_distance_km,
    speed_km_h AS result_speed_km_h,
    date_format(event_start_date, 'yyyyMMdd')::bigint AS date_id
FROM STREAM marathos.silver.marathon_obt;