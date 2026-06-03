CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
COMMENT 'Dim event deduplicated - gold layer' 
AS
SELECT DISTINCT
    event_id,
    event_name_cleaned AS event_name,
    event_type,
    year_of_event,
    event_start_date,
    event_end_date,
    event_country_code,
    event_country_name
FROM
    marathos.silver.marathon_obt;