CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
COMMENT 'Dim date table - gold layer' 

AS

SELECT DISTINCT
    CAST(date_format(event_start_date, 'yyyyMMdd') AS BIGINT) AS date_id,
    event_start_date AS date,
    YEAR(event_start_date) AS year,
    MONTH(event_start_date) AS month,
    DATE_FORMAT(event_start_date, 'E') AS weekday
FROM
    marathos.silver.marathon_obt
WHERE 
    event_start_date IS NOT NULL;