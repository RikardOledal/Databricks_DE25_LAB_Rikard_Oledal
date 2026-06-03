CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
COMMENT 'Dim athlete deduplicated - gold layer' 
AS
SELECT DISTINCT
    athlete_id,
    athlete_gender,
    athlete_country_name,
    athlete_year_of_birth AS athlete_birth_year
FROM
    marathos.silver.marathon_obt;