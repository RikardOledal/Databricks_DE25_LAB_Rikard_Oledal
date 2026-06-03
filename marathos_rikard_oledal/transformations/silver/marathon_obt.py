from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case, transform_event_dates, extract_event_name_details, calculate_performance_metrics, append_country_name, handle_global_events, clean_gender, clean_age_category, create_unique_athlete_id
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round, sha2, concat_ws, expr

@dp.table(
    name="marathos.silver.marathon_obt",
    comment="Cleaned marathos data for 1798 to 2022",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5", 
    },
)
def clean_marathon_data():
    dim_countries = spark.table("marathos.bronze.dim_countries")
    df = spark.sql("FROM STREAM marathos.bronze.raw_marathon")
    df = rename_columns_to_snake_case(df)
    df = transform_event_dates(df, "event_dates")
    df = extract_event_name_details(df)
    df = append_country_name(df, dim_countries, code_col="event_country_code", new_name_col="event_country_name")
    df = handle_global_events(df, name_col="event_country_name", code_col="event_country_code")
    df = append_country_name(df, dim_countries, code_col="athlete_country", new_name_col="athlete_country_name")
    df = calculate_performance_metrics(df)
    df = clean_gender(df)
    df = clean_age_category(df)
    df = create_unique_athlete_id(df)
    df = df.withColumn("athlete_age", col("year_of_event") - col("athlete_year_of_birth"))

    # REMOVE RUNNERS WHO
    # - Have null in distance or speed_km_h or duration_h
    # - Are under 15 or over 100
    # - Runs faster than speed limit 21.19 km/h (World Record)
    df_clean = (
        df
        .dropna(subset=["distance_km", "speed_km_h", "duration_h"])
        .filter((col("athlete_age").between(15, 100))|col("athlete_age").isNull())
        .filter(col("speed_km_h") <= 21.19)
        )
    
    # Add event_id and result_id with SHA-256
    df_clean = (
        df_clean
        .withColumn(
            "event_id", 
            sha2(concat_ws("_", col("event_name"), col("year_of_event").cast("string")), 256)
        )
        .withColumn(
            "result_id", 
            expr("uuid()")
        )
    )

    return df_clean