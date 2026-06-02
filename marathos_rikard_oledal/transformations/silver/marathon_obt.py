from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case, transform_event_dates, extract_event_name_details, calculate_performance_metrics, append_country_name, handle_global_events
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round


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

    df_clean = df.dropna(subset=["distance_km", "speed_km_h", "duration_h"])
    return df_clean