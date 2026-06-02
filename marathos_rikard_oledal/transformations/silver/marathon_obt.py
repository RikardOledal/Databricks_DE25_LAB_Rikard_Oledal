from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case, transform_event_dates, extract_event_name_details, enrich_with_countries
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
    df = enrich_with_countries(df, dim_countries)
    return df