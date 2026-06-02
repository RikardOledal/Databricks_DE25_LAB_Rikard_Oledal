import re 
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, concat, regexp_extract, lit, col, regexp_replace, to_date, broadcast

def transform_event_dates(df: DataFrame, date_column: str) -> DataFrame:
    """
    Cleans up Event-dates and creates event_start_date and event_end_date.
    In case day or month is 00 it is going to be replaced by 01 and column
    "is_estimated_date" is going to be True.
    """
    return (
        df
        # Cleaning of start dates
        .withColumn(
            "start_date_clean",
            when(
                # FORMAT 2: dd.-dd.MM.YYYY (Day2Day)
                col(date_column).rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}$"),
                concat(
                    regexp_extract(col(date_column), r"^(\d{2})\.-", 1),
                    lit("."),
                    regexp_extract(col(date_column), r"\.(\d{2}\.\d{4})$", 1)
                )
            )
            .when(
                # FORMAT 3: dd.MM-dd.MM.YYYY or dd.MM.-dd.MM.YYYY (Month2Month)
                col(date_column).rlike(r"^\d{2}\.\d{2}\.?-\d{2}\.\d{2}\.\d{4}$"),
                concat(
                    regexp_extract(col(date_column), r"^(\d{2}\.\d{2})\.?-", 1),
                    lit("."),
                    regexp_extract(col(date_column), r"(\d{4})$", 1)
                )
            )
            .when(
                # FORMAT 4 :dd.MM.YYYY-dd.MM.YYYY (Year2year)
                col(date_column).rlike(r"^\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}$"),
                regexp_extract(col(date_column), r"^(\d{2}\.\d{2}\.\d{4})-", 1)
            )
            .otherwise(
                # FORMAT 1: dd.MM.YYYY (Correct format)
                col(date_column)
            )
        )
        
        # Cleaning of end dates
        .withColumn(
            "end_date_clean",
            when(
                # Takes everyting after -
                col(date_column).contains("-"),
                regexp_extract(col(date_column), r"(\d{2}\.\d{2}\.\d{4})$", 1)
            )
            .otherwise(
                # Annars är det ett endagslopp -> Slutdatum är samma som startdatum
                col(date_column)
            )
        )
        
        # If day or month is 00 we change it to 01 but we mark it with True i this column
        .withColumn(
            "is_estimated_date",
            when(
                col("start_date_clean").contains("00.") | col("end_date_clean").contains("00."), 
                True
            ).otherwise(False)
        )
        
        # Replace 00. with 01.
        .withColumn("start_date_clean", regexp_replace(col("start_date_clean"), r"00\.", "01."))
        .withColumn("end_date_clean", regexp_replace(col("end_date_clean"), r"00\.", "01."))
        
        # Convert to datevalue
        .withColumn("event_start_date", to_date(col("start_date_clean"), "dd.MM.yyyy"))
        .withColumn("event_end_date", to_date(col("end_date_clean"), "dd.MM.yyyy"))
        
        # Remove cleaning columns
        .drop("start_date_clean", "end_date_clean")
    )



def extract_event_name_details(df: DataFrame, event_col: str = "event_name") -> DataFrame:
    """
    Extracts the event name and country code from the original string.
    Also cleans up excess quotes.
    """
    return (
        df
        .withColumn("event_country_code", regexp_extract(col(event_col), r"\(([A-Z]{3})\)[\s\"']*$", 1))
        .withColumn("event_name_cleaned", regexp_extract(col(event_col), r"^[\s\"']*(.*?)\s*\([A-Z]{3}\)[\s\"']*$", 1))
        .withColumn(
            "event_name_cleaned", 
            when(
                col("event_name_cleaned") == "", 
                regexp_replace(col(event_col), r"^[\s\"']+|[\s\"']+$", "")
            ).otherwise(col("event_name_cleaned"))
        )
        .withColumn(
            "event_name_cleaned",
            regexp_replace(col("event_name_cleaned"), '""', '"')
        )
    )

def enrich_with_countries(df_events: DataFrame, df_dim_countries: DataFrame) -> DataFrame:
    """
    Turns on the dimension table for countries and handles known corner cases
    (e.g. Wings for Life World Run).
    """
    df_dim_ready = df_dim_countries.withColumnRenamed("country_name", "country_name_full")

    return (
        df_events.alias("events")
        .join(
            broadcast(df_dim_ready).alias("countries"),
            col("events.event_country_code") == col("countries.iso_code"),
            "left"
        )
        # Hantera globala lopp utan specifikt land
        .withColumn(
            "country_name_full",
            when(col("event_name").contains("Wings for Life"), "Global")
            .otherwise(col("country_name_full"))
        )
        .withColumn(
            "event_country_code",
            when(col("event_name").contains("Wings for Life"), "GLO")
            .otherwise(col("event_country_code"))
        )
        # Droppa join-nyckeln så vi håller pipelinen ren
        .drop("iso_code")
    )

def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)