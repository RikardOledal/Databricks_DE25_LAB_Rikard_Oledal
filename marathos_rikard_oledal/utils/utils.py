import re 
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, concat, regexp_extract, lit, col, regexp_replace, to_date, broadcast, round, coalesce, trim, lower, split, nullif, upper, substring, md5, concat_ws, sha2

def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)

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

def append_country_name(df: DataFrame, df_dim_countries: DataFrame, code_col: str, new_name_col: str) -> DataFrame:
    """
    General function to translate a 3-letter code into a full country name.
    """
    df_dim_ready = df_dim_countries.withColumnRenamed("country_name", new_name_col)

    return (
        df
        .withColumn(code_col, upper(col(code_col)))
        
        .join(
            broadcast(df_dim_ready),
            col(code_col) == col("iso_code"),
            "left"
        )

        .withColumn(
            new_name_col,
            when(col(code_col) == "XXX", "Unknown")           # Hantera kända XXX-koder
            .when(col(new_name_col).isNull(), "Unknown")
            .otherwise(col(new_name_col))
        )
        # Drop the join key
        .drop("iso_code")
    )

def handle_global_events(df: DataFrame, name_col: str = "event_country_name", code_col: str = "event_country_code") -> DataFrame:
    """
    Handles special cases for races that lack a specific country (e.g. Wings for Life).
    Does NOT affect the athletes countries.
    """
    return (
        df.withColumn(
            name_col,
            when(col("event_name").contains("Wings for Life"), "Global")
            .otherwise(col(name_col))
        )
        .withColumn(
            code_col,
            when(col("event_name").contains("Wings for Life"), "GLO")
            .otherwise(col(code_col))
        )
    )

def calculate_performance_metrics(df: DataFrame, dist_col: str = "event_distance_length", perf_col: str = "athlete_performance", event_col: str = "event_name") -> DataFrame:
    """
    Standardizes distance and time.
    Combines athlete performance with event distance length to get speed_km_h, distance_km and duration_h. It also gives event_type to clarify whether it is a distance event, time event or endurance event.
    """
    return (
        df
        # Checks for fotrally
        .withColumn("is_fotrally", col(event_col).rlike("(?i)fotrally|maratonmarschen"))

        # Checks data to evalute marathon
        .withColumn("ev_val_str", nullif(regexp_extract(col(dist_col), r"^([\d\.:]+)", 1), lit("")))
        .withColumn("ev_val", 
                    when(col("ev_val_str").contains(":"), 
                         split(col("ev_val_str"), ":").getItem(0).cast("float") + 
                         (split(col("ev_val_str"), ":").getItem(1).cast("float") / 60)
                    ).otherwise(col("ev_val_str").cast("float")))
        .withColumn("ev_unit", lower(regexp_extract(col(dist_col), r"^[\d\.:]+\s*([a-zA-Z]+)", 1)))
        .withColumn("event_type", 
                    when(col("is_fotrally"), "Endurance")
                    .when(col("ev_unit").isin("h", "d"), "Time")
                    .otherwise("Distance"))
        .withColumn("ev_dist_km", 
                    when(col("ev_unit").isin("km", "k"), col("ev_val"))
                    .when(col("ev_unit").isin("mi", "mile", "miles"), col("ev_val") * 1.60934))
        .withColumn("ev_time_h", 
                    when(col("ev_unit") == "h", col("ev_val"))
                    .when(col("ev_unit") == "d", col("ev_val") * 24))

        # Interpreting athlete performance depending on race type
        # Clean up incorrect units (like "km" at the end of a time) to not fool regex
        .withColumn("perf_clean", trim(regexp_replace(col(perf_col), r"[a-zA-Z\s]+$", "")))

        # Distance calculation
        .withColumn("perf_dist_val", nullif(regexp_extract(col("perf_clean"), r"^([\d\.]+)", 1), lit("")).cast("float"))
        .withColumn("perf_dist_km", 
                    when(col("event_type") == "Time", 
                         when(lower(col(perf_col)).contains("mi"), col("perf_dist_val") * 1.60934)
                         .otherwise(col("perf_dist_val"))))
        
        # Time calculation
        .withColumn("perf_d", nullif(regexp_extract(col("perf_clean"), r"(\d+)d", 1), lit("")).cast("float"))
        .withColumn("perf_h", nullif(regexp_extract(col("perf_clean"), r"(?:^|\s)(\d+):", 1), lit("")).cast("float"))
        .withColumn("perf_m", nullif(regexp_extract(col("perf_clean"), r":(\d{2}):", 1), lit("")).cast("float"))
        .withColumn("perf_s", nullif(regexp_extract(col("perf_clean"), r":(\d{2})$", 1), lit("")).cast("float"))
        
        # We calculate perf_time_h for ALL time formats, and force it to run against Fotrally
        .withColumn("perf_time_h", 
                    when((col("event_type") == "Distance") | col("is_fotrally"), 
                         (coalesce(col("perf_d"), lit(0)) * 24) + 
                         coalesce(col("perf_h"), lit(0)) + 
                         (coalesce(col("perf_m"), lit(0)) / 60) + 
                         (coalesce(col("perf_s"), lit(0)) / 3600)))
        # Set to NULL if the time was exactly 0
        .withColumn("perf_time_h", when(col("perf_time_h") > 0, col("perf_time_h")).otherwise(None))

        # Combine the results with fotrally
        .withColumn("duration_h", round(
            when(col("is_fotrally"), col("perf_time_h")) # Om Fotrally: Hämta atletens tid!
            .otherwise(coalesce(col("ev_time_h"), col("perf_time_h"))), 3))
        
        .withColumn("distance_km", round(
            when(col("is_fotrally"), col("duration_h") * 5.0) # Om Fotrally: Tid * 5 km/h
            .otherwise(coalesce(col("ev_dist_km"), col("perf_dist_km"))), 3))
        
        
        # Calculate speed
        .withColumn("speed_km_h", 
                    when(col("is_fotrally"), lit(5.0)) # Om Fotrally: Alltid 5 km/h
                    .otherwise(when(col("duration_h") > 0, round(col("distance_km") / col("duration_h"), 2)).otherwise(None)))

        # clean up all help columns
        .drop("is_fotrally", "ev_val_str", "ev_val", "ev_unit", "ev_dist_km", "ev_time_h", 
              "perf_clean", "perf_dist_val", "perf_dist_km", "perf_d", "perf_h", "perf_m", "perf_s", "perf_time_h", dist_col, perf_col)
    )

def clean_gender(df: DataFrame) -> DataFrame:
    """
    Standardizes the athlete's gender (M/F/X/U).
    """
    return (
        df
        .withColumn("gender_clean", trim(upper(col("athlete_gender"))))
        .withColumn("gender_clean", substring(col("gender_clean"), 1, 1))
        .withColumn(
            "athlete_gender",
            when(col("gender_clean").isin("M", "F", "X"), col("gender_clean"))
            .otherwise(lit("U"))
        )
        .drop("gender_clean")
    )

def clean_age_category(df: DataFrame) -> DataFrame:
    """
    Standardizes the athlete's age group and changes F to W in the women's class
    """
    return (
        df
        .withColumn(
            "athlete_age_category", 
            coalesce(trim(upper(col("athlete_age_category"))), lit("Unknown"))
        )
        .withColumn(
            "athlete_age_category",
            regexp_replace(col("athlete_age_category"), r"^F", "W")
        )
    )

def create_unique_athlete_id(df: DataFrame) -> DataFrame:
    """
    Creates a guaranteed unique athlete ID based on a combination of 
    their original ID, gender, country, and birth year.
    Solves the issue of recycled or merged IDs in historical data.
    """
    return (
        df
        .withColumn(
            "athlete_id",
            sha2(concat_ws("_", 
                col("athlete_id").cast("string"),
                coalesce(col("athlete_gender"), lit("UNKNOWN")),
                coalesce(col("athlete_country_name"), lit("UNKNOWN")),
                coalesce(col("athlete_year_of_birth").cast("string"), lit("UNKNOWN"))
            ), 256)
        )
    )