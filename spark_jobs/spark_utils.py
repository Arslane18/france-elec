import argparse
from pyspark.sql import functions as F


def parse_silver_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eco2mix-path", required=True)
    parser.add_argument("--openmeteo-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", required=False)
    parser.add_argument("--holiday-path", required=False)
    parser.add_argument("--staging-dir", required=False, help="Flat, non-partitioned copy for the Snowflake MERGE, coalesced into fewer files")
    return parser.parse_args()

def clean_eco2mix(df):
    df = df.withColumn("region_code", F.col("code_insee_region").cast("int"))
    return agg_temporelle(df, ["region_code"], "consommation")

def clean_meteo(df):
    df = df.withColumnRenamed("time", "date_heure")
    # Backup so duplicates rows are not carried further.
    return (
        df
        .groupBy("region_code", "date_heure")
        .agg(
            F.avg("temperature_2m").alias("temperature_2m"),
            F.avg("precipitation").alias("precipitation"),
        )
    )

def clean_holiday(df):
    # Kept at day grain ("date", not renamed to "date_heure"): a holiday applies to every
    # hour of that day, so the caller joins it against date_heure truncated to a date.
    return df.drop("jour_ferie")


def agg_temporelle(
    df,
    group_cols: list[str],
    col_to_agg: str,
    date_col: str = "date_heure",
    granularity: str = "hour",
    agg_func=F.avg,
    alias: str | None = None,
):
    alias = alias or col_to_agg
    return (
        df
        .withColumn(date_col, F.date_trunc(granularity, F.col(date_col)))
        .groupBy(*group_cols, date_col)
        .agg(agg_func(col_to_agg).alias(alias))
    )