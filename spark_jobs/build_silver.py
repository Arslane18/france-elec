import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eco2mix-path", required=True)
    parser.add_argument("--openmeteo-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", required=False)
    return parser.parse_args()

def clean_eco2mix(df):
    df = df.withColumn("region_code", F.col("code_insee_region").cast("int"))
    return agg_temporelle(df, ["region_code"], "consommation")

def clean_meteo(df):
    df = df.withColumnRenamed("time", "date_heure")
    return df.drop("region")

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

def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("silver_layer").config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()
    

    eco2mix_df = spark.read.parquet(args.eco2mix_path)
    meteo_df = spark.read.parquet(args.openmeteo_path)

    if args.start_date:
        eco2mix_df = eco2mix_df.filter(F.col("date_heure") >= args.start_date)
        meteo_df = meteo_df.filter(F.col("time") >= args.start_date)

    clean_eco2mix_df = clean_eco2mix(eco2mix_df)
    clean_meteo_df = clean_meteo(meteo_df)
    join_df = clean_eco2mix_df.join(clean_meteo_df, on = ["date_heure", "region_code"], how="left")

    join_df = (
        join_df
        .withColumn("year", F.year("date_heure"))
    )

    join_df.write.partitionBy(
                "region_code", "year"
            ).parquet(args.output_dir, mode="overwrite")
    
if __name__ == "__main__":
    main()