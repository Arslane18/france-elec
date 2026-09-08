from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_utils import parse_args, clean_meteo, clean_eco2mix, clean_holiday

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

    result_df = clean_eco2mix_df.join(clean_meteo_df, on=["date_heure", "region_code"], how="left")

    if args.holiday_path:
        holiday_df = spark.read.parquet(args.holiday_path)
        if args.start_date:
            holiday_df = holiday_df.filter(F.col("date") >= args.start_date)
        clean_holiday_df = clean_holiday(holiday_df)
        result_df = (
            result_df
            .withColumn("date", F.to_date("date_heure"))
            .join(clean_holiday_df, on=["date", "region_code"], how="left")
            .drop("date")
        )
    else:
        result_df = result_df.withColumn("is_holiday", F.lit(0))

    result_df = result_df.fillna({"is_holiday": 0})
    result_df = result_df.withColumn("year", F.year("date_heure"))

    result_df.write.partitionBy(
                "region_code", "year"
            ).parquet(args.output_dir, mode="overwrite")

    if args.staging_dir:
        # Same rows, flattened (no partitionBy) and coalesced into few, larger files, used
        # only for the Snowflake MERGE. Hive partition columns (region_code/year) are
        # stripped from the actual parquet file content by the partitioned writer, and PUT
        # flattens the stage folder away, so loading straight from output_dir would drop
        # REGION_CODE (NOT NULL, part of the merge key) -- same issue as bronze/openmeteo.
        # result_df is already scoped to just what this run processed (the full history for
        # a backfill, a rolling window for daily_data_transformation), so no extra pruning
        # is needed here.
        result_df.coalesce(8).write.parquet(args.staging_dir, mode="overwrite")

if __name__ == "__main__":
    main()