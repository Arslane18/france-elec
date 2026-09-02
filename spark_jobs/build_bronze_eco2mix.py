from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from energy_pipeline.global_utils import parse_args

def main() -> None:
    args = parse_args()

    # Pinned to UTC: so date based column all have the same schema
    spark = (
        SparkSession.builder
        .appName("eco2mix-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        df = spark.read.parquet(args.raw_glob)
        # Partitioned by eco2mix's own local calendar day (the `date` field), not
        # by date_heure's UTC day, so this matches the daily Polars job (which
        # partitions the same way) instead of disagreeing with it for the ~2h/day
        # where the UTC day and the French local day differ.
        parsed_date = F.to_date("date", "yyyy-MM-dd")
        df_with_parts = (
            df
            .withColumn("year", F.year(parsed_date))
            .withColumn("month", F.month(parsed_date))
            .withColumn("day", F.dayofmonth(parsed_date))
        )
        df_with_parts.write.partitionBy(
            "year", "month", "day"
        ).parquet(args.output_dir, mode="overwrite")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
