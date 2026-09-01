import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-glob", required=True, help="Glob pattern matching the raw eco2mix parquet files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the partitioned parquet dataset")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("eco2mix-bronze").getOrCreate()
    try:
        df = spark.read.parquet(args.raw_glob)
        df_with_parts = (
            df
            .withColumn("year", F.year("date_heure"))
            .withColumn("month", F.month("date_heure"))
            .withColumn("day", F.dayofmonth("date_heure"))
        )
        df_with_parts.write.partitionBy(
            "year", "month", "day"
        ).parquet(args.output_dir, mode="overwrite")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
