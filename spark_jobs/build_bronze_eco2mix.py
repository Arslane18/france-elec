from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from energy_pipeline.global_utils import parse_args


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
