import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from energy_pipeline.global_utils import parse_bronze_args, region_code_from_filename
from energy_pipeline.config import WEATHER_NAME, DAILY_MARKER



def main() -> None:
    args = parse_bronze_args()
    raw_dir = Path(args.raw_dir)

    spark = SparkSession.builder.appName("openmeteo-bronze").getOrCreate()
    try:
        rows = []
        # Exclude the daily DAG's own raw file (openmeteo-<region>-daily.json) because it leads to duplicates.
        for path in sorted(raw_dir.glob(f"{WEATHER_NAME}-*.json")):
            if path.stem.endswith(f"-{DAILY_MARKER}"):
                continue
            region_code = region_code_from_filename(path)
            with open(path, "r") as file:
                hourly = json.load(file)["hourly"]
            for values in zip(*hourly.values()):
                row = dict(zip(hourly.keys(), values))
                row["region_code"] = region_code
                rows.append(row)

        df = (
            spark.createDataFrame(rows)
            .withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            .withColumn("year", F.year("time"))
            .withColumn("month", F.month("time"))
            .withColumn("day", F.dayofmonth("time"))
        )
        df.write.partitionBy(
            "region_code", "year", "month", "day"
        ).parquet(args.output_dir, mode="overwrite")

        if args.staging_dir:
            df.coalesce(8).write.parquet(
                args.staging_dir, mode="overwrite"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
