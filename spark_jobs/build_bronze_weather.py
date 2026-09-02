import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from energy_pipeline.global_utils import parse_args, region_name_from_filename



def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)

    spark = SparkSession.builder.appName("openmeteo-bronze").getOrCreate()
    try:
        rows = []
        for path in sorted(raw_dir.glob("openmeteo-*.json")):
            region_name = region_name_from_filename(path)
            with open(path, "r") as file:
                hourly = json.load(file)["hourly"]
            for values in zip(*hourly.values()):
                row = dict(zip(hourly.keys(), values))
                row["region"] = region_name
                rows.append(row)

        df = (
            spark.createDataFrame(rows)
            .withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            .withColumn("year", F.year("time"))
            .withColumn("month", F.month("time"))
            .withColumn("day", F.dayofmonth("time"))
        )
        df.write.partitionBy(
            "region", "year", "month", "day"
        ).parquet(args.output_dir, mode="overwrite")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
