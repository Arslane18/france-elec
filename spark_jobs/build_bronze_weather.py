import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=True, help="Directory with openmeteo-<region>-<year>.json files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the partitioned parquet dataset")
    return parser.parse_args()


def region_name_from_filename(path: Path) -> str:
    # openmeteo-<region>-<year>.json ; region can itself contain - in its name, year is always 4 digits straight.
    stem = path.stem.removeprefix("openmeteo-")
    region_name, _, _year = stem.rpartition("-")
    return region_name


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
