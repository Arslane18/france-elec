import argparse
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eco2mix-path", required=True)
    parser.add_argument("--openmeteo-path", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def read_eco2mix(spark, path: str, mode: str):


def read_openmeteo(spark, path: str, mode: str):



def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("silver_layer").getOrCreate()

    conso_df = read_eco2mix(spark, args.conso_path, args.mode)
    meteo_df = read_openmeteo(spark, args.meteo_path, args.mode)


if __name__ == "__main__":
    main()