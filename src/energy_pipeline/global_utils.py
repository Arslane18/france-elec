import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=False, help="Directory with openmeteo-<region>-<year>.json files")
    parser.add_argument("--raw-glob", required=False, help="Glob pattern matching the raw eco2mix parquet files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the partitioned parquet dataset")
    return parser.parse_args()


def region_name_from_filename(path: Path) -> str:
    # openmeteo-<region>-<year>.json ; region can itself contain - in its name, year is always 4 digits straight.
    stem = path.stem.removeprefix("openmeteo-")
    region_name, _, _year = stem.rpartition("-")
    return region_name