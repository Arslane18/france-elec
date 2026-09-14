import argparse
from pathlib import Path


def parse_bronze_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=False, help="Directory with openmeteo-<region>-<year>.json files")
    parser.add_argument("--raw-glob", required=False, help="Glob pattern matching the raw eco2mix parquet files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the partitioned parquet dataset")
    parser.add_argument("--staging-dir", required=False, help="Flat, non-partitioned copy for the Snowflake load, coalesced into fewer files")
    return parser.parse_args()


def region_code_from_filename(path: Path) -> str:
    '''
    Retrieve region code from filename as it is like : openmeteo-<region_code>-<year>.json
    '''
    stem = path.stem.removeprefix("openmeteo-")
    region_code, _, _year = stem.rpartition("-")
    return region_code