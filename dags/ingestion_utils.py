import requests
import polars as pl

from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timedelta
from airflow.sdk import task
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.sdk.exceptions import AirflowFailException

from energy_pipeline.config import BASE_URL, RAW_DIR, ECO2MIX_NAME, WEATHER_NAME, DAILY_MARKER


session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch_data_from_api(url: str, params: Dict[str,Any]):
    '''Simply fetch data from a given url'''
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp

@task
def compute_years() -> list[int]:
    boundaries = retrieve_boundaries_years()
    return list(range(boundaries["start_year"] + 1, boundaries["end_year"] + 1)) # We add 1 to start_year cause start_year of eco2mix is an empty year.

def retrieve_boundaries_years() -> Dict[str, int]:
    '''
    Retrieve boundaries years and send them as dict. Keys are start_year and end_year.'''
    params = {"select": "Min(year(date_heure)) as start_year, Max(year(date_heure)) as end_year"}
    fetched_result = fetch_data_from_api(url=BASE_URL + "/records", params=params).json()
    return fetched_result["results"][0]

def fetch_and_store(url: str, params: dict, path: str) -> None:
    resp = fetch_data_from_api(url=url, params=params)
    write_bytes_to_file(resp.content, path)

def write_bytes_to_file(content: bytes, path: str) -> None:
    with open(path, "wb") as f:
        f.write(content)

def raw_weather_path(region_code: int, year: int | str) -> str:
    return f"{RAW_DIR}/{WEATHER_NAME}-{region_code}-{year}.json"

def raw_eco2mix_path(year: int) -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}-{year}.parquet"

def raw_eco2mix_glob() -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}*.parquet"

def raw_eco2mix_daily_path() -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}.parquet"

def raw_weather_daily_path(region_code: int) -> str:
    return raw_weather_path(region_code, DAILY_MARKER)

def raw_weather_daily_glob_pattern() -> str:
    return f"{WEATHER_NAME}-*-{DAILY_MARKER}.json"

def year_date_range(year: int) -> tuple[str, str]:
    '''Full calendar year, capped at today for the current year.'''
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    if year == datetime.today().year:
        end_date = datetime.today().strftime("%Y-%m-%d")
    return start_date, end_date

def write_bronze_partitioned(df: pl.DataFrame, partition_date: pl.Expr, path: str, region_partitioned: bool = False, staging_path: str | None = None) -> list[str]:
    '''Derive year/month/day hive partitions from partition_date and write, replacing any existing data in the touched partitions.

    If staging_path is given, also writes a flat (non-Hive) copy of the same rows there
    and returns that file's path instead of the touched partition directories. Hive
    partition columns (region_code/year/month/day) are stripped from the actual parquet
    file content by pyarrow's partitioned writer -- they only live in the directory
    names -- and PUT to a Snowflake stage flattens that directory structure away, so
    loading straight from the touched partition dirs silently drops those columns. The
    flat staging copy keeps them as real columns so the Snowflake load doesn't lose them.

    Otherwise, returns the touched partition directory paths (e.g.
    ".../year=2026/month=9/day=7"), scoped to just what this call wrote. Spark/pyarrow
    give every partition rewrite a new random filename, so a downstream Snowflake load
    must PUT exactly these directories rather than rescan the whole bronze tree, or a
    reprocessed day gets loaded twice.
    '''
    df = df.with_columns(
        year=partition_date.dt.year(),
        month=partition_date.dt.month(),
        day=partition_date.dt.day(),
    )
    partition_cols = ["region_code", "year", "month", "day"] if region_partitioned else ["year", "month", "day"]
    df.write_parquet(
        path,
        pyarrow_options={"partition_cols": partition_cols, "existing_data_behavior": "delete_matching"},
        use_pyarrow=True,
    )

    if staging_path:
        Path(staging_path).mkdir(parents=True, exist_ok=True)
        flat_path = f"{staging_path}/daily.parquet"
        df.write_parquet(flat_path)
        return [flat_path]

    touched = df.select(partition_cols).unique()
    return [
        f"{path}/" + "/".join(f"{col}={row[i]}" for i, col in enumerate(partition_cols))
        for row in touched.iter_rows()
    ]

def _latest_partition_value(directory: Path, prefix: str) -> int:
    values = [int(p.name.split("=")[1]) for p in directory.glob(f"{prefix}=*")]
    if not values:
        raise AirflowFailException(
            f"No partitions '{prefix}=*' under {directory} — "
            "try to launch backfill DAG for this dataset before this daily dag."
        )
    return max(values)

def resolve_latest_date(path: str) -> str:
    base = Path(path)
    last_year = _latest_partition_value(base, "year")
    last_month = _latest_partition_value(base / f"year={last_year}", "month")
    last_day = _latest_partition_value(base / f"year={last_year}/month={last_month}", "day")
    return f"{last_year}-{last_month:02d}-{last_day:02d}"

@task
def resolve_target_date(delta_day: int, **context) -> str:
    '''
    Resolve the target date to process.

    If a "target_date" parameter is explicitly provided in the DAG run
    context, it is used as-is. Otherwise, the target date is computed
    by subtracting `delta_day` days from the DAG's logical date.
    '''
    target = context["params"].get("target_date")
    if target:
        return target
    return (context["logical_date"] - timedelta(days=delta_day)).date().isoformat()