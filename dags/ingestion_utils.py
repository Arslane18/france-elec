import requests

from typing import Dict, Any
from pathlib import Path
from datetime import datetime
from airflow.sdk import task
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from energy_pipeline.config import BASE_URL, RAW_DIR, ECO2MIX_NAME, WEATHER_NAME, DAILY_YEAR_MARKER


session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch_data_from_api(url: str, params: Dict[str,Any]):
    '''Simply fetch data from an given url'''
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

def raw_weather_path(region_name: str, year: int) -> str:
    return f"{RAW_DIR}/{WEATHER_NAME}-{region_name}-{year}.json"

def raw_eco2mix_path(year: int) -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}{year}.parquet"

def raw_eco2mix_glob() -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}*.parquet"

def raw_eco2mix_daily_path() -> str:
    return f"{RAW_DIR}/{ECO2MIX_NAME}.parquet"

def raw_weather_daily_path(region_name: str) -> str:
    return raw_weather_path(region_name, DAILY_YEAR_MARKER)

def raw_weather_daily_glob_pattern() -> str:
    return f"{WEATHER_NAME}-*-{DAILY_YEAR_MARKER}.json"

def year_date_range(year: int) -> tuple[str, str]:
    '''Full calendar year, capped at today for the current year.'''
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    if year == datetime.today().year:
        end_date = datetime.today().strftime("%Y-%m-%d")
    return start_date, end_date

def get_latest_date(path):
        base = Path(path)
        last_year = max(int(p.name.split("=")[1]) for p in base.glob("year=*"))
        last_month = max(int(p.name.split("=")[1]) for p in (base / f"year={last_year}").glob("month=*"))
        last_day = max(int(p.name.split("=")[1]) for p in (base / f"year={last_year}" / f"month={last_month}").glob("day=*"))
        return f"{last_year}-{last_month:02d}-{last_day:02d}"