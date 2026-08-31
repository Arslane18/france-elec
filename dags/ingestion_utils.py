import requests
from typing import Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from energy_pipeline.config import BASE_URL, WEATHER_URL


session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch_data_from_api(url: str, params: Dict[str,Any]):
    '''Simply fetch data from an given url'''
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp

def retrieve_boundaries_years() -> Dict[str, int]:
    '''
    Retrieve boundaries years and send them as dict. Keys are start_year and end_year.'''
    params = {"select": "Min(year(date_heure)) as start_year, Max(year(date_heure)) as end_year"}
    fetched_result = fetch_data_from_api(url=BASE_URL + "/records", params=params).json()
    return fetched_result["results"][0]

def write_bytes_to_file(content: bytes, path: str) -> None:
    with open(path, "wb") as f:
        f.write(content)
    