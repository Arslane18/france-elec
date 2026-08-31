import requests
from typing import Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from energy_pipeline.config import BASE_URL


session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch_data_from_api(select: str = "*", limit=None, offset=0, where=None, endpoint:str="/records"):
    params = {"select": select, "limit": limit, "offset": offset}
    if where:
        params["where"] = where
    resp = requests.get(BASE_URL + endpoint, params=params, timeout=30)
    resp.raise_for_status()
    return resp

def retrieve_boundaries_years() -> Dict[str, int]:
    '''
    Retrieve boundaries years and send them as dict. Keys are start_year and end_year.'''
    fetched_result = fetch_data_from_api(select="Min(year(date_heure)) as start_year, Max(year(date_heure)) as end_year").json()
    return fetched_result["results"][0]
    