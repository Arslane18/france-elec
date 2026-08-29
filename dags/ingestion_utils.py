import requests
from typing import Dict
from energy_pipeline.config import BASE_URL


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
    