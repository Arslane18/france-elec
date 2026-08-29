from airflow.sdk import dag, task
from ingestion_utils import fetch_data_from_api, retrieve_boundaries_years


@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion"],
)
def simple_extraction():
    """
    Simple ingestion from Elec API 
    """

    @task
    def compute_years() -> list[int]:
        boundaries = retrieve_boundaries_years()
        return list(range(boundaries["start_year"] + 1, boundaries["end_year"] + 1)) # We add 1 to start_year cause start_year of eco2mix is an empty year.

    @task(max_active_tis_per_dagrun=1)
    def fetch_year(year: int):
        where = f"year(date_heure) = {year}"
        print(where)
        year_of_data = fetch_data_from_api(endpoint="/exports/parquet", where=where)
        with open(f"data/eco2mix-regional-cons-def{str(year)}.parquet", "wb") as wb:
            wb.write(year_of_data.content)

    years = compute_years()
    fetch_year.expand(year=years)


simple_extraction()