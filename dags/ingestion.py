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

    @task()
    def extract_data_from_eco2mix():
        boundaries_years = retrieve_boundaries_years()
        start_year, end_year = boundaries_years["start_year"], boundaries_years["end_year"]
        for curr_year in range(start_year + 1, end_year + 1): # We add 1 to start_year cause start_year of eco2mix is an empty year. 
            year_of_data = fetch_data_from_api(endpoint="/exports/parquet").content
            with open("data/eco2mix-regional-cons-def"+str(curr_year)+".parquet", "wb") as wb:
                wb.write(year_of_data)

    extract_data_from_eco2mix()

simple_extraction()