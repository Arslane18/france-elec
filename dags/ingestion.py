from airflow.sdk import dag, task
from ingestion_utils import fetch_data_from_api, retrieve_boundaries_years, write_bytes_to_file
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import timedelta
from energy_pipeline.config import BASE_URL, ECO2MIX_DATA_PATH

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

    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_year(year: int):
        params = {"where": f"year(date_heure) = {year}"}
        resp = fetch_data_from_api(url=BASE_URL + "/exports/parquet", params=params)
        path = f"data/raw/eco2mix-regional-cons-def{year}.parquet"
        write_bytes_to_file(resp.content, path)

    @task
    def split_hive_format():
        '''
        Split in hive format /year= /month= /day= for data storage'''
        spark = SparkSession.builder.appName("eco2mix-bronze").getOrCreate()
        df = spark.read.parquet("data/raw/eco2mix-regional-cons-def*.parquet")
        df_with_parts = (
            df
            .withColumn("year", F.year("date_heure"))
            .withColumn("month", F.month("date_heure"))
            .withColumn("day", F.dayofmonth("date_heure"))
        )
        df_with_parts.write.partitionBy("year", "month", "day").parquet(ECO2MIX_DATA_PATH)

    years = compute_years()
    fetch_year.expand(year=years) >> split_hive_format()


simple_extraction()