from airflow.sdk import dag, task
from ingestion_utils import fetch_data_from_api, retrieve_boundaries_years
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

    @task
    def split_hive_format(root_folder_name: str):
        '''
        Split in hive format /year= /month= /day= for data storage'''
        spark = SparkSession.builder.appName("eco2mix-bronze").getOrCreate()
        df = spark.read.parquet("data/eco2mix-regional-cons-def*.parquet")
        df_with_parts = (
            df
            .withColumn("year", F.year("date_heure"))
            .withColumn("month", F.month("date_heure"))
            .withColumn("day", F.dayofmonth("date_heure"))
        )
        df_with_parts.write.partitionBy("year", "month", "day").parquet(f"data/bronze/{root_folder_name}")

    years = compute_years()
    fetch_year.expand(year=years) >> split_hive_format("eco2mix-regional-cons-def")


simple_extraction()