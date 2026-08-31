from airflow.sdk import dag, task
from ingestion_utils import fetch_data_from_api, retrieve_boundaries_years, write_bytes_to_file
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, lit
from datetime import timedelta, datetime
import polars as pl
import json
from energy_pipeline.config import WEATHER_URL, WEATHER_HOURLY, REGION_COORDS


@dag(
    schedule=None,
    catchup=False,
    tags=["oui"],
)
def ingestion_weather():
    """
    Simple ingestion from Weather API
    """

    @task
    def compute_years() -> list[int]:
        boundaries = retrieve_boundaries_years()
        return list(range(boundaries["start_year"] + 1, boundaries["end_year"] + 1)) # We add 1 to start_year cause start_year of eco2mix is an empty year.
    
    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_weather_data_by_year(year: int, region):
        region_name = region[0]
        region_coord = region[1]
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        if str(year) == datetime.today().strftime('%Y'):
            end_date = datetime.today().strftime('%Y-%m-%d')
        params = {
            "latitude": region_coord[0],
            "longitude": region_coord[1],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": WEATHER_HOURLY
        }
        resp = fetch_data_from_api(url=WEATHER_URL, params=params)
        path = f"data/raw/openmeteo-{region_name}-{year}.json"
        write_bytes_to_file(resp.content, path)

    @task
    def split_hive_format(year, region):
            '''
            Split in hive format /year= /month= /day= for data storage'''
            root_folder_name = "openmeteo"
            spark = SparkSession.builder.appName("openmeteo").getOrCreate()
            region_name = region[0]
            with open(f"data/raw/openmeteo-{region_name}-{year}.json", "r") as file:
                data = json.load(file)
            hourly = data["hourly"]
            rows = [dict(zip(hourly.keys(), values)) for values in zip(*hourly.values())]
            df = spark.createDataFrame(rows)
            df = df.withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            df = df.withColumn("region", lit(region_name))
            df_with_parts = (
                df
                .withColumn("year", F.year("time"))
                .withColumn("month", F.month("time"))
                .withColumn("day", F.dayofmonth("time"))
            )
            df_with_parts.write.partitionBy("region","year", "month", "day").parquet(f"data/bronze/{root_folder_name}", mode="overwrite")


    years = compute_years()
    fetch_weather_data_by_year.expand(year=years, region=REGION_COORDS) >> split_hive_format.expand(year=years, region=REGION_COORDS)

ingestion_weather()