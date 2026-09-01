from airflow.sdk import dag, task
from ingestion_utils import fetch_data_from_api, write_bytes_to_file, raw_weather_path, year_date_range, compute_years, fetch_and_store
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from datetime import timedelta
import json
from energy_pipeline.config import WEATHER_URL, WEATHER_HOURLY, REGION_COORDS, WEATHER_DATA_PATH


@dag(
    schedule=None,
    catchup=False,
    tags=["oui"],
)
def ingestion_weather():
    """
    Simple ingestion from Weather API
    """
    
    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_weather_data_by_year(year: int, region):
        region_name, (latitude, longitude) = region
        start_date, end_date = year_date_range(year)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": WEATHER_HOURLY,
        }
        fetch_and_store(url=WEATHER_URL, params=params, path=raw_weather_path(region_name, year))

    @task
    def split_hive_format(year, region):
        '''
        Split in hive format /year= /month= /day= for data storage'''
        region_name, _ = region
        with open(raw_weather_path(region_name, year), "r") as file:
            hourly = json.load(file)["hourly"]
        rows = [dict(zip(hourly.keys(), values)) for values in zip(*hourly.values())]

        spark = SparkSession.builder.appName("openmeteo").getOrCreate()
        df = (
            spark.createDataFrame(rows)
            .withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            .withColumn("region", lit(region_name))
            .withColumn("year", F.year("time"))
            .withColumn("month", F.month("time"))
            .withColumn("day", F.dayofmonth("time"))
        )
        df.write.partitionBy("region", "year", "month", "day").parquet(
            WEATHER_DATA_PATH, mode="overwrite"
        )


    years = compute_years()
    fetch_weather_data_by_year.expand(year=years, region=REGION_COORDS) >> split_hive_format.expand(year=years, region=REGION_COORDS)

ingestion_weather()