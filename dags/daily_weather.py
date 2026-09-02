from airflow.sdk import dag, task
from ingestion_utils import fetch_and_store, get_latest_date, raw_weather_path, region_name_from_filename
from datetime import timedelta, date
from energy_pipeline.config import REGION_COORDS, WEATHER_HOURLY, WEATHER_URL
import json
import polars as pl
from pathlib import Path



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "weather"],
)
def daily_ingestion_weather():
    """
    Simple ingestion from openmeteo API
    """


    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_up_to_date(region):
        start_date = get_latest_date(path="data/bronze/openmeteo/region=Bretagne")
        end_date = date.today().isoformat()
        region_name, (latitude, longitude) = region
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": WEATHER_HOURLY,
        }
        fetch_and_store(url=WEATHER_URL, params=params, path=raw_weather_path(region_name, 0))

    @task
    def add_to_hive(path):
        rows = []
        for path in sorted(Path("/home/arsla/personal_projects/france-elec/data/raw").glob(pattern="openmeteo-*-0.json")):
            region_name = region_name_from_filename(path)
            with open(path, "r") as file:
                hourly = json.load(file)["hourly"]
            for values in zip(*hourly.values()):
                row = dict(zip(hourly.keys(), values))
                row["region"] = region_name
                rows.append(row)

        df = pl.from_dicts(rows)
        df = df.with_columns(
            time = pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M"),
        )
        df = df.with_columns(
            year = pl.col('time').dt.year(),
            month = pl.col('time').dt.month(),
            day = pl.col('time').dt.day(),
        )
        df.write_parquet("data/bronze/openmeteo", pyarrow_options={"partition_cols": ["region", "year", "month", "day"]}, use_pyarrow=True)
    
    path = fetch_up_to_date.expand(region=REGION_COORDS)
    add_to_hive(path)

daily_ingestion_weather()