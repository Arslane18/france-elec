import json
import polars as pl

from pathlib import Path
from datetime import timedelta, date
from airflow.sdk import dag, task

from energy_pipeline.global_utils import region_name_from_filename

from ingestion_utils import (
    fetch_and_store,
    get_latest_date,
    raw_weather_daily_path,
    raw_weather_daily_glob_pattern,
)
from energy_pipeline.config import (
    REGION_COORDS, 
    WEATHER_HOURLY, 
    WEATHER_URL, 
    WEATHER_DATA_PATH, 
    RAW_DIR
)



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "weather", "daily"],
)
def daily_weather():
    """Daily incremental ingestion of open-meteo hourly weather, per region, into the bronze layer."""


    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_weather_updates(region):
        """Fetch hourly weather data for one region from the last ingested date up to today, and write it as raw JSON."""
        start_date = get_latest_date(path=f"{WEATHER_DATA_PATH}/region=Bretagne")
        end_date = date.today().isoformat()
        region_name, (latitude, longitude) = region
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": WEATHER_HOURLY,
        }
        fetch_and_store(url=WEATHER_URL, params=params, path=raw_weather_daily_path(region_name))

    @task
    def write_weather_bronze(_fetched_paths):
        """Combine all raw daily weather JSON files into the openmeteo bronze table, partitioned by region/year/month/day."""
        # _fetched_paths is unused: it only forces this task to depend on every fetch_weather_updates instance.
        rows = []
        for path in sorted(Path(RAW_DIR).glob(pattern=raw_weather_daily_glob_pattern())):
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
        df.write_parquet(WEATHER_DATA_PATH, pyarrow_options={"partition_cols": ["region", "year", "month", "day"]}, use_pyarrow=True)
    
    fetched_paths = fetch_weather_updates.expand(region=REGION_COORDS)
    write_weather_bronze(fetched_paths)

daily_weather()