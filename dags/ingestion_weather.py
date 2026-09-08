from datetime import timedelta

from airflow.sdk import dag, task

from ingestion_utils import (
    raw_weather_path,
    year_date_range,
    compute_years,
    fetch_and_store
)
from snowflake_utils import load_bronze_to_snowflake
from spark_submit_helpers import build_spark_submit_operator
from energy_pipeline.config import (
    WEATHER_URL,
    WEATHER_HOURLY,
    REGION_COORDS,
    WEATHER_DATA_PATH,
    WEATHER_STAGING_PATH,
    RAW_DIR,
    SPARK_JOBS_DIR,
)



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "weather", "backfill"],
)
def backfill_weather():
    """Backfills open-meteo hourly weather year by year, per region, into the bronze layer via Spark."""

    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_weather_year(year: int, region):
        """Fetch one full year of hourly weather data for a single region and write it as raw JSON."""
        region_code, (latitude, longitude) = region
        start_date, end_date = year_date_range(year)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": WEATHER_HOURLY,
        }
        fetch_and_store(url=WEATHER_URL, params=params, path=raw_weather_path(region_code, year))

    build_bronze_weather = build_spark_submit_operator(
        task_id="build_bronze_weather",
        application=f"{SPARK_JOBS_DIR}/build_bronze_weather.py",
        application_args=["--raw-dir", RAW_DIR, "--output-dir", WEATHER_DATA_PATH, "--staging-dir", WEATHER_STAGING_PATH],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="512m",
    )

    load_snowflake = load_bronze_to_snowflake(WEATHER_STAGING_PATH, "openmeteo", "OPENMETEO")

    years = compute_years()
    fetch_weather_year.expand(year=years, region=list(REGION_COORDS.items())) >> build_bronze_weather >> load_snowflake

backfill_weather()
