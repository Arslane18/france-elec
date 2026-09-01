from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from ingestion_utils import raw_weather_path, year_date_range, compute_years, fetch_and_store
from datetime import timedelta
from energy_pipeline.config import (
    WEATHER_URL,
    WEATHER_HOURLY,
    REGION_COORDS,
    WEATHER_DATA_PATH,
    RAW_DIR,
    SPARK_JOBS_DIR,
)


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

    build_bronze_weather = SparkSubmitOperator(
        task_id="build_bronze_weather",
        conn_id="spark_standalone",
        application=f"{SPARK_JOBS_DIR}/build_bronze_weather.py",
        application_args=["--raw-dir", RAW_DIR, "--output-dir", WEATHER_DATA_PATH],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="512m",
        conf={
            "spark.sql.shuffle.partitions": "4",
            # Read from the driver's own env at submit time and baked into the
            # serialized job, so it must be set here explicitly (the image's
            # PYSPARK_PYTHON alone does not reach the executor's worker exec).
            "spark.pyspark.python": "python3.13",
            "spark.pyspark.driver.python": "python3.13",
            # v1 commits by renaming whole per-partition directories, which
            # breaks when two executors both write the same partition value
            # (e.g. two tasks both have "region=Normandie" rows) on a local
            # filesystem. v2 commits file-by-file instead.
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        },
        pool="spark_pool",
    )

    years = compute_years()
    fetch_weather_data_by_year.expand(year=years, region=REGION_COORDS) >> build_bronze_weather

ingestion_weather()
