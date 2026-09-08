from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ingestion_utils import (
    compute_years,
    fetch_and_store,
    raw_eco2mix_path,
    raw_eco2mix_glob
)
from snowflake_utils import load_bronze_to_snowflake
from energy_pipeline.config import (
    BASE_URL, 
    ECO2MIX_DATA_PATH, 
    ECO2MIX_STAGING_PATH, 
    ECO2MIX_SELECT_COLUMNS, 
    SPARK_JOBS_DIR, 
    SPARK_CONFIG,
)



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "eco2mix", "backfill"],
)
def backfill_eco2mix():
    """Backfills eco2mix regional consumption year by year from RTE's API into the bronze layer via Spark."""

    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_eco2mix_year(year: int):
        """Fetch one full year of eco2mix consumption records and write it as raw parquet."""
        params = {"select": ECO2MIX_SELECT_COLUMNS, "where": f"year(date_heure) = {year}"}
        fetch_and_store(url=BASE_URL + "/exports/parquet", params=params, path=raw_eco2mix_path(year))

    build_bronze_eco2mix = SparkSubmitOperator(
        task_id="build_bronze_eco2mix",
        conn_id="spark_standalone",
        application=f"{SPARK_JOBS_DIR}/build_bronze_eco2mix.py",
        application_args=["--raw-glob", raw_eco2mix_glob(), "--output-dir", ECO2MIX_DATA_PATH, "--staging-dir", ECO2MIX_STAGING_PATH],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="512m",
        conf=SPARK_CONFIG,
        pool="spark_pool",
    )

    load_snowflake = load_bronze_to_snowflake(ECO2MIX_STAGING_PATH, "eco2mix", "ECO2MIX_REGIONAL")

    years = compute_years()
    fetch_eco2mix_year.expand(year=years) >> build_bronze_eco2mix >> load_snowflake


backfill_eco2mix()
