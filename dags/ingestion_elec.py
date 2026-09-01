from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from ingestion_utils import compute_years, fetch_and_store, raw_eco2mix_path, raw_eco2mix_glob
from datetime import timedelta
from energy_pipeline.config import BASE_URL, ECO2MIX_DATA_PATH, SPARK_JOBS_DIR

@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion"],
)
def simple_extraction():
    """
    Simple ingestion from Elec API
    """

    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_year(year: int):
        params = {"where": f"year(date_heure) = {year}"}
        fetch_and_store(url=BASE_URL + "/exports/parquet", params=params, path=raw_eco2mix_path(year))

    build_bronze_eco2mix = SparkSubmitOperator(
        task_id="build_bronze_eco2mix",
        conn_id="spark_standalone",
        application=f"{SPARK_JOBS_DIR}/build_bronze_eco2mix.py",
        application_args=["--raw-glob", raw_eco2mix_glob(), "--output-dir", ECO2MIX_DATA_PATH],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="512m",
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.pyspark.python": "python3.13",
            "spark.pyspark.driver.python": "python3.13",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        },
        pool="spark_pool",
    )

    years = compute_years()
    fetch_year.expand(year=years) >> build_bronze_eco2mix


simple_extraction()
