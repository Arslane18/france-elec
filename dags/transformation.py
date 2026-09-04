from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from energy_pipeline.config import (
    ECO2MIX_DATA_PATH,
    WEATHER_DATA_PATH,
    HOLIDAY_PATH,
    SPARK_JOBS_DIR, 
    ECO2MIX_WEATHER_DATA_PATH, 
)



@dag(
    schedule=None,
    catchup=False,
    tags=["transformation", "eco2mix", "openmeteo"],
)
def data_transformation():

    build_silver = SparkSubmitOperator(
        task_id="build_silver",
        conn_id="spark_standalone",
        application=f"{SPARK_JOBS_DIR}/build_silver.py",
        application_args=["--eco2mix-path", ECO2MIX_DATA_PATH, "--openmeteo-path", WEATHER_DATA_PATH, "--output-dir", ECO2MIX_WEATHER_DATA_PATH, "--holiday-path", HOLIDAY_PATH],
        total_executor_cores=1,
        executor_cores=1,
        executor_memory="1024m",
        driver_memory="1024m",
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.pyspark.python": "python3.13",
            "spark.pyspark.driver.python": "python3.13",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        },
        pool="spark_pool",
    )
    
    build_silver


data_transformation()
