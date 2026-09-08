from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from energy_pipeline.config import SPARK_CONFIG


def build_spark_submit_operator(
    task_id: str,
    application: str,
    application_args: list[str],
    total_executor_cores: int,
    executor_cores: int,
    executor_memory: str,
    driver_memory: str,
) -> SparkSubmitOperator:
    """SparkSubmitOperator against the project's standalone cluster (conn_id
    spark_standalone, pool spark_pool), with SPARK_CONFIG applied.
    """
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_standalone",
        application=application,
        application_args=application_args,
        total_executor_cores=total_executor_cores,
        executor_cores=executor_cores,
        executor_memory=executor_memory,
        driver_memory=driver_memory,
        conf=SPARK_CONFIG,
        pool="spark_pool",
    )