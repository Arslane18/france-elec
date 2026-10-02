from airflow.sdk import dag

from ingestion_utils import resolve_target_date
from snowflake_utils import (
    merge_silver_to_snowflake,
    construct_gold_layer,
    run_silver_data_quality_checks,
)
from spark_submit_helpers import build_spark_submit_operator
from energy_pipeline.config import (
    ECO2MIX_DATA_PATH,
    WEATHER_DATA_PATH,
    ECO2MIX_WEATHER_DATA_PATH,
    ECO2MIX_WEATHER_STAGING_PATH,
    HOLIDAY_PATH,
    SPARK_JOBS_DIR,
)


def _build_transformation_dag(dag_id: str, tags: list[str], with_window: bool):
    """Factory for the two eco2mix+openmeteo transformation DAGs: a one-off full
    rebuild (with_window=False) and the daily incremental run, which rescopes
    build_silver to a rolling window via resolve_target_date (with_window=True).
    Structurally identical otherwise -- same Spark job, same Snowflake load/merge --
    so kept as one factory instead of two near-duplicate DAG files.

    Only the full rebuild writes the local silver lake: the daily run only feeds the
    staging copy merged into SILVER.CONSO_METEO_HORAIRE, which is the silver source of
    truth (the local lake is a snapshot as of the last full rebuild).
    """

    @dag(dag_id=dag_id, schedule=None, catchup=False, tags=tags)
    def _transformation_dag():
        start_date = resolve_target_date(delta_day=7) if with_window else None

        application_args = [
            "--eco2mix-path", ECO2MIX_DATA_PATH,
            "--openmeteo-path", WEATHER_DATA_PATH,
            "--holiday-path", HOLIDAY_PATH,
            "--staging-dir", ECO2MIX_WEATHER_STAGING_PATH,
        ]
        if with_window:
            application_args += ["--start-date", start_date]
        else:
            application_args += ["--output-dir", ECO2MIX_WEATHER_DATA_PATH]

        build_silver = build_spark_submit_operator(
            task_id="build_silver",
            application=f"{SPARK_JOBS_DIR}/build_silver.py",
            application_args=application_args,
            total_executor_cores=1,
            executor_cores=1,
            executor_memory="1024m",
            driver_memory="1024m",
        )

        load_snowflake = merge_silver_to_snowflake(ECO2MIX_WEATHER_STAGING_PATH)
        dq_check = run_silver_data_quality_checks()

        build_silver >> load_snowflake >> dq_check >> construct_gold_layer(start_date=start_date)

    return _transformation_dag()


data_transformation_dag = _build_transformation_dag(
    dag_id="data_transformation",
    tags=["transformation", "eco2mix", "openmeteo"],
    with_window=False,
)

daily_data_transformation_dag = _build_transformation_dag(
    dag_id="daily_data_transformation",
    tags=["transformation", "eco2mix", "openmeteo", "daily"],
    with_window=True,
)
