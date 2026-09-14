import polars as pl

from datetime import timedelta
from airflow.sdk import dag, task
from ingestion_utils import (
    fetch_and_store,
    resolve_latest_partition_date,
    raw_eco2mix_daily_path,
    write_bronze_partitioned,
)
from snowflake_utils import load_bronze_to_snowflake
from energy_pipeline.config import ECO2MIX_DATA_PATH, ECO2MIX_SELECT_COLUMNS, RTE_URL



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "eco2mix", "daily"],
)
def daily_eco2mix():
    """Daily incremental ingestion of eco2mix consumption records into the bronze layer."""


    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_eco2mix_updates() -> str:
        """Fetch eco2mix records more recent than target_date and write them as raw parquet."""
        target_date = resolve_latest_partition_date(path=ECO2MIX_DATA_PATH)
        params = {"select": ECO2MIX_SELECT_COLUMNS, "where": f"date_heure > date'{target_date}'"}
        path = raw_eco2mix_daily_path()
        fetch_and_store(url=RTE_URL + "/exports/parquet", params=params, path=path)
        return path

    @task
    def write_eco2mix_bronze(path) -> list[str]:
        """Append the freshly fetched raw parquet to the eco2mix bronze table, partitioned by year/month/day."""
        df = pl.read_parquet(path)
        # Used to have problems between dailys and backfills runs, this resolve the problem by standardizing schema.
        df = df.with_columns(
            date_heure=pl.col('date_heure').dt.replace_time_zone(None).dt.cast_time_unit("us"),
        )
        return write_bronze_partitioned(
            df,
            partition_date_expr=pl.col('date').str.to_datetime("%Y-%m-%d"),
            path=ECO2MIX_DATA_PATH,
        )

    path = fetch_eco2mix_updates()
    touched_partitions = write_eco2mix_bronze(path)
    load_bronze_to_snowflake(touched_partitions, "eco2mix", "ECO2MIX_REGIONAL")

daily_eco2mix()