import polars as pl

from datetime import timedelta
from airflow.sdk import dag, task
from ingestion_utils import (
    fetch_and_store,
    get_latest_date,
    raw_eco2mix_daily_path,
    write_bronze_partitioned,
)
from energy_pipeline.config import ECO2MIX_DATA_PATH, RTE_URL



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "eco2mix", "daily"],
)
def daily_eco2mix():
    """Daily incremental ingestion of eco2mix consumption records into the bronze layer."""


    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_eco2mix_updates(target_date) -> str:
        """Fetch eco2mix records more recent than target_date and write them as raw parquet."""
        params = {"where": f"date_heure > date'{target_date}'"}
        path = raw_eco2mix_daily_path()
        fetch_and_store(url=RTE_URL + "/exports/parquet", params=params, path=path)
        return path

    @task
    def write_eco2mix_bronze(path):
        """Append the freshly fetched raw parquet to the eco2mix bronze table, partitioned by year/month/day."""
        df = pl.read_parquet(path)
        df = df.with_columns(date=pl.col('date').str.to_datetime("%Y-%m-%d"))
        write_bronze_partitioned(df, date_col="date", path=ECO2MIX_DATA_PATH)
    
    target_date = get_latest_date(path=ECO2MIX_DATA_PATH)
    path = fetch_eco2mix_updates(target_date)
    write_eco2mix_bronze(path)

daily_eco2mix()