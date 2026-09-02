from airflow.sdk import dag, task
from ingestion_utils import fetch_and_store
from datetime import timedelta
from energy_pipeline.config import RAW_DIR, ECO2MIX_NAME, RTE_URL
import json
import polars as pl
from pathlib import Path



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion"],
)
def daily_ingestion():
    """
    Simple ingestion from Elec API
    """


    @task
    def get_latest_date():
        base = Path("data/bronze/eco2mix-regional-cons-def")
        last_year = max(int(p.name.split("=")[1]) for p in base.glob("year=*"))
        last_month = max(int(p.name.split("=")[1]) for p in (base / f"year={last_year}").glob("month=*"))
        last_day = max(int(p.name.split("=")[1]) for p in (base / f"year={last_year}" / f"month={last_month}").glob("day=*"))
        return f"{last_year}-{last_month:02d}-{last_day:02d}"

    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_up_to_date(target_date) -> str:
        params = {"where": f"date_heure > date'{target_date}'"}
        path = f"{RAW_DIR}/{ECO2MIX_NAME}.parquet"
        fetch_and_store(url=RTE_URL + "/exports/parquet", params=params, path=path)
        return path

    @task
    def add_to_hive(path):
        df = pl.read_parquet(path)
        df = df.with_columns(
            year = pl.col('date').str.to_datetime("%Y-%m-%d").dt.year(),
            month = pl.col('date').str.to_datetime("%Y-%m-%d").dt.month(),
            day = pl.col('date').str.to_datetime("%Y-%m-%d").dt.day(),
        )
        df.write_parquet("data/bronze/eco2mix-regional-cons-def", pyarrow_options={"partition_cols": ["year", "month", "day"]}, use_pyarrow=True)
    
    target_date = get_latest_date()
    path = fetch_up_to_date(target_date)
    add_to_hive(path)

daily_ingestion()