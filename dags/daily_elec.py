from airflow.sdk import dag, task
from ingestion_utils import fetch_and_store, get_latest_date, raw_eco2mix_daily_path
from datetime import timedelta
from energy_pipeline.config import ECO2MIX_DATA_PATH, RTE_URL
import polars as pl



@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion"],
)
def daily_ingestion():
    """
    Simple ingestion from Elec API
    """


    @task(max_active_tis_per_dagrun=1, retries=3, retry_delay=timedelta(minutes=1))
    def fetch_up_to_date(target_date) -> str:
        params = {"where": f"date_heure > date'{target_date}'"}
        path = raw_eco2mix_daily_path()
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
        df.write_parquet(ECO2MIX_DATA_PATH, pyarrow_options={"partition_cols": ["year", "month", "day"]}, use_pyarrow=True)
    
    target_date = get_latest_date(path=ECO2MIX_DATA_PATH)
    path = fetch_up_to_date(target_date)
    add_to_hive(path)

daily_ingestion()