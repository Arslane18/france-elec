from airflow.sdk import dag, task

from pubic_holiday_generator import build_dataframe
from snowflake_utils import load_bronze_to_snowflake
from energy_pipeline.config import HOLIDAY_PATH


@dag(
    schedule=None,
    catchup=False,
    tags=["ingestion", "holidays"],
)
def holidays():
    """Generates French public holidays per region and loads them into the bronze layer.

    Manually triggered, not scheduled: the calendar rarely changes, no need for a daily run.
    """

    @task
    def build_holidays() -> str:
        df = build_dataframe(start_year=2013, end_year=2030)
        df.to_parquet(HOLIDAY_PATH, index=False)
        return HOLIDAY_PATH

    path = build_holidays()
    load_bronze_to_snowflake(path, "holidays", "PUBLIC_HOLIDAYS")


holidays()
