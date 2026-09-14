import pandas as pd

from airflow.sdk import dag, task
from snowflake_utils import load_bronze_to_snowflake
from jours_feries_france import JoursFeries
from energy_pipeline.config import HOLIDAY_PATH, REGION_COORDS

REGION_ZONE = {
    region: "Alsace-Moselle" if region == 44 else "Métropole"
    for region in REGION_COORDS
}

def build_dataframe(start_year: int, end_year: int) -> pd.DataFrame:
    rows = []
    for year in range(start_year, end_year + 1):
        holidays_by_zone = {
            zone: JoursFeries.for_year(year, zone=zone)
            for zone in set(REGION_ZONE.values())
        }
        for region_code in REGION_COORDS.keys():
            zone = "Alsace-Moselle" if region_code == 44 else "Métropole"
            for name, date in holidays_by_zone[zone].items():
                rows.append(
                    {
                        "region_code": region_code,
                        "date": date,
                        "jour_ferie": name,
                        "is_holiday": 1,
                    }
                )

    df = pd.DataFrame(rows)
    return df.sort_values(["region_code", "date"]).reset_index(drop=True)


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
