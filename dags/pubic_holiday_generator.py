import argparse

import pandas as pd
from jours_feries_france import JoursFeries
from energy_pipeline.config import REGION_COORDS

REGION_ZONE = {
    region: "Alsace-Moselle" if region == "Grand Est" else "Métropole"
    for region in REGION_COORDS
}

def build_dataframe(start_year: int, end_year: int) -> pd.DataFrame:
    rows = []
    for year in range(start_year, end_year + 1):
        holidays_by_zone = {
            zone: JoursFeries.for_year(year, zone=zone)
            for zone in set(REGION_ZONE.values())
        }
        for region, (lat, lon) in REGION_COORDS.items():
            zone = REGION_ZONE[region]
            for name, date in holidays_by_zone[zone].items():
                rows.append(
                    {
                        "region": region,
                        "date": date,
                        "annee": year,
                        "jour_ferie": name,
                    }
                )

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["region", "date"]).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2013)
    parser.add_argument("--end-year", type=int, default=2030)
    parser.add_argument("--output", default="data/bronze/public_holidays.parquet")
    args = parser.parse_args()

    df = build_dataframe(args.start_year, args.end_year)
    df.to_parquet(args.output, index=False)
    print(f"{len(df)} lines written in {args.output}")


if __name__ == "__main__":
    main()