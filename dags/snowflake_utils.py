from pathlib import Path

from airflow.sdk import task, XComArg
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@task
def load_bronze_to_snowflake(local_paths: list[str] | str | XComArg, stage_folder: str, table: str) -> None:
    """PUT every parquet file under local_paths to BRONZE.LANDING_STAGE/<stage_folder>/,
    then COPY INTO the target bronze table.

    local_paths should be scoped to just the partition(s) a run actually touched (what
    write_bronze_partitioned returns), not the whole bronze tree: Spark/Polars give every
    partition rewrite a new random filename, so rescanning everything would reload and
    duplicate rows for any day/year that gets reprocessed. The one exception is a one-off
    backfill/full rebuild, where loading the whole tree once is fine since it isn't
    routinely re-run.
    """
    if isinstance(local_paths, str):
        local_paths = [local_paths]

    files = []
    for local_path in local_paths:
        root = Path(local_path)
        files.extend([root] if root.is_file() else sorted(root.rglob("*.parquet")))

    if not files:
        return

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        for f in files:
            cur.execute(
                f"PUT file://{f} @BRONZE.LANDING_STAGE/{stage_folder}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
        cur.execute(f"""
            COPY INTO {table}
            FROM @BRONZE.LANDING_STAGE/{stage_folder}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT
        """)
    finally:
        cur.close()
        conn.close()


@task
def merge_silver_to_snowflake(local_path: str) -> None:
    """PUT the flat, coalesced CONSO_METEO_HORAIRE parquet file(s) to BRONZE.LANDING_STAGE/silver/,
    then MERGE them into SILVER.CONSO_METEO_HORAIRE.

    A MERGE (upsert on DATE_HEURE, REGION_CODE), not a COPY INTO, because
    daily_data_transformation reprocesses a rolling 7-day window on every run -- a plain
    append would duplicate rows for any day inside that overlap. Hardcoded to this one
    table/schema rather than made generic like load_bronze_to_snowflake: there's only one
    silver table today, and unlike COPY INTO's MATCH_BY_COLUMN_NAME, a MERGE's column
    list and join keys can't be inferred generically.
    """
    root = Path(local_path)
    files = [root] if root.is_file() else sorted(root.rglob("*.parquet"))
    if not files:
        return

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        for f in files:
            cur.execute(
                f"PUT file://{f} @BRONZE.LANDING_STAGE/silver/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
        cur.execute("""
            MERGE INTO SILVER.CONSO_METEO_HORAIRE AS tgt
            USING (
                SELECT $1:date_heure::TIMESTAMP_NTZ AS DATE_HEURE,
                       $1:region_code::NUMBER        AS REGION_CODE,
                       $1:consommation::FLOAT        AS CONSOMMATION,
                       $1:temperature_2m::FLOAT      AS TEMPERATURE_2M,
                       $1:precipitation::FLOAT       AS PRECIPITATION,
                       $1:is_holiday::INT            AS IS_HOLIDAY,
                       $1:year::INT                  AS YEAR
                FROM @BRONZE.LANDING_STAGE/silver/
            ) AS src
            ON tgt.DATE_HEURE = src.DATE_HEURE AND tgt.REGION_CODE = src.REGION_CODE
            WHEN MATCHED THEN UPDATE SET
                tgt.CONSOMMATION = src.CONSOMMATION,
                tgt.TEMPERATURE_2M = src.TEMPERATURE_2M,
                tgt.PRECIPITATION = src.PRECIPITATION,
                tgt.IS_HOLIDAY = src.IS_HOLIDAY
            WHEN NOT MATCHED THEN INSERT (DATE_HEURE, REGION_CODE, CONSOMMATION, TEMPERATURE_2M, PRECIPITATION, IS_HOLIDAY, YEAR)
            VALUES (src.DATE_HEURE, src.REGION_CODE, src.CONSOMMATION, src.TEMPERATURE_2M, src.PRECIPITATION, src.IS_HOLIDAY, src.YEAR)
        """)
    finally:
        cur.close()
        conn.close()



@task
def construct_gold_layer(start_date: str | None | XComArg = None) -> None:
    """MERGE SILVER.CONSO_METEO_HORAIRE into GOLD.FACT_CONSOMMATION_HORAIRE.

    Snowflake-to-Snowflake, no PUT/staging needed since both tables already live in the
    warehouse. If start_date is given, scopes the source read to DATE_HEURE >= start_date
    (e.g. daily_data_transformation's own rolling window) instead of rescanning the whole
    silver history -- and re-comparing it row by row against the fact table -- on every run.
    Leave it unset for a one-off full rebuild (mirrors load_bronze_to_snowflake's backfill case).
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    where_clause = "WHERE DATE_HEURE >= %(start_date)s" if start_date else ""
    try:
        cur.execute(f"""
            MERGE INTO GOLD.FACT_CONSOMMATION_HORAIRE AS tgt
            USING (
                SELECT
                    DATE(DATE_HEURE) AS DATE_KEY,
                    DATE_HEURE,
                    REGION_CODE,
                    CONSOMMATION,
                    TEMPERATURE_2M,
                    PRECIPITATION,
                    IS_HOLIDAY,
                    YEAR
                FROM SILVER.CONSO_METEO_HORAIRE
                {where_clause}
            ) AS src
            ON tgt.DATE_HEURE = src.DATE_HEURE AND tgt.REGION_CODE = src.REGION_CODE
            WHEN MATCHED THEN UPDATE SET
                tgt.CONSOMMATION = src.CONSOMMATION,
                tgt.TEMPERATURE_2M = src.TEMPERATURE_2M,
                tgt.PRECIPITATION = src.PRECIPITATION,
                tgt.IS_HOLIDAY = src.IS_HOLIDAY
            WHEN NOT MATCHED THEN INSERT (DATE_KEY, DATE_HEURE, REGION_CODE, CONSOMMATION, TEMPERATURE_2M, PRECIPITATION, IS_HOLIDAY, YEAR)
            VALUES (src.DATE_KEY, src.DATE_HEURE, src.REGION_CODE, src.CONSOMMATION, src.TEMPERATURE_2M, src.PRECIPITATION, src.IS_HOLIDAY, src.YEAR)
        """, {"start_date": start_date} if start_date else None)
    finally:
        cur.close()
        conn.close()

