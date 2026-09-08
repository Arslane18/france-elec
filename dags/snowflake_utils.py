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
