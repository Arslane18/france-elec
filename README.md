# france-elec

**Forecasting French electricity demand** — an end-to-end data pipeline that ingests French electricity consumption and weather data, models the relationship between them, and serves a demand forecast.


## Pipeline

```
RTE éCO2mix + Open-Meteo  →  Airflow (ingestion + backfill)  →  PySpark (join + features)
    →  Snowflake (Bronze / Silver / Gold)  →  Prophet + FastAPI
```

- **Sources**: [RTE éCO2mix](https://www.rte-france.com/eco2mix) regional consumption (hourly, national + regional, history since 2012) joined with [Open-Meteo](https://open-meteo.com/) historical temperature/precipitation (strongly correlated with consumption) and a French public-holiday calendar.
- **Orchestration**: Airflow DAGs handle both the one-off historical backfill and the daily incremental ingestion, for each source independently.
- **Processing**: raw API responses are landed untouched, then a PySpark job (run on a standalone Spark cluster) builds the Hive-partitioned Bronze layer and joins the sources into Silver. Daily incremental runs use Polars directly instead of Spark — the volume doesn't justify a cluster job outside the backfill.
- **Warehouse**: Snowflake holds Bronze (raw per-source tables), Silver (joined hourly consumption + weather), and Gold (a star schema with rolling-average and day-minus-7 features, ready for modeling).
- **Serving** *(planned)*: a Prophet model trained on the Gold layer, exposed through a FastAPI endpoint.

## Tech stack

| | |
|---|---|
| Orchestration | Apache Airflow |
| Distributed processing | PySpark (standalone cluster via Docker) |
| Local/incremental processing | Polars |
| Warehouse | Snowflake |
| Forecasting | Prophet |
| Serving | FastAPI |
| Packaging | uv |

## Repository layout

```
dags/                   Airflow DAGs (ingestion, backfill, daily runs, Snowflake loads)
spark_jobs/             PySpark jobs run via SparkSubmitOperator (Bronze build, Silver join)
src/energy_pipeline/    Shared package: config, path helpers, API clients
snowflake/              DDL, run in order (00_setup → 01_bronze → 02_silver → 03_gold → 04_data_test)
docker/spark/           Spark cluster image (docker-compose.yml: 1 master + 2 workers)
data/                   Local raw/bronze/silver parquet lake (backfill working area)
tests/
```

## Status

Built and wired end-to-end:
- Bronze ingestion (backfill + daily) for both RTE éCO2mix and Open-Meteo, Hive-partitioned in the local lake and loaded into Snowflake Bronze.
- Silver layer: Spark join of consumption + weather + public holidays, merged into Snowflake Silver.
- Gold layer: star schema (`DIM_DATE`, `DIM_REGION`, `FACT_CONSOMMATION_HORAIRE`) populated in Snowflake with 7-day rolling average and J-7 lag features.

## Running it locally

Requires Python 3.13, [uv](https://docs.astral.sh/uv/), Docker, and a (free) [Snowflake trial account](https://signup.snowflake.com/) for the warehouse steps.

### 1. Install dependencies

```bash
uv sync
```

### 2. Start the Spark cluster

```bash
docker compose up -d --build
```

Spins up 1 master + 2 workers (`docker-compose.yml`), reachable at `spark://localhost:7077`, UI at `http://localhost:8090`.

### 3. Set up Snowflake

Run the DDL scripts against your account, in order:

```bash
snowsql -f snowflake/00_setup_warehouse_db.sql
snowsql -f snowflake/01_bronze_ddl.sql
snowsql -f snowflake/02_silver_ddl.sql
snowsql -f snowflake/03_gold_star_schema.sql
```

This creates the `ELEC_FORECAST` database with `BRONZE` / `SILVER` / `GOLD` schemas.

### 4. Configure Airflow

```bash
export AIRFLOW_HOME=$(pwd)/.airflow
uv run airflow standalone
```

`airflow standalone` prints an auto-generated admin password and serves the UI at `http://localhost:8080`. Then, in another terminal, register the two connections and the Spark pool the DAGs expect:

```bash
uv run airflow connections add snowflake_default \
  --conn-type snowflake \
  --conn-login <your_snowflake_user> \
  --conn-extra "{\"account\": \"<your_account_id>\", \"warehouse\": \"<your_warehouse>\", \"database\": \"ELEC_FORECAST\", \"private_key_file\": \"$(pwd)/rsa_key.p8\"}"

uv run airflow connections add spark_standalone \
  --conn-type spark \
  --conn-host spark://localhost \
  --conn-port 7077

uv run airflow pools set spark_pool 2 "Spark standalone cluster slots"
```

`dags/` and `src/` need to be importable — point Airflow at the repo's `dags/` folder (default `$AIRFLOW_HOME/dags`, or set `[core] dags_folder` in `airflow.cfg` to this repo's `dags/`), and make sure the project package is installed in the same environment (`uv sync` already does this).

### 5. Run the pipeline

From the Airflow UI (`http://localhost:8080`), trigger, in order:
1. `ingestion_elec` and `ingestion_weather` — historical backfill (RTE since 2012, Open-Meteo per region), writes raw + Bronze locally and to Snowflake.
2. `holidays` — generates the public-holiday calendar.
3. `data_transformation` — Spark join into Silver, then Gold (rolling average + J-7 lag) in Snowflake.

After that, `daily_eco2mix`, `daily_weather`, and `daily_data_transformation` keep it up to date incrementally and can be scheduled.

Backfilling the full history (2012–present, ~2.8M rows) takes a while — the year range is computed automatically from what the RTE API reports as available, not hardcoded, so to try it on a smaller slice first you'd need to temporarily edit `compute_years()` in `dags/ingestion_utils.py`.

## TODO

- [ ] **Forecasting model** — train/evaluate Prophet on the Gold layer (per-region and/or national), with a backtesting strategy.
- [ ] **Serving API** — FastAPI endpoint(s) to expose forecasts (and possibly historical/actuals for comparison).
- [ ] **Model artifact handling** — where trained models are stored/versioned and how the API loads them.
- [ ] **Tests** — `tests/` is currently empty; add coverage for ingestion parsing, Spark transformations, and Snowflake load logic.
- [ ] **Deployment** — containerize the API (and possibly Airflow) for a reproducible demo; currently only the Spark cluster is dockerized.
- [ ] **Dashboard/visualization** *(nice-to-have)* — simple front end or notebook to showcase forecast vs. actual consumption for recruiters.
