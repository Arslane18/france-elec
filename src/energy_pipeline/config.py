from pathlib import Path

# Absolute on purpose: the Spark standalone cluster (Docker) mounts this same
# project directory at this same absolute path, so paths built from RAW_DIR /
# BRONZE_DIR resolve identically whether read from the Airflow host process
# or from a Spark executor running inside a container.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

BASE_URL = "https://reseaux-energies-rte.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def"
RTE_URL = "https://reseaux-energies-rte.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr"

RAW_DIR = str(PROJECT_ROOT / "data" / "raw")
BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
SPARK_JOBS_DIR = str(PROJECT_ROOT / "spark_jobs")

ECO2MIX_NAME = "eco2mix-regional-cons-def"
ECO2MIX_DATA_PATH = f"{BRONZE_DIR}/{ECO2MIX_NAME}"
ECO2MIX_SELECT_COLUMNS = "date_heure, date, code_insee_region, libelle_region, nature, consommation"

WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"

WEATHER_HOURLY = ["temperature_2m", "precipitation"]

WEATHER_NAME = "openmeteo"
WEATHER_DATA_PATH = f"{BRONZE_DIR}/{WEATHER_NAME}"

DAILY_MARKER = "daily"

# The joined eco2mix + weather (+ later, calendar) table — one dataset, not one
# per source like bronze, since the sources are already merged by this point.
ECO2MIX_WEATHER_DATA_PATH = f"{SILVER_DIR}/eco2mix_weather"

#We need this because Historical Weather API only accepts geographical points and not names of city/regions. Sorted regions in alphabetical order bc im a psycho.
REGION_COORDS = {
    "Auvergne-Rhône-Alpes": (45.7640, 4.8357),      # Lyon
    "Bourgogne-Franche-Comté": (47.3220, 5.0415),   # Dijon
    "Bretagne": (48.1173, -1.6778),                 # Rennes
    "Centre-Val de Loire": (47.3941, 0.6848),       # Tours
    "Grand Est": (48.5734, 7.7521),                 # Strasbourg
    "Hauts-de-France": (50.6292, 3.0573),           # Lille
    "Normandie": (49.4944, 0.1079),                 # Le Havre
    "Nouvelle-Aquitaine": (44.8378, -0.5792),        # Bordeaux
    "Occitanie": (43.6047, 1.4442),                 # Toulouse
    "Pays de la Loire": (47.2184, -1.5536),         # Nantes
    "Provence-Alpes-Côte d'Azur": (43.2965, 5.3698), # Marseille
    "Île-de-France": (48.8566, 2.3522),             # Paris
}