"""Weather ELT pipeline: Open-Meteo API -> raw schema -> dbt (staging, marts, tests).

Each daily run:
  1. create_raw_table : make sure raw.weather_daily exists
  2. extract_and_load : fetch the run's day for each city and upsert it (idempotent)
  3. dbt_run          : build staging and mart models
  4. dbt_test         : run the data tests
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
CONN_ID = "warehouse_db"  # defined in docker-compose via AIRFLOW_CONN_WAREHOUSE_DB

# dbt is installed in its own virtual environment; the project is mounted at /opt/airflow/dbt.
# Warehouse credentials reach dbt through environment variables (see dbt/profiles.yml).
DBT_CMD = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt"

# city: (latitude, longitude)
CITIES = {
    "Bengaluru": (12.9716, 77.5946),
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.6139, 77.2090),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867),
    "Pune": (18.5204, 73.8567),
    "Ahmedabad": (23.0225, 72.5714),
    "Jaipur": (26.9124, 75.7873),
    "Visakhapatnam": (17.6868, 83.2185),
}

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "weather_code",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.weather_daily (
    city               TEXT             NOT NULL,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    weather_date       DATE             NOT NULL,
    temp_max_c         DOUBLE PRECISION,
    temp_min_c         DOUBLE PRECISION,
    temp_mean_c        DOUBLE PRECISION,
    precipitation_mm   DOUBLE PRECISION,
    wind_speed_max_kmh DOUBLE PRECISION,
    weather_code       INT,
    loaded_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (city, weather_date)
);
"""

UPSERT_SQL = """
INSERT INTO raw.weather_daily (
    city, latitude, longitude, weather_date,
    temp_max_c, temp_min_c, temp_mean_c,
    precipitation_mm, wind_speed_max_kmh, weather_code
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, weather_date) DO UPDATE SET
    temp_max_c         = EXCLUDED.temp_max_c,
    temp_min_c         = EXCLUDED.temp_min_c,
    temp_mean_c        = EXCLUDED.temp_mean_c,
    precipitation_mm   = EXCLUDED.precipitation_mm,
    wind_speed_max_kmh = EXCLUDED.wind_speed_max_kmh,
    weather_code       = EXCLUDED.weather_code,
    loaded_at          = now();
"""


def build_session() -> requests.Session:
    """HTTP session that retries temporary API errors with a growing wait."""
    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dag(
    dag_id="weather_raw_ingest",
    description="Daily weather ELT: Open-Meteo -> raw.weather_daily -> dbt staging/marts + tests",
    schedule="@daily",
    start_date=datetime(2026, 10, 12),  # keep within the last ~90 days (API limit)
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "ramesh",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["weather", "elt", "dbt"],
)
def weather_raw_ingest():
    @task
    def create_raw_table() -> None:
        PostgresHook(postgres_conn_id=CONN_ID).run(CREATE_TABLE_SQL)

    @task
    def extract_and_load(ds: str | None = None) -> int:
        """Fetch one day of weather per city and upsert it into the raw table."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        session = build_session()
        loaded = 0

        for city, (lat, lon) in CITIES.items():
            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": ",".join(DAILY_VARS),
                "timezone": "Asia/Kolkata",
                "start_date": ds,
                "end_date": ds,
            }
            response = session.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            daily = response.json().get("daily")

            if not daily or not daily.get("time"):
                raise ValueError(f"No daily data returned for {city} on {ds}")

            hook.run(
                UPSERT_SQL,
                parameters=(
                    city,
                    lat,
                    lon,
                    daily["time"][0],
                    daily["temperature_2m_max"][0],
                    daily["temperature_2m_min"][0],
                    daily["temperature_2m_mean"][0],
                    daily["precipitation_sum"][0],
                    daily["wind_speed_10m_max"][0],
                    daily["weather_code"][0],
                ),
            )
            loaded += 1
            log.info("Loaded %s for %s", city, ds)

        log.info("Run complete: %s cities loaded for %s", loaded, ds)
        return loaded

    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT_CMD} run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT_CMD} test")

    create_raw_table() >> extract_and_load() >> dbt_run >> dbt_test


weather_raw_ingest()
