FROM apache/airflow:2.10.5

# dbt lives in its own virtual environment so its dependencies
# cannot clash with Airflow's.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-postgres~=1.9.0"
