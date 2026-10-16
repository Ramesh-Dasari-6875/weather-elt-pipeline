-- Runs once, the first time the warehouse container is created.
-- raw      : data exactly as loaded from the API
-- staging  : cleaned and typed (dbt)
-- marts    : analytics-ready tables (dbt)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
