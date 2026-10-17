-- Mart: one analytics-ready row per city per day.
select
    weather_key,
    city,
    weather_date,
    temp_max_c,
    temp_min_c,
    temp_mean_c,
    round((temp_max_c - temp_min_c)::numeric, 1) as temp_range_c,
    precipitation_mm,
    (precipitation_mm >= 1.0) as is_rainy_day,
    wind_speed_max_kmh,
    weather_description
from {{ ref('stg_weather_daily') }}
