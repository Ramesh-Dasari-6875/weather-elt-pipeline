-- Mart: one row per city, summarising everything loaded so far.
select
    city,
    count(*)                                      as days_recorded,
    min(weather_date)                             as first_date,
    max(weather_date)                             as last_date,
    round(avg(temp_mean_c)::numeric, 1)           as avg_temp_c,
    max(temp_max_c)                               as hottest_c,
    min(temp_min_c)                               as coldest_c,
    round(sum(precipitation_mm)::numeric, 1)      as total_precipitation_mm,
    sum(case when is_rainy_day then 1 else 0 end) as rainy_days,
    round(avg(wind_speed_max_kmh)::numeric, 1)    as avg_max_wind_kmh
from {{ ref('fct_weather_daily') }}
group by city
