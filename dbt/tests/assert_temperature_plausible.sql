-- Singular test: returns the rows that break the rule. The test passes when no rows come back.
select *
from {{ ref('stg_weather_daily') }}
where temp_mean_c < -10
   or temp_mean_c > 55
   or temp_max_c < temp_min_c
