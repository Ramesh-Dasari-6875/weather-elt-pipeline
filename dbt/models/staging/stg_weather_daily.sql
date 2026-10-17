-- Staging: rename, tidy and enrich the raw data. One row per city per day.
select
    city || '|' || weather_date::text as weather_key,
    city,
    latitude,
    longitude,
    weather_date,
    temp_max_c,
    temp_min_c,
    temp_mean_c,
    precipitation_mm,
    wind_speed_max_kmh,
    weather_code,
    case
        when weather_code = 0 then 'Clear sky'
        when weather_code = 1 then 'Mainly clear'
        when weather_code = 2 then 'Partly cloudy'
        when weather_code = 3 then 'Overcast'
        when weather_code in (45, 48) then 'Fog'
        when weather_code in (51, 53, 55) then 'Drizzle'
        when weather_code in (56, 57) then 'Freezing drizzle'
        when weather_code in (61, 63, 65) then 'Rain'
        when weather_code in (66, 67) then 'Freezing rain'
        when weather_code in (71, 73, 75, 77) then 'Snow'
        when weather_code in (80, 81, 82) then 'Rain showers'
        when weather_code in (85, 86) then 'Snow showers'
        when weather_code in (95, 96, 99) then 'Thunderstorm'
        else 'Unknown'
    end as weather_description,
    loaded_at
from {{ source('raw', 'weather_daily') }}
