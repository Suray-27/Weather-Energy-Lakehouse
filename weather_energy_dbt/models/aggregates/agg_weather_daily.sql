{{ config(
    materialized='table',
    schema='aggregates'
) }}

SELECT
    location_key,
    date_key,

    COUNT(*) AS total_hourly_records,

    ROUND(AVG(temperature_2m), 2) AS avg_temperature,
    ROUND(MIN(temperature_2m), 2) AS min_temperature,
    ROUND(MAX(temperature_2m), 2) AS max_temperature,

    ROUND(AVG(relative_humidity_2m), 2) AS avg_relative_humidity,

    ROUND(SUM(precipitation), 2) AS total_precipitation,

    ROUND(AVG(wind_speed_10m), 2) AS avg_wind_speed,
    ROUND(MAX(wind_speed_10m), 2) AS max_wind_speed

FROM {{ ref('fct_weather_hourly') }}

GROUP BY
    location_key,
    date_key