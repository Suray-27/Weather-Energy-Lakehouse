
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['location_key', 'weather_timestamp'],
        schema='facts'
    )
}}

SELECT
    dl.location_key,
    dd.date_key,

    s.weather_timestamp,
    HOUR(s.weather_timestamp) AS hour_of_day,

    s.temperature_2m,
    s.relative_humidity_2m,
    s.precipitation,
    s.wind_speed_10m,

    s.ingested_at_utc,
    s.ingestion_date,
    s.source_file,
    s.source_last_modified

FROM {{ ref('stg_weather_hourly') }} s

INNER JOIN {{ ref('dim_location') }} dl
    ON s.location = dl.location

INNER JOIN {{ ref('dim_date') }} dd
    ON TO_DATE(s.weather_timestamp) = dd.date_day

{% if is_incremental() %}
WHERE s.ingested_at_utc >= (
    SELECT COALESCE(
        MAX(ingested_at_utc),
        CAST('1900-01-01' AS TIMESTAMP)
    )
    FROM {{ this }}
)
{% endif %}
