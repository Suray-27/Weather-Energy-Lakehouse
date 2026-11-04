
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['location_key', 'date_key'],
        schema='aggregates',
        on_schema_change='append_new_columns'
    )
}}

WITH affected_dates AS (

    SELECT DISTINCT
        location_key,
        date_key

    FROM {{ ref('fct_weather_hourly') }}

    {% if is_incremental() %}

    WHERE ingested_at_utc >= (
        SELECT COALESCE(
            MAX(source_max_ingested_at),
            CAST('1900-01-01' AS TIMESTAMP)
        )
        FROM {{ this }}
    )

    {% endif %}
),

daily_aggregates AS (

    SELECT
        f.location_key,
        f.date_key,

        COUNT(*) AS total_hourly_records,

        ROUND(AVG(f.temperature_2m), 2) AS avg_temperature,
        ROUND(MIN(f.temperature_2m), 2) AS min_temperature,
        ROUND(MAX(f.temperature_2m), 2) AS max_temperature,

        ROUND(AVG(f.relative_humidity_2m), 2)
            AS avg_relative_humidity,

        ROUND(SUM(f.precipitation), 2) AS total_precipitation,

        ROUND(AVG(f.wind_speed_10m), 2) AS avg_wind_speed,
        ROUND(MAX(f.wind_speed_10m), 2) AS max_wind_speed,

        MAX(f.ingested_at_utc) AS source_max_ingested_at

    FROM {{ ref('fct_weather_hourly') }} f

    INNER JOIN affected_dates a
        ON f.location_key = a.location_key
        AND f.date_key = a.date_key

    GROUP BY
        f.location_key,
        f.date_key
)

SELECT *
FROM daily_aggregates
