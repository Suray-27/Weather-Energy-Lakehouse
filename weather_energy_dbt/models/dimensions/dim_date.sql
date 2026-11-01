{{ config(
    materialized='table',
    schema='dimensions'
) }}

WITH date_range AS (
    SELECT
        EXPLODE(
            SEQUENCE(
                TO_DATE(MIN(weather_timestamp)),
                TO_DATE(MAX(weather_timestamp)),
                INTERVAL 1 DAY
            )
        ) AS date_day
    FROM {{ ref('stg_weather_hourly') }}
)

SELECT
    CAST(DATE_FORMAT(date_day, 'yyyyMMdd') AS INT) AS date_key,
    date_day,
    YEAR(date_day) AS year,
    QUARTER(date_day) AS quarter,
    MONTH(date_day) AS month,
    DATE_FORMAT(date_day, 'MMMM') AS month_name,
    WEEKOFYEAR(date_day) AS week_of_year,
    DAYOFMONTH(date_day) AS day_of_month,
    DAYOFWEEK(date_day) AS day_of_week,
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    CASE
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_range