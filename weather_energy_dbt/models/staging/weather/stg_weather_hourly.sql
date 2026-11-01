{{ config( materialized = 'view' ,
    schema = 'staging') }}

SELECT
    location,
    weather_timestamp,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    ingested_at_utc,
    ingestion_date,
    source_file,
    source_last_modified

FROM {{ source('silver', 'weather_hourly_silver')}}
