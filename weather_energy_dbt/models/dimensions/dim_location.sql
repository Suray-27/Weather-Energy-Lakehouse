{{ config( 
    materialized = 'table',
    schema = 'dimensions' 
 ) }}

SELECT DISTINCT 
    sha2(location, 256) AS location_key,
    location
FROM {{ ref('stg_weather_hourly') }}
WHERE location IS NOT NULL