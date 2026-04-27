{{ config(materialized = 'table') }}
SELECT DISTINCT MD5(source || '-' || destination) AS route_id,
    source,
    destination
FROM {{ source('staging', 'flight_prices_staging') }}