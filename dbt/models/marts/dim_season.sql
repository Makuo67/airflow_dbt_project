{{ config(materialized = 'table') }}
SELECT DISTINCT MD5(seasonality) AS season_id,
    seasonality
FROM {{ source('staging', 'flight_prices_staging') }}