{{ config(materialized = 'table') }}
SELECT DISTINCT MD5(airline) AS airline_id,
    airline
FROM {{ source('staging', 'flight_prices_staging') }}