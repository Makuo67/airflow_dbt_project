
SELECT DISTINCT MD5(airline) AS airline_id,
    airline
FROM "analytics_db"."public"."flight_prices_staging"