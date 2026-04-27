
SELECT DISTINCT MD5(seasonality) AS season_id,
    seasonality
FROM "analytics_db"."public"."flight_prices_staging"