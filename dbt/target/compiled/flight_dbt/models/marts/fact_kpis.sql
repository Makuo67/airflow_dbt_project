 WITH route_counts AS (
    SELECT route_id,
        COUNT(*) AS route_count
    FROM "analytics_db"."public"."fact_flight_prices"
    GROUP BY route_id
)
SELECT f.airline_id,
    f.route_id,
    f.season_id,
    AVG(f.total_fare) AS avg_fare,
    COUNT(*) AS booking_count,
    COALESCE(rc.route_count, 0) AS route_count
FROM "analytics_db"."public"."fact_flight_prices" f
    LEFT JOIN route_counts rc ON f.route_id = rc.route_id
GROUP BY f.airline_id,
    f.route_id,
    f.season_id,
    rc.route_count