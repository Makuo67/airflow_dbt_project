
SELECT MD5(COALESCE(airline, '')) AS airline_id,
    MD5(
        COALESCE(source, '') || '|' || COALESCE(destination, '')
    ) AS route_id,
    MD5(COALESCE(seasonality, '')) AS season_id,
    flight_id,
    airline,
    source,
    destination,
    departure_time,
    arrival_time,
    duration,
    stops,
    aircraft_type,
    class,
    booking_source,
    base_fare,
    tax_and_surcharge,
    total_fare,
    seasonality,
    loaded_at
FROM "analytics_db"."public"."staging_flights"