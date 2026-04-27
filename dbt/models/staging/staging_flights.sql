{{ config(materialized = 'table') }} WITH deduplicated AS (
    SELECT DISTINCT airline,
        source,
        source_name,
        destination,
        destination_name,
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
        seasonality
    FROM {{ source('staging', 'flight_prices_staging') }}
)
SELECT MD5(
        COALESCE(airline, '') || '|' || COALESCE(source, '') || '|' || COALESCE(destination, '') || '|' || COALESCE(CAST(departure_time AS VARCHAR), '') || '|' || COALESCE(CAST(total_fare AS VARCHAR), '')
    ) AS flight_id,
    airline,
    source,
    source_name,
    destination,
    destination_name,
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
    CURRENT_TIMESTAMP AS loaded_at
FROM deduplicated