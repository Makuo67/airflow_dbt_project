from src.utils.db import postgres_connection


def create_postgres_schema():
    sql = """
        CREATE TABLE IF NOT EXISTS flight_prices_staging (
            id SERIAL PRIMARY KEY,
            airline VARCHAR(100),
            source VARCHAR(5),
            source_name VARCHAR(100),
            destination VARCHAR(5),
            destination_name VARCHAR(100),
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            duration FLOAT,
            stops VARCHAR(20),
            aircraft_type VARCHAR(50),
            class VARCHAR(20),
            booking_source VARCHAR(20),
            base_fare FLOAT,
            tax_and_surcharge FLOAT,
            total_fare FLOAT,
            seasonality VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_stg_airline ON flight_prices_staging(airline);
        CREATE INDEX IF NOT EXISTS idx_stg_seasonality ON flight_prices_staging(seasonality);
        CREATE INDEX IF NOT EXISTS idx_stg_route ON flight_prices_staging(source, destination);
        """
    with postgres_connection() as (conn, cursor):
        cursor.execute(sql)
        conn.commit()
        print("PostgreSQL staging schema created successfully")
