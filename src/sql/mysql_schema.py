from src.utils.db import mysql_connection


def create_mysql_schema():
    schema_statements = [
        """
        CREATE TABLE IF NOT EXISTS flight_prices_raw (
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
            INDEX idx_airline (airline),
            INDEX idx_seasonality (seasonality),
            INDEX idx_source_dest (source, destination)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
            airline VARCHAR(100) PRIMARY KEY,
            avg_fare DECIMAL(12, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_seasonal_fare (
            seasonality VARCHAR(20) PRIMARY KEY,
            avg_fare DECIMAL(12, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_booking_count_airline (
            airline VARCHAR(100) PRIMARY KEY,
            booking_count INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kpi_popular_routes (
            source VARCHAR(5),
            destination VARCHAR(5),
            route_count INT,
            PRIMARY KEY (source, destination)
        );
        """
    ]

    with mysql_connection() as (conn, cursor):
        for statement in schema_statements:
            cursor.execute(statement)
        conn.commit()

    print("MySQL schema and KPI tables created successfully")
