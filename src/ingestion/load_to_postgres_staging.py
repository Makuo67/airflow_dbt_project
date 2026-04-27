import io
import csv
from airflow.exceptions import AirflowFailException
from src.utils.db import mysql_connection, postgres_connection
from src.utils.config import STAGING_TABLE


def load_to_postgres():
    try:
        # -----------------------------------
        # 1. Extract raw data from MySQL
        # -----------------------------------
        with mysql_connection() as (m_conn, m_cursor):
            m_cursor.execute("""
                SELECT
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
                    seasonality
                FROM flight_prices_raw
            """)
            raw_data = m_cursor.fetchall()

        if not raw_data:
            raise AirflowFailException(
                "No data found in MySQL raw table to load into PostgreSQL.")

        # -----------------------------------
        # 2. Load into PostgreSQL staging with COPY (minimal latency)
        # -----------------------------------
        with postgres_connection() as (p_conn, p_cursor):
            # Idempotent load: truncate first (inside same transaction for consistency)
            p_cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE};")

            # Build a CSV buffer in-memory for COPY FROM
            buffer = io.StringIO()
            writer = csv.writer(
                buffer,
                delimiter=',',
                lineterminator='\n',
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                escapechar='\\'
            )

            for row in raw_data:
                # Convert None to \N for COPY protocol
                cleaned = [
                    str(col) if col is not None else r'\N'
                    for col in row
                ]
                writer.writerow(cleaned)

            buffer.seek(0)
            p_cursor.copy_expert(
                f"""
                COPY {STAGING_TABLE}
                (airline, source, source_name, destination,
                 destination_name, departure_time, arrival_time,
                 duration, stops, aircraft_type, class,
                 booking_source, base_fare, tax_and_surcharge,
                 total_fare, seasonality)
                FROM STDIN WITH (FORMAT CSV, NULL '\\N');
                """,
                buffer
            )

            p_conn.commit()

        print(
            f"MySQL data successfully loaded into PostgreSQL staging table. "
            f"Rows inserted: {len(raw_data)}"
        )
        return True
    except Exception as e:
        raise AirflowFailException(
            f"Failed to load data into PostgreSQL staging: {e}")
