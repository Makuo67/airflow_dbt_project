from airflow.exceptions import AirflowFailException
from src.utils.db import mysql_connection
from src.utils.config import RAW_TABLE

REQUIRED_COLUMNS = [
    "airline",
    "source",
    "destination",
    "base_fare",
    "tax_and_surcharge",
    "total_fare"
]


def validate_raw_data():
    validation_errors = []

    with mysql_connection() as (conn, cursor):
        # -------------------------------
        # 1. Check required columns exist
        # -------------------------------
        cursor.execute(f"SHOW COLUMNS FROM {RAW_TABLE}")
        existing_cols = {row[0] for row in cursor.fetchall()}

        missing_cols = set(REQUIRED_COLUMNS) - existing_cols
        if missing_cols:
            validation_errors.append(
                f"Missing required columns: {missing_cols}")

        # -------------------------------
        # 2. Count NULL / empty values
        # -------------------------------
        for col in REQUIRED_COLUMNS:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {RAW_TABLE}
                WHERE {col} IS NULL OR {col} = ''
            """)
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                validation_errors.append(
                    f"Column `{col}` contains {null_count} NULL/empty values.")

        # -------------------------------
        # 3. Validate numeric columns
        # -------------------------------
        numeric_cols = ["base_fare", "tax_and_surcharge", "total_fare"]

        for col in numeric_cols:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {RAW_TABLE}
                WHERE {col} < 0
            """)
            negative_count = cursor.fetchone()[0]
            if negative_count > 0:
                validation_errors.append(
                    f"Column `{col}` contains {negative_count} negative values.")

            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {RAW_TABLE}
                WHERE {col} = 0 OR {col} IS NULL
            """)
            zero_count = cursor.fetchone()[0]
            if zero_count > 0:
                validation_errors.append(
                    f"Column `{col}` contains {zero_count} zero/NULL values.")

        # -------------------------------
        # 4. Validate categorical columns
        # -------------------------------
        cursor.execute(f"""
            SELECT COUNT(*) FROM {RAW_TABLE}
            WHERE airline IS NULL OR airline = '' OR airline REGEXP '^[0-9]+$'
        """)
        bad_airlines = cursor.fetchone()[0]
        if bad_airlines > 0:
            validation_errors.append(
                f"`airline` contains {bad_airlines} invalid values.")

        # -------------------------------
        # 5. Check for duplicate rows
        # -------------------------------
        cursor.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT
                airline, source, destination, departure_time,
                arrival_time, total_fare, seasonality
            ) FROM {RAW_TABLE}
        """)
        dupes = cursor.fetchone()[0]
        if dupes and dupes > 0:
            validation_errors.append(
                f"Table contains {dupes} potential duplicate rows.")

    # -------------------------------
    # 6. Final Result
    # -------------------------------
    if validation_errors:
        error_msg = "\n DATA VALIDATION FAILED WITH THE FOLLOWING ISSUES:\n" + \
                    "\n".join(f" - {err}" for err in validation_errors)
        raise AirflowFailException(error_msg)

    print("Data validation passed with no issues.")
    return True
