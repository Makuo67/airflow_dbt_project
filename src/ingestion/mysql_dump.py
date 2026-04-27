from airflow.exceptions import AirflowFailException
from src.utils.db import mysql_connection
from src.utils.config import RAW_DATA_PATH, RAW_TABLE


def load_csv_to_mysql():
    try:
        with mysql_connection() as (conn, cursor):
            # Idempotency: clear existing raw data before reload
            cursor.execute(f"TRUNCATE TABLE {RAW_TABLE};")

            load_sql = f"""
                LOAD DATA LOCAL INFILE %s
                INTO TABLE {RAW_TABLE}
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS;
            """
            cursor.execute(load_sql, (RAW_DATA_PATH,))
            conn.commit()

        print("Raw CSV successfully loaded into MySQL using native bulk loader.")
    except Exception as e:
        raise AirflowFailException(f"Failed to load CSV into MySQL: {e}")
