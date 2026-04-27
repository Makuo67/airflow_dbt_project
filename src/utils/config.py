MYSQL_CONFIG = {
    "host": "mysql",
    "user": "root",
    "password": "root",
    "database": "staging_db"
}

POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "analytics_db",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

RAW_DATA_PATH = "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
RAW_TABLE = "flight_prices_raw"
STAGING_TABLE = "flight_prices_staging"

# Marts transfer configuration
MARTS_SCHEMA = "marts"
MART_TABLES = [
    "dim_airline",
    "dim_route",
    "dim_season",
    "fact_flight_prices",
    "fact_kpis",
]
