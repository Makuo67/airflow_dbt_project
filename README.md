# Airflow Flight Price Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-yellow.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Production-grade ETL pipeline** for flight price analytics using **Apache Airflow**, **MySQL** (staging), **PostgreSQL** (data warehouse). Processes CSV flight data → validates → computes KPIs → loads dimensional models.

## Business Value

- **Data Quality Gate**: Multi-layer validation prevents bad data downstream.
- **KPI Layer**: 4 core metrics for airline/route/seasonal analysis.
- **Decoupled Architecture**: Staging (MySQL) → Analytics (Postgres).
- **Scalable**: Dockerized, Airflow-orchestrated, bulk loaders.

## Pipeline Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   CSV Data      │───▶│  MySQL       │──▶│  PostgreSQL     │
│   ./data/       │    │ Staging DB   │    │ Data Warehouse  │
└──────────┬──────┘    │              │    │                 │
           │           │ flight_prices│    │ dim_*_kpis      │
           ▼           │ kpi_* tables │    │ fact_flight_*   │
┌─────────────────┐    └──────┬───────┘    └─────────────────┘
│   Airflow DAG   │           │
│ flight_price_   │◀──────────┘
│ pipelinev5      │
└─────────────────┘
```

**3-Tier Docker Stack** (`docker-compose.yml`):

- **MySQL 8** (3306): Staging + temp KPI computation (`--local-infile=1`).
- **Postgres 14** (5432): Analytics DB with dimensional KPI tables.
- **Airflow** (8080): LocalExecutor, custom src/dags mounted.

## Quick Start

1. **Prepare Data**: Place `flight_prices.csv` in `./data/`
2. **Spin Up**:
   - `docker compose up -d postgres mysql`
   - `docker compose run airflow airflow db init`
   - ` docker compose run airflow airflow users create   --username admin   --password admin   --firstname admin   --lastname admin   --role Admin   --email example@email.com`
   - `docker compose up -d airflow`
   -
3. **Airflow UI**: http://localhost:8080 (admin/admin)
4. **Trigger DAG**: `flight_price_pipelinev5`
5. **Query Results**:
   ```sql
   -- Postgres KPIs
   SELECT * FROM dim_airline_kpis ORDER BY avg_fare DESC LIMIT 5;
   SELECT * FROM dim_popular_routes ORDER BY route_count DESC;
   ```

## Pipeline Execution Flow

**DAG**: `flight_price_pipelinev5` (manual trigger)
**Total Tasks**: 6 sequential PythonOperators
**Default Args**: owner='okeke', retries=1

| Step | Task ID                  | Module                                 | Duration (est.) | Dependencies |
| ---- | ------------------------ | -------------------------------------- | --------------- | ------------ |
| 1    | `create_mysql_schema`    | `src/sql/mysql_schema.py`              | 2s              | -            |
| 2    | `load_csv_to_mysql`      | `src/ingestion/mysql_dump.py`          | 10s             | Step 1       |
| 3    | `validate_raw_data`      | `src/validation/validation.py`         | 5s              | Step 2       |
| 4    | `transform_kpis`         | `src/transformation/transform_kpis.py` | 15s             | Step 3       |
| 5    | `create_postgres_schema` | `src/sql/postgres_schema.py`           | 2s              | Step 4       |
| 6    | `load_to_postgres`       | `src/ingestion/load_to_postgres.py`    | 10s             | Step 5       |

**Full Flow**: Schema → Bulk CSV Load → DQ Validation → KPI Compute (MySQL) → Postgres Schema → KPI Materialize (Postgres)

## KPI Definitions & Computation Logic

| KPI                          | Table (MySQL → Postgres)                                       | SQL Logic                                                  | Business Use            |
| ---------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------- | ----------------------- |
| **Avg Fare by Airline**      | `kpi_avg_fare_by_airline` → `dim_airline_kpis.avg_fare`        | `AVG(total_fare) GROUP BY airline`                         | Pricing competitiveness |
| **Booking Count by Airline** | `kpi_booking_count_airline` → `dim_airline_kpis.booking_count` | `COUNT(*) GROUP BY airline`                                | Market share            |
| **Avg Seasonal Fare**        | `kpi_seasonal_fare` → `dim_seasonality_kpis.avg_fare`          | `AVG(total_fare) GROUP BY seasonality`                     | Dynamic pricing         |
| **Popular Routes**           | `kpi_popular_routes` → `dim_route_kpis.route_count`            | `COUNT(*) GROUP BY source,destination ORDER BY COUNT DESC` | Capacity planning       |

**Key Schema**:

```sql
-- MySQL Raw (17 cols)
flight_prices_raw (airline, source, total_fare, seasonality, ...)

-- Postgres Dims (idempotent truncate-insert)
dim_airline_kpis (airline, avg_fare, booking_count)
```

## Challenges & Engineering Solutions

| Challenge              | Impact               | Solution                                                                                     |
| ---------------------- | -------------------- | -------------------------------------------------------------------------------------------- |
| **Bulk CSV Loading**   | Slow/failed inserts  | MySQL `LOAD DATA LOCAL INFILE` (native, 10x faster than pandas). Enabled `--local-infile=1`. |
| **Data Quality**       | Garbage KPIs         | 5-layer validation: cols/missing/NULLs/negatives/invalid-categoricals. Fail-fast task.       |
| **Idempotency**        | Dupe data on retries | `DELETE` + `INSERT` pattern for all KPI tables. Schema `IF NOT EXISTS`.                      |
| **Two-Phase Compute**  | Complex staging      | MySQL for fast agg (staging), Postgres for analytics (star schema).                          |
| **Docker Volume Sync** | Missing CSV/DB state | `./data:/data`, `./src:/opt/airflow/src` mounts. Airflow PYTHONPATH.                         |
| **Connection Mgmt**    | Leaks on failures    | Context managers (`@contextmanager`) for MySQLdb/psycopg2.                                   |

**Future Enhancements**:

- XComs for validation metrics.
- Branching DAG for incremental loads.
- `@daily` scheduler + S3 source.
- Load raw to `fact_flight_prices`.

## Tech Stack & Best Practices

- **Orchestration**: Airflow 2.10 (LocalExecutor)
- **Staging**: MySQL 8 (bulk ops)
- **Warehouse**: Postgres 14 (dimensional)
- **Validation**: Custom DQ rules
- **Patterns**: Idempotent tasks, context managers, bulk ops

**Questions?** Trigger the DAG and explore `analytics_db`!

---

_Author:Okeke Makuochukwu_
