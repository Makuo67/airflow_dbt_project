# Airflow Flight Price Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.13.13-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.1-yellow.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.8-orange.svg)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Production-grade ETL pipeline** for flight price analytics using **Apache Airflow**, **MySQL** (operational staging), **PostgreSQL** (analytics warehouse), and **dbt** (analytics engineering layer). Processes CSV flight data through a validated ingestion path into a star-schema dimensional model with versioned, tested SQL transformations.

## Business Value

- **Data Quality Gate**: Multi-layer validation prevents bad data from reaching the warehouse.
- **Analytics Engineering Layer**: dbt enables version-controlled, tested, and documented transformations. SQL logic is no longer buried in Python scripts.
- **Star Schema Model**: Dimensional design (facts + dimensions) optimized for BI querying and aggregation.
- **Decoupled Architecture**: MySQL handles raw operational staging; PostgreSQL serves as the analytics warehouse; dbt owns the transformation contract.
- **Scalable**: Dockerized, Airflow-orchestrated, bulk loaders, and idempotent dbt models.

## Pipeline Architecture

```
+----------------+     +------------------+     +---------------------+
|   CSV Data     |     |   MySQL 8        |     |   PostgreSQL 16     |
|   ./data/      |---->|   staging_db     |---->|   analytics_db      |
|                |     |   flight_prices  |     |   (staging + marts) |
+----------------+     +--------+---------+     +----------+----------+
                                |                          |
                                v                          v
                       +------------------+      +------------------+
                       |  Airflow DAG     |      |   dbt Core       |
                       |  flight_price_   |----->|   flight_dbt     |
                       |  pipelinev8      |      |                  |
                       +------------------+      +--------+---------+
                                                          |
                                +-------------------------+
                                v
                       +------------------+
                       |   Star Schema    |
                       |   Marts Layer    |
                       |                  |
                       |  dim_airline     |
                       |  dim_route       |
                       |  dim_season      |
                       |  dim_date        |
                       |  fact_flight_    |
                       |    prices        |
                       |  fact_kpis       |
                       +------------------+
```

**Docker Stack** (`docker-compose.yaml`):

- **MySQL 8** (port 3306): Operational staging with `LOAD DATA LOCAL INFILE` enabled for fast bulk ingestion.
- **PostgreSQL 16** (port 5432): Analytics warehouse hosting both the raw staging table and the dbt-materialized mart tables.
- **Airflow 3.2.1** (port 8080): LocalExecutor with DAGs, source code, and dbt project mounted as volumes.
- **dbt Core**: Runs inside the Airflow container via `BashOperator`, using `/opt/airflow/dbt` as the project directory.

## Quick Start

1. **Prepare Data**: Place `Flight_Price_Dataset_of_Bangladesh.csv` in `./data/`.
2. **Spin Up Infrastructure**:

   ```bash
   docker compose up --build -d
   ```

3. **Airflow UI**: Navigate to `http://localhost:8080` and log in with the credentials defined in your `.env` (default: `airflow` / `airflow`).
4. **Trigger DAG**: Unpause and trigger `flight_price_pipelinev8`.
5. **Query Results**:
   ```sql
   -- Postgres Marts
   SELECT * FROM dim_airline LIMIT 5;
   SELECT * FROM fact_kpis ORDER BY avg_fare DESC LIMIT 5;
   ```

## Pipeline Execution Flow

**DAG**: `flight_price_pipelinev8` (manual trigger)
**Total Tasks**: 7 sequential tasks
**Default Args**: `owner='okeke'`, `retries=1`, `retry_delay=2 minutes`

| Step | Task ID                    | Type           | Module / Command                            | Purpose                                                                                                                 |
| ---- | -------------------------- | -------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 1    | `create_mysql_schema`      | PythonOperator | `src/sql/mysql_schema.py`                   | Idempotently creates the MySQL raw schema (`flight_prices_raw`).                                                        |
| 2    | `load_csv_to_mysql`        | PythonOperator | `src/ingestion/mysql_dump.py`               | Bulk loads CSV via `LOAD DATA LOCAL INFILE` into MySQL.                                                                 |
| 3    | `validate_raw_data`        | PythonOperator | `src/validation/validation.py`              | Enforces 5-layer data quality: schema, nulls, negatives, categoricals, and referential sanity. Fails fast on violation. |
| 4    | `create_postgres_schema`   | PythonOperator | `src/sql/postgres_staging_schema.py`        | Idempotently creates the PostgreSQL staging table (`flight_prices_staging`) with performance indexes.                   |
| 5    | `load_to_postgres_staging` | PythonOperator | `src/ingestion/load_to_postgres_staging.py` | Extracts from MySQL, then loads into Postgres staging via the `COPY FROM` protocol (truncate-insert for idempotency).   |
| 6    | `dbt_run`                  | BashOperator   | `dbt run --project-dir /opt/airflow/dbt`    | Executes the full dbt DAG: `staging_flights` → dimensions → `fact_flight_prices` → `fact_kpis`.                         |
| 7    | `dbt_test`                 | BashOperator   | `dbt test --project-dir /opt/airflow/dbt`   | Runs the dbt test suite: uniqueness, not-null, referential integrity, and accepted-range assertions on all marts.       |

**Full Flow**:
`MySQL Schema` → `Bulk CSV Load` → `DQ Validation` → `Postgres Staging Schema` → `COPY to Staging` → `dbt Transformations` → `dbt Tests`

## Dimensional Model (Star Schema)

The analytics layer is implemented as a classic star schema. All marts are materialized as tables via dbt.

### Surrogate Key Strategy

All dimensional and fact tables use deterministic MD5 surrogate keys generated from business-key concatenations. This ensures idempotency and reproducibility across runs.

### Dimension Tables

| Table         | Source                  | Grain                                | Key Logic                                |
| ------------- | ----------------------- | ------------------------------------ | ---------------------------------------- |
| `dim_airline` | `flight_prices_staging` | One row per airline                  | `MD5(airline)`                           |
| `dim_route`   | `flight_prices_staging` | One row per source-destination pair  | `MD5(source \|\| '\|' \|\| destination)` |
| `dim_season`  | `flight_prices_staging` | One row per seasonality value        | `MD5(seasonality)`                       |
| `dim_date`    | Generated               | One row per calendar day (2010–2035) | `date_day` (native date)                 |

**`dim_date` Implementation**: Uses `dbt_utils.date_spine` to generate a 25-year date spine, then extracts `year`, `month`, `day`, `quarter`, `day_name`, and `is_weekend` flags. This eliminates external date-dimension dependencies.

### Fact Tables

| Table                | Grain                                        | Measures                                                            | Dimensions                                                 |
| -------------------- | -------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------- |
| `fact_flight_prices` | One row per unique flight record             | `base_fare`, `tax_and_surcharge`, `total_fare`, `duration`, `stops` | `airline_id`, `route_id`, `season_id`, `departure_date_id` |
| `fact_kpis`          | One row per airline-route-season combination | `avg_fare`, `booking_count`, `route_count`                          | `airline_id`, `route_id`, `season_id`                      |

**`fact_flight_prices`**: References `staging_flights` and links to all four dimensions via surrogate keys. The `flight_id` is a deterministic MD5 hash of the natural key (`airline`, `source`, `destination`, `departure_time`, `total_fare`).

**`fact_kpis`**: Aggregates from `fact_flight_prices` using a CTE to compute route-level counts, then joins back to produce airline-route-season level metrics.

## KPI Definitions and Computation Logic

KPIs are not computed in MySQL. All business logic is centralized in dbt models under `dbt/models/marts/`.

| KPI                 | dbt Model                 | SQL Logic                                                  | Business Use                                  |
| ------------------- | ------------------------- | ---------------------------------------------------------- | --------------------------------------------- |
| **Average Fare**    | `fact_kpis.avg_fare`      | `AVG(total_fare) GROUP BY airline_id, route_id, season_id` | Pricing competitiveness and yield management. |
| **Booking Count**   | `fact_kpis.booking_count` | `COUNT(*) GROUP BY airline_id, route_id, season_id`        | Volume and market share analysis.             |
| **Route Frequency** | `fact_kpis.route_count`   | `COUNT(*) GROUP BY route_id` (via CTE join)                | Capacity planning and route profitability.    |

**Computation Flow**:

1. `staging_flights` deduplicates the raw staging table and assigns a deterministic `flight_id` plus a `loaded_at` audit timestamp.
2. `fact_flight_prices` builds the atomic fact table with all foreign keys and financial measures.
3. `fact_kpis` performs the final aggregation via a CTE (`route_counts`) that computes `COUNT(*)` per `route_id`, then joins it back to the main aggregation to produce `avg_fare`, `booking_count`, and `route_count`.

This design decouples atomic facts from aggregated metrics, enabling downstream consumers to drill from summary KPIs to individual flight records.

## dbt Tests and Data Quality

Data quality is enforced at two levels: **Python validation during ingestion** (Airflow tasks 1–5) and **dbt assertions after transformation** (Airflow task 7). The dbt test suite is defined in `dbt/models/marts/schema.yml` and executed via `dbt test` as the final pipeline step. A failure here prevents the pipeline from marking the run as successful.

### Test Coverage

| Model                | Column              | Test Type                                   | Purpose                                                           |
| -------------------- | ------------------- | ------------------------------------------- | ----------------------------------------------------------------- |
| `dim_airline`        | `airline_id`        | `unique`, `not_null`                        | Surrogate key integrity.                                          |
| `dim_airline`        | `airline`           | `not_null`                                  | Every record must have an airline name.                           |
| `dim_route`          | `route_id`          | `unique`, `not_null`                        | Surrogate key integrity.                                          |
| `dim_season`         | `season_id`         | `unique`, `not_null`                        | Surrogate key integrity.                                          |
| `dim_season`         | `seasonality`       | `not_null`                                  | Every record must have a season value.                            |
| `dim_date`           | `date_id`           | `unique`, `not_null`                        | Date spine completeness.                                          |
| `fact_flight_prices` | `flight_id`         | `not_null`, `unique`                        | Natural key integrity at the grain of one flight.                 |
| `fact_flight_prices` | `airline_id`        | `not_null`, `relationships` → `dim_airline` | Referential integrity: every flight must link to a valid airline. |
| `fact_flight_prices` | `route_id`          | `not_null`, `relationships` → `dim_route`   | Referential integrity: every flight must link to a valid route.   |
| `fact_flight_prices` | `season_id`         | `not_null`, `relationships` → `dim_season`  | Referential integrity: every flight must link to a valid season.  |
| `fact_flight_prices` | `departure_date_id` | `not_null`, `relationships` → `dim_date`    | Referential integrity: every flight must link to a valid date.    |
| `fact_flight_prices` | `total_fare`        | `not_null`, `accepted_range` (min: 0)       | Business rule: fares must be non-negative.                        |

### dbt Packages

The project depends on `dbt-labs/dbt_utils` (v1.3.3), declared in `dbt/packages.yml`. This package provides:

- `dbt_utils.date_spine`: Used in `dim_date` to generate the calendar dimension without external dependencies.
- `dbt_utils.unique_combination_of_columns`: Used to assert composite uniqueness on `fact_flight_prices`.
- `dbt_utils.accepted_range`: Used to enforce the `total_fare >= 0` constraint on `fact_flight_prices`.

Install packages before the first dbt run:

```bash
dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

## Challenges and Engineering Solutions

| Challenge                              | Impact                                                                                                             | Solution                                                                                                                                                                                                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **MySQL-to-Postgres Data Transfer**    | Pandas-based loads introduced high memory overhead and slow inserts for large CSVs.                                | Extract from MySQL via `cursor.fetchall()`, then use PostgreSQL's native `COPY FROM` protocol with an in-memory CSV buffer. Achieved idempotency via `TRUNCATE` followed by `COPY` inside the same transaction.                                                                 |
| **Data Quality at the Edge**           | Garbage data (negative fares, invalid airlines, null critical fields) would corrupt downstream KPIs.               | Implemented a 5-layer validation module (`src/validation/validation.py`) that asserts schema completeness, missing values, NULL constraints, negative numeric guards, and categorical domain checks. The DAG fails fast before any warehouse load.                              |
| **Idempotency Across Runs**            | Re-triggering the DAG produced duplicate records in both MySQL and Postgres.                                       | MySQL schema uses `IF NOT EXISTS`. Postgres staging uses `TRUNCATE + COPY`. dbt models are materialized as `table`, which executes `DROP + CREATE` on every run, guaranteeing a clean state.                                                                                    |
| **dbt-Postgres Integration in Docker** | dbt could not resolve the `postgres` host or locate the project profile when executed from Airflow's BashOperator. | Centralized connection config in `dbt/profiles.yml` pointing to `host: postgres`. Mounted the dbt project into the Airflow container at `/opt/airflow/dbt`. The BashOperator explicitly passes `--project-dir` and `--profiles-dir` to dbt commands.                            |
| **Surrogate Key Generation**           | Auto-increment keys in Postgres are non-deterministic and break idempotency across environments.                   | Adopted deterministic MD5 hashing in SQL for all surrogate keys. This guarantees the same `airline_id`, `route_id`, and `flight_id` on every run, enabling clean incremental comparisons and idempotent rebuilds.                                                               |
| **Date Dimension Maintenance**         | Hard-coded date tables require manual updates and external ETL.                                                    | Implemented `dim_date` using `dbt_utils.date_spine`, generating a 25-year calendar spine (2010–2035) natively in SQL. Extracts `year`, `month`, `day`, `quarter`, `day_name`, and `is_weekend` without external dependencies.                                                   |
| **Two-Phase Compute Complexity**       | Computing KPIs directly in MySQL coupled transformation logic to the operational database.                         | Separated concerns: MySQL owns raw ingestion; Postgres owns staging; dbt owns all transformation and business logic. This follows the ELT pattern and allows analytics engineers to iterate on SQL without touching Python or operational databases.                            |
| **Post-Transform Data Quality**        | dbt could materialize models with orphaned keys or negative fares silently.                                        | Added a dedicated `dbt_test` task in the Airflow DAG (`flight_price_pipelinev8`) that runs after `dbt_run`. Tests enforce uniqueness, not-null constraints, referential integrity across all dimension-FK relationships, and accepted-value ranges directly on the mart tables. |

## Tech Stack

- **Orchestration**: Apache Airflow 3.2.1 (LocalExecutor, DAGs mounted via Docker volume)
- **Operational Staging**: MySQL 8 (bulk `LOAD DATA LOCAL INFILE`)
- **Analytics Warehouse**: PostgreSQL 16 (dimensional star schema)
- **Analytics Engineering**: dbt Core (versioned SQL models, `ref()` and `source()` contracts, `dbt_utils` macros)
- **Data Quality**: Custom Python validation with AirflowFailException hard-stops; dbt tests for post-transform assertions
- **Ingestion Patterns**: `COPY FROM` protocol for high-throughput Postgres loads
- **Modeling Patterns**: Star schema, deterministic surrogate keys, idempotent table materializations
- **Infrastructure**: Docker Compose with healthchecks and service dependencies

<!-- ## Future Enhancements

- **Incremental Models**: Convert `fact_flight_prices` to an incremental model to support daily append-only loads without full table rebuilds.
- **dbt Snapshots**: Track slowly changing dimensions (e.g., airline rebranding) using dbt snapshots on `dim_airline`.
- **XCom Instrumentation**: Publish row counts and validation metrics from Python tasks into Airflow XCom for observability.
- **S3 Source**: Replace local CSV with an S3 `S3KeySensor` trigger for cloud-native ingestion.
- **dbt Docs**: Generate and serve dbt documentation via `dbt docs generate` and `dbt docs serve` inside the Docker network. -->

## Project Structure

```
airflow_dbt_project/
├── dags/
│   └── flight_price_pipeline.py      # Airflow DAG
├── dbt/
│   ├── dbt_project.yml               # dbt project
│   ├── profiles.yml                  # Postgres connection
│   ├── packages.yml                  # dbt package
│   └── models/
│       ├── staging/
│       │   ├── sources.yml           # Staging source
│       │   └── staging_flights.sql   # Deduplicated staging
│       └── marts/
│           ├── schema.yml            # dbt tests and model
│           ├── dim_airline.sql       # Airline dimension
│           ├── dim_route.sql         # Route dimension
│           ├── dim_season.sql        # Season dimension
│           ├── dim_date.sql          # Date dimension
│           ├── fact_flight_prices.sql # Atomic fact table
│           └── fact_kpis.sql         # Aggregated KPIs
├── src/
│   ├── ingestion/
│   │   ├── mysql_dump.py             # CSV → MySQL bulk
│   │   └── load_to_postgres_staging.py # MySQL → Postgres
│   ├── sql/
│   │   ├── mysql_schema.py           # MySQL DDL
│   │   └── postgres_staging_schema.py # Postgres staging DDL
│   ├── validation/
│   │   └── validation.py             # 5-layer DQ checks
│   └── utils/
│       ├── config.py                 # Environment
│       └── db.py                     # Connection context
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv  # Source
├── docker-compose.yaml               # Multi-service Docker
├── Dockerfile                        # Custom Airflow image
└── requirements.txt                  # Python + dbt
```

**Questions or Contributions?** Trigger the DAG and inspect the `analytics_db` marts directly.

---

_Author: Okeke Makuochukwu_
