# Data Warehouse Medallion V2

A clean implementation of a data warehouse using the Medallion Architecture (Bronze, Silver, Gold) with PostgreSQL, Apache Airflow for orchestration, and Docker for local development.

## Architecture

- **Bronze Layer**: Raw data ingestion from CSV sources.
- **Silver Layer**: Cleansed, deduplicated, and transformed data.
- **Gold Layer**: Business-ready views (dimensions and facts) for analytics.

## Technologies

- PostgreSQL 15
- Apache Airflow 2.8.3
- Docker & Docker Compose

## Setup

1. Clone or copy this project.
2. Ensure Docker and Docker Compose are installed.
3. Run `docker-compose -f docker/docker-compose.yml up --build`
4. Access Airflow UI at http://localhost:8080 (admin/admin)
5. Trigger the `medallion_pipeline` DAG manually.

## Data Flow

1. DDL creation for schemas and tables/views.
2. Load CSV data into Bronze tables.
3. Transform and load into Silver tables.
4. Run quality checks on Silver and Gold layers, exporting results to `artifacts/`.

## Artifacts

Quality check results are saved as CSV files in the `artifacts/` directory:
- Silver layer checks: duplicates, invalid values, etc.
- Gold layer checks: key uniqueness, referential integrity.

## Project Structure

- `datasets/`: Source CSV files.
- `migrations/postgres/`: SQL scripts for DDL and loads.
- `tests/`: Quality check SQL scripts.
- `dags/`: Airflow DAG definitions.
- `docker/`: Docker Compose configuration.
- `docs/`: Documentation.
- `artifacts/`: Output from quality checks.