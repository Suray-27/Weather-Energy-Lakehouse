# Weather & Energy Lakehouse: Automated Medallion Data Platform

An end-to-end data engineering project that ingests, processes, and transforms hourly weather data from the Open-Meteo REST API using Microsoft Azure, Azure Databricks, PySpark, dbt, and Apache Airflow.

The project implements a Medallion Architecture (Bronze → Silver → Gold), supporting historical weather data backfills and 7-day forecasts for Chennai, Bengaluru, and Hyderabad. The resulting datasets are structured for analytical queries and downstream reporting.

---

## Project Overview

The objective is to build an automated weather data pipeline that
demonstrates key data engineering concepts:

-   REST API-based data ingestion.
-   Parameterized pipeline execution using Azure Data Factory (ADF).
-   Raw data storage in Azure Data Lake Storage Gen2.
-   Data parsing, standardization, and deduplication using PySpark.
-   Incremental data processing using Delta Lake.
-   Dimensional modeling and aggregation using dbt.
-   End-to-end workflow orchestration using Apache Airflow.
-   Data quality validation through dbt tests.

### Data Source

**Open-Meteo REST API**

The pipeline retrieves hourly weather data, including:

-   Temperature at 2 metres.
-   Relative humidity at 2 metres.
-   Precipitation.
-   Wind speed at 10 metres.

Supported ingestion modes:

| Mode | Description |
|---|---|
| **Forecast** | Retrieves the next 7 days of forecast data.|
| **Archive** | Retrieves historical weather data for a specified date range. | 

Locations covered:

-   Chennai
-   Bengaluru
-   Hyderabad

## Architectural Overview

The platform uses a metadata-driven, decoupled design where each layer has dedicated responsibilities, state tracking, and failure recovery.

The project follows a Medallion Architecture with Apache Airflow
coordinating the end-to-end workflow.

``` mermaid
flowchart TD
    API["Open-Meteo REST API"]
    ADF["Azure Data Factory<br/>Parameterized Ingestion"]
    BRONZE[("Bronze Layer<br/>ADLS Gen2<br/>Raw JSON")]
    SILVER["Silver Layer<br/>Databricks + PySpark<br/>Parsing, Deduplication"]
    DELTA[("Silver Delta Table")]
    STAGING["dbt Staging View"]
    GOLD["Gold Layer<br/>Dimensions, Facts<br/>Daily Aggregation"]
    TESTS["dbt Data Quality Tests"]

    API --> ADF
    ADF --> BRONZE
    BRONZE --> SILVER
    SILVER --> DELTA
    DELTA --> STAGING
    STAGING --> GOLD
    GOLD --> TESTS

    AIRFLOW["Apache Airflow<br/>End-to-End Orchestration"]
    AIRFLOW -.-> ADF
    AIRFLOW -.-> SILVER
    AIRFLOW -.-> GOLD
    AIRFLOW -.-> TESTS
```

### Pipeline Execution Flow

1.  Airflow triggers the Azure Data Factory master pipeline.
2.  ADF retrieves weather data from the Open-Meteo API.
3.  Raw JSON files are stored in the Bronze layer.
4.  Databricks processes Bronze files and loads standardized records
    into the Silver Delta table.
5.  dbt builds the Gold dimensional models and daily aggregates.
6.  dbt tests validate the resulting datasets.

---

## Core Technologies & Tooling

| Domain | Technology | Purpose |
|---|---|---|
| **Cloud Infrastructure** | Microsoft Azure | Cloud infrastructure |
| **Data Ingestion** | Azure Data Factory (ADF) | Parameterized, metadata-driven REST API ingestion with automated retry & recovery |
| **Distributed Compute** | Azure Databricks, Pyspark | Data transformation and processing |
| **Storage & Table Format** | Delta Lake | Transactional data storage and MERGE operations |
| **Data Transformation** | dbt Core | SQL-based transformations and dimensional modeling |
| **Workflow Orchestration** | Apache Airflow | DAG-driven scheduling, parameter propagation and cross-platform orchestration |
| **Source Data** | Python, SQL | Pipeline development and transformations |
| **Source Data** | Open-Meteo REST API | Weather data |

---

## Repository Structure

```text
Weather-Energy-Lakehouse/
├── adf/                                    # Azure Data Factory source code & ARM configurations
│   ├── dataset/                            # Input/output dataset definitions (HTTP, ADLS, Azure SQL)
│   ├── factory/                            # Data Factory resource definition
│   ├── linkedService/                      # Linked services (Key Vault, SQL, ADLS Gen2, Open-Meteo HTTP)
│   ├── pipeline/                           # ADF pipeline JSON definitions
│   │   ├── PL_Weather_Master.json          # Master orchestration pipeline with dynamic metadata & recovery
│   │   └── PL_Weather_API_To_Bronze.json   # Parameterized worker pipeline for API extraction to Bronze
│   └── trigger/                            # Scheduled & event triggers
├── airflow/                                # Apache Airflow orchestration platform
│   └── dags/                               # Airflow DAG definitions
│       ├── weather_lakehouse_orchestration.py # Master end-to-end DAG (ADF -> Databricks -> dbt)
│       └── hello_weather_lakehouse.py      # Health-check verification DAG
├── Spark_notebook/                         # Databricks PySpark notebooks
│   └── Silver_layer/
│       └── Bronze_To_Silver_Weather.ipynb  # Incremental JSON extraction, deduplication, and Silver Delta MERGE
├── weather_energy_dbt/                     # dbt Core dimensional modeling project
│   ├── dbt_project.yml                     # dbt project configuration and paths
│   ├── macros/                             # Custom Jinja macros (schema naming)
│   └── models/
│       ├── staging/weather/                # Staging views over Silver Delta tables
│       │   ├── source.yml                  # Source schema definitions
│       │   └── stg_weather_hourly.sql      # Standardized weather view
│       ├── dimensions/                     # Kimball dimension tables
│       │   ├── dim_location.sql            # Location dimension with SHA-256 surrogate keys
│       │   ├── dim_date.sql                # Comprehensive calendar dimension (day, week, month, quarter)
│       │   └── schema.yml                  # Primary key uniqueness and non-null constraints
│       ├── facts/                          # Fact tables
│       │   ├── fct_weather_hourly.sql      # Incremental hourly weather fact table with foreign key joins
│       │   └── schema.yml                  # Referential integrity & metric data tests
│       └── aggregates/                     # Aggregated data marts
│           ├── agg_weather_daily.sql       # Incremental daily weather metrics (min/max/avg, precipitation)
│           └── schema.yml                  # Aggregation data tests
├── publish_config.json                     # ADF Git publishing configuration
└── README.md                               # Repository documentation and architecture guide
```

---

## Medallion Architecture Deep Dive

### 1. Bronze Layer -- Raw Data Ingestion

**Technology:** Azure Data Factory and Azure Data Lake Storage Gen2.

- **Metadata-Driven Pipeline**: Ingestion is governed by tables in an Azure SQL Database (`control.dataset_config`, `control.dataset_location`). Location coordinates (Chennai, Bengaluru, Hyderabad) and endpoints are read dynamically at runtime.
- **Automated Failure Recovery**: `PL_Weather_Master` queries `control.pipeline_run_history` and `control.ingestion_attempt_history`. If a prior run failed or was partial, the pipeline automatically isolates and retries only the failed locations before processing new batches.
- **Dual Ingestion Modes**: Supports both `forecast` (7-day forward predictions) and `archive` (historical date-range backfills).
- **Storage Structure**: Raw JSON responses are landed into ADLS Gen2 `bronze` container using structured partitioning:
  `weather/{api_mode}/location={location}/{api_mode}_{start_date}_to_{end_date}_{run_id}.json`
- **Immutability**: Files in Bronze are retained in raw, unmutated form to preserve full lineage and allow deterministic reprocessing.

### 2. Silver Layer -- Data Transformation

**Technology:** Azure Databricks, PySpark, Delta Lake.

- **Incremental File Detection**: Databricks scans ADLS Gen2 using REST API listings and compares each file against `workspace.default.bronze_file_processing_log`. Only new, modified (ETag/timestamp change), or previously failed files are processed.
- **Array Parsing**: Extracts multi-variable hourly arrays (`temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`) into structured relational rows.
- **Business Key Deduplication**: Applies PySpark window functions partitioned by `(location, weather_timestamp)` ordered by `source_last_modified DESC, source_file DESC` to remove duplicate records using the location and weather timestamp business key before performing Delta MERGE operations.
- **Delta MERGE Upsert**: Merges records into `workspace.default.weather_hourly_silver`, updating existing rows only when measure values differ, preserving computation cycles.
- **Per-File Audit Logging**: Updates `bronze_file_processing_log` with file-level `SUCCESS` or `FAILED` statuses and detailed error messages.

### 3. Gold Layer -- Analytical Modeling

**Technology:** dbt Core and Databricks SQL.

The Gold layer implements a Kimball Star Schema optimized for analytics and BI:
- **`dim_location`**: Materialized table containing distinct locations with deterministic SHA-256 hash keys:
  `sha2(location, 256) AS location_key`.
- **`dim_date`**: Calendar dimension dynamically generated using Spark's `SEQUENCE(MIN_DATE, MAX_DATE, INTERVAL 1 DAY)` and `EXPLODE()`, computing year, quarter, month, day of week, and weekend indicators.
- **`fct_weather_hourly`**: Incremental fact table (`merge` strategy on `['location_key', 'weather_timestamp']`). Joins staging records with dimensions and filters incrementally via:
  `WHERE s.ingested_at_utc >= (SELECT COALESCE(MAX(ingested_at_utc), '1900-01-01') FROM {{ this }})`.
- **`agg_weather_daily`**: Incremental daily aggregation mart rollup (`merge` strategy on `['location_key', 'date_key']`) computing summary statistics:
  - Total hourly records per day
  - Min, Average, and Max Temperature (°C)
  - Average Relative Humidity (%)
  - Cumulative Precipitation (mm)
  - Average and Max Wind Speed (10m)

### 4. Data Quality & Automated Testing
Tests are defined in `schema.yml` files across every model layer:
- **Primary & Surrogate Key Validation**: `unique` and `not_null` on `location_key`, `date_key`, and `date_day`.
- **Referential Integrity**: `relationships` tests ensuring every `location_key` and `date_key` in `fct_weather_hourly` and `agg_weather_daily` successfully resolves to its parent dimension.
- **Incremental Data Testing**: Configured `where` clauses to test recent ingestion windows without incurring full-table scan costs.

### 5. End-to-End Orchestration (Apache Airflow)
The Airflow DAG `weather_lakehouse_orchestration` provides automated, single-command execution:
1. `trigger_adf_pipeline`: Triggers `PL_Weather_Master` via Azure CLI passing dynamic parameters (`api_mode`, `start_date`, `end_date`).
2. `wait_for_adf_completion`: Polls ADF pipeline run status every 30 seconds until successful completion.
3. `run_bronze_to_silver_databricks`: Triggers the Databricks PySpark job via `DatabricksRunNowOperator`.
4. `dbt_run_gold`: Executes `dbt run` inside a virtual environment to build staging, dimensions, facts, and aggregate models.
5. `dbt_test_gold`: Executes `dbt test` to validate data quality before marking the pipeline run complete.

---

## Getting Started & Execution

### Prerequisites
- Azure Subscription (ADLS Gen2, Azure Data Factory, Azure SQL Database, Azure Key Vault).
- Azure Databricks Workspace with a configured cluster or Serverless SQL Warehouse.
- Apache Airflow (2.8+ or Airflow SDK) with Databricks and Azure providers installed.
- Python 3.10+ and dbt-databricks adapter.

### Running the End-to-End Pipeline

Trigger the forecast pipeline:

```bash
airflow dags trigger weather_lakehouse_orchestration \
    --conf '{"api_mode": "forecast"}'
```
Trigger a historical archive run:

```bash
airflow dags trigger weather_lakehouse_orchestration \
    --conf '{"api_mode": "archive", "start_date": "2026-10-01", "end_date": "2026-10-07"}'
```

### Running dbt Models Locally
```bash
cd weather_energy_dbt
export DBT_PROFILES_DIR=~/.dbt

# Compile SQL models and verify configuration
dbt compile

# Run models incrementally
dbt run

# Run data quality tests on recent data
dbt test --vars '{"test_lookback_hours": 24}'
```
---

## Key Data Engineering Concepts Demonstrated

This project provides practical implementation experience with:

-   REST API ingestion.
-   Parameterized ETL pipelines.
-   Cloud object storage.
-   Medallion architecture.
-   Distributed data processing using PySpark.
-   JSON array flattening.
-   Window functions and deduplication.
-   Delta Lake MERGE operations.
-   Incremental data transformations.
-   Dimensional modeling and star schemas.
-   Surrogate key generation.
-   Late-arriving data correction handling.
-   Data quality testing.
-   Workflow orchestration and failure handling.

---

## Future Enhancements

Potential extensions to the current weather pipeline include:

-   Integrating renewable energy generation datasets.
-   Combining weather and electricity consumption data.
-   Building Power BI dashboards for weather trends.
-   Implementing additional monitoring and alerting.
-   Adding automated deployment through CI/CD.
-   Introducing data cataloging and lineage management.
-   Extending the pipeline to additional cities and weather variables.

These are future enhancements and are not represented as completed
features of the current implementation.

---

## Project Scope

The current implementation focuses on building a weather data pipeline
using Azure Data Factory, ADLS Gen2, Databricks, PySpark, Delta Lake,
dbt, and Apache Airflow.

The emphasis is on practical data engineering concepts: ingestion,
transformation, incremental processing, dimensional modeling, data
quality, and orchestration.
