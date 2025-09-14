# NYC Taxi Data Lakehouse 🚖📊

This project demonstrates a real-world, end-to-end data engineering pipeline built with Dockerized open-source tools. The pipeline ingests NYC Yellow Taxi trip data, transforms it with dbt, orchestrates workflows with Airflow, and serves analytics via Metabase and Power BI, all running locally with Docker Compose.

## ✨ Key Features

- Containerized stack with Docker: Postgres, Airflow, and Metabase run in isolated containers via Docker Compose.

- Orchestration: Apache Airflow DAG automates ingestion, loading, and transformation.

- Data Warehouse: PostgreSQL stores raw, staging, and marts layers.

- Transformations: dbt models build a clean staging layer and analytical fact tables.

- Analytics:

  - Metabase dashboards for quick exploration.

  - Power BI views for professional visualization.

  - Schema Design: Clear separation into staging, marts (and optional silver) schemas.

  - Reproducibility: Infrastructure can be spun up or torn down with a single docker compose command.

## 🏗️ Architecture Diagram

flowchart LR
  A[NYC Taxi Data (Parquet)] -->|Airflow DAG download| B[Postgres: staging.yellow_trips]
  B -->|dbt stg_trips| C[staging.stg_trips]
  C -->|dbt fct_trip_metrics| D[marts.fct_trip_metrics]
  D -->|Metabase/Power BI| E[Dashboards & Reports]

  <img width="2538" height="1263" alt="image" src="https://github.com/user-attachments/assets/d0789447-da19-47a4-9b58-5d2487b4934b" />


## 🛠️ Tech Stack

- Docker & Docker Compose: Containerized local development.
- Airflow: Workflow orchestration (nyc_yellow_to_postgres DAG).
- Postgres: Data warehouse for raw + modeled layers.
- dbt: SQL transformations (stg_trips, fct_trip_metrics).
- Metabase: Lightweight BI for exploration.
- Power BI: Enterprise-ready dashboarding.
- Python: Ingestion logic and operator code.

## 🚀 How It Works

### 1. Docker Compose spins up Postgres, Airflow, and Metabase in containers.
  ```bash
  docker compose up -d
  ```

### 2. Airflow DAG (nyc_yellow_to_postgres):

  - Downloads monthly Yellow Taxi Parquet file.
  - Loads into staging.yellow_trips.
  - Runs dbt models to build staging.stg_trips and marts.fct_trip_metrics.

### 3. dbt models:

  - stg_trips: Cleans & renames raw columns.
  - fct_trip_metrics: Aggregates trips into hourly metrics (trips, avg distance, fare, tip, revenue).

### 4. Views for Analytics:

  - Additional SQL views (marts.v_*) simplify reporting.
  - Power BI and Metabase connect directly to these views.

### 5. Dashboards:

  - Trips per hour
  - Average fare & tips by time of day
  - Payment type breakdown
  - Revenue by distance buckets
  - Top pickup & drop-off zones

## 🔧 Getting Started
Prerequisites:
  - Docker Desktop installed and running
  - Python 3.12+ (for dbt if running outside container)
  - dbt-core installed locally (pip install dbt-core dbt-postgres)

## 📦 Setup 

Follow these steps to set up the project on your local machine:

### clone repo
```bash
git clone https://github.com/your-username/nyc-mini-lakehouse.git
cd nyc-mini-lakehouse

# start infra with Docker
docker compose up -d

# run dbt models (from dbt folder)
cd dbt
dbt run
```
]

## 🎯 Learnings

- Designed layered schema (staging, marts) for clarity and scalability.
- Used Airflow + Docker Compose to orchestrate ELT tasks with dbt integration.
- Built analytics-ready tables and exposed them to BI tools.
- Showcased how Docker simplifies reproducibility and deployment.

## 📌 Next Steps

Add incremental dbt models for monthly appends.
Automate materialized view refresh after DAG runs.
Deploy to cloud (AWS/GCP) with managed Airflow & dbt.
CI/CD pipelines for dbt & container builds.
