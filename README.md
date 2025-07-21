# 🧱 Data Engineering Project: End-to-End Pipeline (Bronze → Silver → Gold)

This repository contains a complete **data pipeline** built on the **Medallion Architecture** to process raw CRM and ERP datasets into clean, business-ready analytics views using PostgreSQL and PySpark.

Inspired by the excellent [sql-data-warehouse-project](https://github.com/DataWithBaraa/sql-data-warehouse-project), this repo has been restructured and extended to reflect more real-world practice, automation, and validation.

---

## 🆕 What’s Upgraded From Original

| Area               | Original                        | Personalized Version                          |
|--------------------|----------------------------------|-----------------------------------------------|
| 💻 Runtime          | SQL-only, manual execution       | Automated with **PySpark + Docker**           |
| 🔄 Load Method      | SQL scripts only                | **PySpark ETL scripts** with modular control  |
| 🧪 Validation       | Basic checks on Bronze          | **Post-load validations in Silver**, error logs |
| 📊 Modeling         | Basic views                     | **Star schema modeling** in Gold layer with Materialized views and indexes for query performance        |
| 🐳 Environment      | Microsoft SQL only      | Full **Docker Compose** with Spark and Postgres |

---

## 🚀 Key Features & Highlights

- 🧱 Implements full Medallion Architecture (Bronze → Silver → Gold)
- 🔁 End-to-end pipeline using **PySpark** for ingestion and PostgreSQL for storage
- 🧼 Cleansing, enrichment, and standardization in Silver; integrated business logic in Gold
- 🧪 Post-load validation strategy with `load_log` and `data_validation_logs` tables
- 🔍 Quality checks and logging at multiple stages (nulls, PKs, type mismatches)
- 📁 Modular SQL structure by layer: `bronze/`, `silver/`, `gold/`
- 🐳 Fully containerized setup with **Docker Compose (Spark + PostgreSQL)**
- 📚 Git-based versioning and detailed documentation (catalog, flow, model, naming)

---

## 🧰 Tech Stack

| Layer        | Technology                         |
|--------------|-------------------------------------|
| Ingestion    | PySpark                             |
| Database     | PostgreSQL (via Docker container)   |
| Data Layer   | Medallion Architecture              |
| Monitoring   | Custom `silver.load_log` and `monitoring.data_validation_logs` table      |
| Deployment   | Docker, Docker Compose              |
| Scripting    | SQL, Python                         |

---

## 🧭 Project Stages

### 1. Requirement Analysis
- Identify business processes and data ownership
- Understand source systems, data scope, and access protocols

### 2. Data Architecture Design
- Architecture: Data Warehouse with Medallion model
- Layers: Bronze (raw), Silver (cleaned), Gold (business-ready)
- Naming conventions, Git structure, database schema planning

### 3. Bronze Layer – Raw Ingestion
- Source: CSV files or other extractable formats
- Load method: Full load (truncate & insert)
- No transformations applied
- Stored in raw tables for traceability

### 4. Silver Layer – Standardization & Cleansing
- Clean and enrich Bronze data
- Handle nulls, duplicates, invalid formats
- Add derived fields and standardize codes
- Load to clean Silver tables via stored procedures
- Constraints are avoided to prevent errors from bad bronze data

### 5. Gold Layer – Business-Ready Data
- Build analytical views with friendly naming
- Implement dimension and fact tables (star schema where needed)
- Apply business logic, rules, and aggregations
- Use views to present flat or aggregated models for reporting

---

## 📐 Architecture Overview

### Medallion Layers

| Layer   | Description                              | Transformation                           | Object Type | Load Type      |
|---------|------------------------------------------|------------------------------------------|-------------|----------------|
| Bronze  | Raw ingested data from external sources  | None (as-is from source)                 | Tables      | Full (T&I)     |
| Silver  | Cleaned and standardized data            | Cleaning, normalization, enrichment      | Tables      | Full (T&I)     |
| Gold    | Business-ready analytical data           | Integration, Aggregation, Business Logic | Views       | Full (T&I)     |

---

## 🚀 Pipeline Flow

### 🥉 Bronze Layer
- `etl.py` ingests source files into raw PostgreSQL tables
- No transformation, full load with truncation
- Goal: Auditable, traceable raw data snapshot

### 🥈 Silver Layer
- Stored procedures first **insert data from Bronze** into Silver tables
- After insertion, **validation logic is applied**:
  - Detect nulls or duplicates in primary keys
  - Check for invalid or inconsistent values
  - Identify transformation or enrichment issues
- Logs quality issues and failed rows into `load_log` and `data_validation_logs` tables
- All transformations are still handled (cleaning, standardizing, enriching)

### 🥇 Gold Layer
- Business objects created via SQL Materialized Views
- Combines multiple silver tables into analytical dimensions and facts
- Aggregates, applies logic, and formats for downstream consumption
- Includes flat models and **star schemas**
- Documented in the data catalog and model flow diagram

---

## 📒 Documentation

- 📐 [docs/images/data_architecture.png](docs/images/data_architecture.png)
- 🔁 [docs/images/data_flow.png](docs/images/data_flow.png)
- ⭐ [docs/images/data_model.png](docs/images/data_model.png)
- 📘 [docs/data_catalog.md](docs/data_catalog.md)
- 🧾 [docs/naming_convention.md](docs/naming_convention.md)

---

## 📌 Design Assumptions

- Full Load (truncate & insert) — no change tracking needed
- SCD Type 1 — latest state only
- No FK constraints in Silver — validations are soft, logged after loading
- Views in Gold — transparency and flexibility
- Dockerized for easy local testing

---

### 🔐 Environment Variables

This project uses a `.env` file to manage configuration and sensitive values.

To get started:

```bash
# Create your local .env file from the template
cp .env.example .env

## 🏁 Getting Started

```bash
# 1. Start everything (Docker Compose will run Spark + PostgreSQL + trigger ETL)
docker-compose up --build

# 2. Run SQL validations and transformations in order

# Validate Bronze Layer
psql -f sql/bronze/00_validate_bronze_layer.sql

# Run Silver Layer
psql -f sql/silver/00_create_load_procedures.sql
psql -f sql/silver/01_validate_silver_layer.sql
psql -f sql/silver/02_run_silver_pipeline.sql

# Create Gold Layer
psql -f sql/gold/00_create_gold_views.sql
psql -f sql/gold/01_create_indexes.sql