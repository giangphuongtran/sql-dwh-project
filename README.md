# 🧱 Data Engineering Project: End-to-End Pipeline (Bronze → Silver)

This repository contains a complete **data pipeline** built on the **Medallion Architecture** to process raw CRM and ERP datasets from source CSV files into a clean and queryable format using PostgreSQL and PySpark.

The focus of this project is to build a maintainable and scalable foundation from data ingestion (bronze layer) through data cleaning and standardization (silver layer), while ensuring data quality, error logging, and future readiness for analytics (gold layer).

---

## 📐 Architecture Overview

### Medallion Layers

| Layer   | Description                              | Transformation               | Object Type | Load Type      |
|---------|------------------------------------------|------------------------------|-------------|----------------|
| Bronze  | Raw ingested data from external sources  | None (as-is from source)     | Tables      | Full (T&I)     |
| Silver  | Cleaned and standardized data            | Validation, Cleaning, Enrich | Tables      | Full (T&I)     |
| Gold    | Business-ready analytical views          | Aggregation, Business Logic  | Views       | Full (T&I)     |

> 🔎 This repo covers the Bronze → Silver transition.

---

## ⚙️ Key Features

- 🔁 End-to-end data pipeline using PySpark + PostgreSQL
- 🔍 Data validation (nulls, duplicates, formats) in bronze
- 🧼 Data cleaning, standardization, and enrichment in silver
- 🧪 Row-level quality checks and logical validations
- 🧾 Centralized error tracking via `load_log`
- 🐳 Fully containerized using Docker and Docker Compose
- 📚 Modular SQL scripts for transparency and reuse

---

## 🧰 Tech Stack

| Layer        | Technology                         |
|--------------|-------------------------------------|
| Ingestion    | PySpark, SQLAlchemy                 |
| Database     | PostgreSQL (via Docker container)   |
| Data Layer   | Medallion Architecture              |
| Monitoring   | Custom `silver.load_log` table      |
| Deployment   | Docker, Docker Compose              |
| Scripting    | SQL, Python                         |

---

## 🚀 Pipeline Flow

### 1. Ingest (Bronze Layer)
- PySpark script `etl.py` ingests CSVs from `datasets/` into PostgreSQL bronze tables.
- No transformation is applied; data is stored raw.
- Goal: Raw, auditable snapshot of source.

### 2. Validate (Bronze Layer)
- Run `04_validate_bronze_layer.sql` to:
    - Check for missing or malformed keys
    - Invalid date formats
    - Identify duplicates
    - Standardize codes, formats, or values
    - Validate logical values (e.g., `sales = quantity * price`)

### 3. Transform & Load (Silver Layer)
- `03_create_load_procedures.sql` contains stored procedures for each table:
  - Cleaned, enriched, standardized, then inserted valid records from bronze into silver
  - Catches errors via `EXCEPTION` and logs to `load_log` (e.g., FK violations, parsing errors,...)
- `05_run_pipeline.sql` runs all in order

### 4. Monitor
- `02_setup_monitoring_log.sql` defines `load_log` to track table status
- Monitors for:
  - Success/failure
  - Truncation errors
  - FK constraint violations
  - Transformation errors
