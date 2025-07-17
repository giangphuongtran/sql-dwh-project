# 🧱 Data Engineering Project: End-to-End Pipeline (Bronze → Silver → Gold)

This repository contains a complete **data pipeline** built on the **Medallion Architecture** to process raw CRM and ERP datasets into clean, business-ready analytics views using PostgreSQL and PySpark.

The project includes all stages of a modern data pipeline: from **requirement analysis**, **data architecture design**, **data ingestion**, **cleansing**, to **data modeling for analytics**. The design emphasizes traceability, maintainability, and modularity.

---

## 📐 Architecture Overview

### Medallion Layers

| Layer   | Description                              | Transformation                           | Object Type | Load Type      |
|---------|------------------------------------------|------------------------------------------|-------------|----------------|
| Bronze  | Raw ingested data from external sources  | None (as-is from source)                 | Tables      | Full (T&I)     |
| Silver  | Cleaned and standardized data            | Cleaning, normalization, enrichment      | Tables      | Full (T&I)     |
| Gold    | Business-ready analytical data           | Integration, Aggregation, Business Logic | Views       | Full (T&I)     |

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

## ⚙️ Key Features

- 🔁 Full end-to-end pipeline using PySpark + PostgreSQL
- 🔍 Validation at ingestion and cleansing stages
- 🧼 Enrichment and transformation for analytics readiness
- 🧪 Row-level quality checks, logging, and exception tracking
- 🐳 Dockerized setup with modular SQL + Python
- 📚 Git-based versioning and documentation of data models and flows

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

### 🥉 Bronze Layer
- `etl.py` ingests source files into raw PostgreSQL tables
- No transformation, full load with truncation
- Goal: Auditable, traceable raw data snapshot

### 🥈 Silver Layer
- Stored procedures clean and standardize bronze data
- `03_create_load_procedures.sql` transforms and inserts into silver tables
- Logs quality issues and failed rows into `load_log`
- `05_run_pipeline.sql` executes the full silver loading process

### 🥇 Gold Layer
- Business objects created via SQL Views
- Combines multiple silver tables into analytical dimensions and facts
- Aggregates, applies logic, and formats for downstream consumption
- Includes flat models and star schemas
- Documented in the data catalog and model flow diagram

---

## 📒 Documentation

- 🧾 `docs/`: Contains data flow diagrams, entity relationships, and catalogs
- 📘 `load_log`: Tracks ETL load status and errors
- 📂 `sql/`: Contains modular transformation scripts for all layers

---

## 📌 Notes

- All data flows are **full loads** using Truncate & Insert for simplicity
- Bronze and Silver are implemented as **tables**; Gold is implemented as **views**
- Constraints are deferred to Gold to avoid failures due to dirty source data
- Use of Docker ensures consistent, reproducible environments

---

