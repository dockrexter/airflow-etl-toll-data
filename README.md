# Airflow ETL Pipeline: Toll Data Consolidation

This repository contains an Apache Airflow ETL pipeline that processes toll data from multiple file formats (CSV, TSV, fixed-width) and consolidates it into a single, standardized CSV file for analytics.

The pipeline is designed as a final assignment for an Airflow/ETL course and demonstrates:
- Unzipping and preparing raw data.
- Extracting specific fields from different file formats.
- Consolidating data from multiple sources.
- Simple transformation (uppercase conversion).

---

## 📁 Files

- `dags/ETL_toll_data.py` – The main Airflow DAG that orchestrates the ETL pipeline using `BashOperator`.

---

## 🛠️ Prerequisites

Before using this project, make sure you have:

- Apache Airflow installed and running (local or Docker).
- Access to a Unix-like system (Linux/macOS) where `tar`, `cut`, `paste`, and `tr` are available.
- The dataset `tolldata.tgz` downloaded and placed in the correct directory.
