# 📚 Book Analytics ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-00758F?style=for-the-badge&logo=mysql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=for-the-badge)

## 📌 Project Overview

This project implements a robust, production-style **ETL (Extract, Transform, Load) Pipeline** designed to process book sales data. Unlike simple data migration scripts, this solution emphasizes **Data Engineering best practices**, including strict data validation, relational modeling, and historical data tracking.

The pipeline ingests raw CSV data, cleans localized formatting issues, enforces business logic, and loads data into a **MySQL Data Warehouse** using a **Slowly Changing Dimension (SCD) Type 2** strategy.

### 🚀 Key Objectives
* **Data Integrity:** Enforcing relational integrity and strict typing.
* **Standardization:** Converting raw, localized data into analytics-ready formats.
* **Historical Tracking:** Implementing SCD Type 2 to track changes in dimension attributes over time.
* **Observability:** Comprehensive logging for every pipeline execution.

---

## 🏗 Architecture & Design

The system follows a modular **Extract → Validate → Transform → Load → Log** architecture.

### 🗂 Database Schema (Star Schema)
The database is normalized into a Star Schema to optimize analytical queries.

| Type | Table Name | Description |
| :--- | :--- | :--- |
| **Dimension** | `kitap_adlari` | Book identity information |
| **Dimension** | `yazar_adlari` | Author details |
| **Dimension** | `kitap_turleri` | Category/Genre information |
| **Dimension** | `dil` | Language reference |
| **Fact** | `fact_table` | Transactional sales metrics |

**Fact Table Granularity:**
One row represents: `1 Book` + `1 Publication Date` + `Sales Metrics`.

---

## ⚙️ ETL Workflow

### 1. 📤 Extract & Data Cleaning
The pipeline handles raw CSVs with inconsistent, localized formatting.
* **Date Parsing:** Converts Turkish formats (`15.08.2019`) to ISO 8601 (`2019-08-15`).
* **Numeric Standardization:** Removes thousand separators (`2.207` → `2207`) and converts decimal commas to dots (`30,50` → `30.50`).
* **Type Casting:** Enforces `DECIMAL(10,2)` for monetary values to prevent floating-point errors.

### 2. 🛡 Validation Layer (Fail-Fast)
Execution stops immediately if critical data quality rules are violated:
* ✅ No `NULL` values in business keys.
* ✅ No duplicate business keys.
* ✅ Sales values must be non-negative.
* ✅ Foreign key referential integrity checks.

### 3. 🔄 Transformation
* **Feature Engineering:** Derives `year`, `month`, and calculates `unit_price` (`total_sales / quantity`).
* **Normalization:** Joins dimensions to prepare data for the Fact table.

### 4. 🕰 Slowly Changing Dimension (SCD Type 2)
To preserve historical accuracy without overwriting data, the pipeline uses **SCD Type 2 logic**:

1.  **Hash Generation:** A hash is generated for tracked attributes (`book_name`, `author`, `genre`, `price`).
2.  **Comparison:** The incoming record's hash is compared against the active record in the DB.
3.  **Action:**
    * *No Change:* Skip.
    * *Changed:* "Retire" the old record (update `effective_to`) and insert the new record.
    * *New:* Insert directly.

---

## 📊 Logging & Observability

Every execution is tracked in a dedicated logging table to ensure full traceability.

| Metric | Description |
| :--- | :--- |
| **Run ID** | Unique identifier for the batch |
| **Duration** | Total execution time in seconds |
| **Status** | `SUCCESS` or `FAILED` |
| **Row Counts** | Extracted, Transformed, Inserted |
| **SCD Metrics** | Count of New vs. Changed records |

---

## 💻 Tech Stack

* **Language:** Python 3.x
* **Data Manipulation:** Pandas
* **ORM / Database:** SQLAlchemy, MySQL Connector
* **Configuration:** YAML
* **Logic:** Hash-based Change Detection (SHA256)

## 📂 Project Structure

```bash
book-analytics-etl-scd2/
├── config/           # Database and pipeline configurations (YAML)
├── data/             # Raw input data (CSV)
├── extract/          # Data ingestion modules
├── transform/        # Cleaning and feature engineering logic
├── load/             # SCD2 logic and database loaders
├── logs/             # Execution logs
├── main.py           # Pipeline entry point
├── requirements.txt  # Dependencies
└── README.md         # Project documentation
