# Pipeline Architecture

## **Overview**
This pipeline processes racing data in Databricks, transforming it through the **Medallion Architecture** (Bronze → Silver → Gold) using Delta Lake.

---

## **Pipeline Stages**
### **1. Data Ingestion (Bronze Layer)**
- Reads raw data from a given folder.
- Stores the raw data into **Delta tables** under the **Bronze schema**.
- No transformations are applied at this stage.

### **2. Data Cleaning & Standardization (Silver Layer)**
- Reads from Bronze tables and performs:
  - **Null handling:** Replaces NULLs and incorrect values.
  - **Standardization:** Converts column names to lowercase.
  - **Deduplication:** Drops duplicate rows based on primary keys.
  - **Metadata Addition:** Adds `processed_date` and `is_valid` columns.
- Saves the cleaned data into the **Silver schema**.

### **3. Data Aggregation & Enrichment (Gold Layer)**
- Joins multiple Silver tables to create an analytical dataset.
- Enriches race results by combining driver, constructor, race, and circuit details.
- Generates a **Race Results by Date** table for reporting.
- Stores the final transformed data in the **Gold schema**.

---

## **Technology Stack**
- **Databricks** (Spark, Delta Lake)
- **PySpark** (for ETL transformations)
- **Python** (for data processing and orchestration)
- **Delta Tables** (for optimized storage and queries)

---

## **Data Flow Diagram**
See the `docs/pipeline_architecture.png` for a visual representation of the pipeline.
