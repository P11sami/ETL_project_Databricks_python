# Code Documentation

## **Overview**
This document explains the functionality of each Python script used in the pipeline.

---

## **1. Data Ingestion (`Final_project.py`)**
- Reads structured (CSV) and unstructured (JSON) files from a given folder.
- Stores raw data into **Bronze Delta tables**.
- Uses:
  - `ingest_structured_data(file_path, table_name)`: Loads CSV into Bronze Delta tables.
  - `ingest_unstructured_data(file_path, table_name)`: Loads JSON into Bronze Delta tables.
  - `ingest_all_files_in_folder(folder_path)`: Iterates through files and processes them.

---

## **2. Bronze Table Creation**
- Creates the `bronze` schema.
- Reads all raw tables and saves them as **Bronze Delta tables**.
- Function:
  - `create_bronze_table(raw_table_name, bronze_table_name)`: Moves raw data to Bronze.

---

## **3. Silver Table Processing **
- Cleans, standardizes column names, and deduplicates data.
- Handles NULL values based on table-specific logic.
- Adds metadata (`processed_date`, `is_valid`).
- Saves the cleaned data as **Silver Delta tables**.
- Function:
  - `create_silver_table(bronze_table_name, silver_table_name, primary_keys)`: Processes data from Bronze to Silver.

---

## **4. Gold Table Aggregation**
- Joins Silver tables to create a **Race Results by Date** analytical dataset.
- Function:
  - `create_race_results_by_date()`: Combines multiple Silver tables and writes to Gold.

---

## **Technology Stack**
- Apache Spark (PySpark)
- Delta Lake (Databricks)
- Python
