# Code Documentation

## **Overview**
This document explains the functionality of each Python script used in the pipeline.

---

## **1. Data Ingestion ([Final_project.py](https://github.com/aa-it-vasa/ddca2025-project-group_60/blob/main/code/Final_project.py))**
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

## **5. Integration & Data Quality Testing ([test_script.py](https://github.com/aa-it-vasa/ddca2025-project-group_60/blob/main/code/test_script.py))**

### **Test Functions**

#### **6. `test_catalog_and_schemas()`**
- **Purpose**: Validates that the `ddca_exc4` catalog and all required schemas exist in the Databricks environment.
- **Tested Elements**:
  - Checks if the catalog `ddca_exc4` exists.
  - Verifies the existence of the `default`, `bronze`, `silver`, and `gold` schemas.
  - Ensures the medallion architecture structure is properly initialized before further validation.

#### **7. `test_bronze_tables_exist()`**
- **Purpose**: Ensures that all raw tables ingested into the `default` schema are correctly replicated into the Bronze layer.
- **Tested Elements**:
  - Retrieves all non-temporary tables from `ddca_exc4.default`.
  - Confirms that each raw table exists in `ddca_exc4.bronze`.
  - Validates successful Bronze layer creation.

#### **8. `test_silver_tables_quality()`**
- **Purpose**: Validates structural integrity, transformation logic, and data quality rules applied in the Silver layer.
- **Tested Elements**:
  - Ensures all expected Silver tables exist.
  - Verifies that primary key columns are present in each table.
  - Confirms that no `"Unknown"` values appear in primary key columns.
  - Validates deduplication by checking primary key uniqueness.
  - Ensures metadata columns (`processed_date`, `is_valid`) are present.
  - Confirms all column names are standardized to lowercase.

#### **9. `test_gold_table_quality()`**
- **Purpose**: Validates the correctness and integrity of the `race_results_by_date` Gold table.
- **Tested Elements**:
  - Ensures the `race_results_by_date` table exists in the Gold schema.
  - Verifies that all required business columns exist:
    - `date`
    - `race_name`
    - `circuit_name`
    - `circuit_location`
    - `circuit_country`
    - `driver_forename`
    - `driver_surname`
    - `driver_nationality`
    - `constructor_name`
    - `constructor_nationality`
    - `race_position`
    - `qualifying_position`
  - Confirms there are no NULL values in the final Gold dataset.
  - Validates chronological ordering by ensuring records are correctly ordered by the `date` column.

#### **10. `run_all_tests()`**
- **Purpose**: Executes all integration and data quality tests sequentially.
- **Test Execution**:
  - Starts with catalog and schema validation.
  - Verifies Bronze layer table replication.
  - Validates Silver layer transformations and data quality constraints.
  - Checks the Gold layer aggregation and ordering logic.
  - If all tests pass, it prints a success message; if any test fails, execution stops with a descriptive assertion error.


## **Technology Stack**
- Apache Spark (PySpark)
- Delta Lake (Databricks)
- Python
