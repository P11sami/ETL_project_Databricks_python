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

## **5. Unit testing ([Test_script.py](https://github.com/aa-it-vasa/ddca2025-project-group_60/blob/main/code/Test%20script.py)**

### **Test Functions**

#### **6. `test_catalog_and_schemas_exist()`**
- **Purpose**: Validates that the `ddca_exc4` catalog and the `bronze`, `silver`, and `gold` schemas exist in the Databricks environment.
- **Tested Elements**:
  - Checks if the catalog `ddca_exc4` exists.
  - Verifies the existence of the `bronze`, `silver`, and `gold` schemas in the catalog.

#### **7. `test_tables_created()`**
- **Purpose**: Ensures that the required tables are present in each layer (Bronze, Silver, Gold).
- **Tested Elements**:
  - Checks the existence of all tables in the Bronze and Silver layers based on the `tables_primary_keys` dictionary.
  - Ensures that the `race_results_by_date` table exists in the Gold layer.

#### **8. `test_silver_transformations()`**
- **Purpose**: Ensures that the transformations applied in the Silver layer are correct.
- **Tested Elements**:
  - Verifies that there are no null values where they shouldn’t be in the Silver tables.
  - Ensures that all Silver tables have the necessary metadata columns (`processed_date`, `is_valid`).

#### **9. `test_gold_table()`**
- **Purpose**: Validates the `race_results_by_date` Gold table.
- **Tested Elements**:
  - Ensures that the required columns exist in the Gold table.
  - Confirms that the table is ordered correctly by the `date` column.

#### **10. `run_pipeline_tests()`**
- **Purpose**: Runs all tests sequentially.
- **Test Execution**:
  - Starts with catalog and schema validation.
  - Follows by testing table creation for each layer.
  - Validates Silver layer transformations and checks the Gold layer table.
  - If all tests pass, it prints a success message; if any test fails, it prints the error and halts execution.


## **Technology Stack**
- Apache Spark (PySpark)
- Delta Lake (Databricks)
- Python
