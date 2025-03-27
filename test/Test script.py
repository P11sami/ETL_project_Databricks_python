# Databricks notebook source
# MAGIC %run /Workspace/Users/sami.seppala@abo.fi/Final_project

# COMMAND ----------

# Unit Test Script

def test_catalog_and_schemas_exist():
    """Test that the catalog and all layer schemas exist"""
    print("=== Testing Catalog and Schemas ===")
    
    # Checking for catalog
    catalogs = spark.sql("SHOW CATALOGS").select("catalog").rdd.flatMap(lambda x: x).collect()
    assert "ddca_exc4" in catalogs, f"Catalog 'ddca_exc4' not found. Available catalogs: {catalogs}"
    print("✅ Catalog 'ddca_exc4' exists")
    
    # Checking schemas
    required_schemas = ["bronze", "silver", "gold"]
    existing_schemas = spark.sql("SHOW SCHEMAS IN ddca_exc4").select("databaseName").rdd.flatMap(lambda x: x).collect()
    
    missing_schemas = [s for s in required_schemas if s not in existing_schemas]
    if missing_schemas:
        print(f"Existing schemas in ddca_exc4: {existing_schemas}")
        assert False, f"Missing schemas in ddca_exc4 catalog: {missing_schemas}"
    
    print("✅ All layer schemas exist in ddca_exc4 catalog")
    return True

def test_tables_created():
    """Test that tables exist in each layer"""
    print("\n=== Testing Table Creation ===")
    
    # Getting expected tables from primary keys dictionary
    expected_tables = list(tables_primary_keys.keys())
    
    # Checking bronze layer tables
    bronze_tables = spark.sql("SHOW TABLES IN ddca_exc4.bronze").select("tableName").rdd.flatMap(lambda x: x).collect()
    missing_bronze = [t for t in expected_tables if t not in bronze_tables]
    assert not missing_bronze, f"Missing tables in bronze layer: {missing_bronze}"
    print("✅ All expected tables exist in bronze layer")
    
    # Checking silver layer tables
    silver_tables = spark.sql("SHOW TABLES IN ddca_exc4.silver").select("tableName").rdd.flatMap(lambda x: x).collect()
    missing_silver = [t for t in expected_tables if t not in silver_tables]
    assert not missing_silver, f"Missing tables in silver layer: {missing_silver}"
    print("✅ All expected tables exist in silver layer")
    
    # Checking gold layer table
    gold_tables = spark.sql("SHOW TABLES IN ddca_exc4.gold").select("tableName").rdd.flatMap(lambda x: x).collect()
    assert "race_results_by_date" in gold_tables, "race_results_by_date missing in gold layer"
    print("✅ Gold table exists")

def test_silver_transformations():
    """Test that silver layer transformations were applied correctly"""
    print("\n=== Testing Silver Layer Transformations ===")
    
    for table in tables_primary_keys.keys():
        silver_df = spark.table(f"ddca_exc4.silver.{table}")
        
        # Checking null handling
        null_counts = {col: silver_df.filter(silver_df[col].isNull()).count() 
                      for col in silver_df.columns}
        problematic_cols = {col: cnt for col, cnt in null_counts.items() if cnt > 0}
        assert not problematic_cols, f"Null values found in {table}: {problematic_cols}"
        
        # Checking for metadata columns
        if "processed_date" not in silver_df.columns:
            print(f"ℹ️ Table {table} missing processed_date column")
        if "is_valid" not in silver_df.columns:
            print(f"ℹ️ Table {table} missing is_valid column")
    
    print("✅ Silver layer transformations validated")

def test_gold_table():
    """Test that gold table was created correctly"""
    print("\n=== Testing Gold Layer ===")
    
    gold_df = spark.table("ddca_exc4.gold.race_results_by_date")
    
    # Checking required columns
    required_columns = {
        "date", "race_name", "circuit_name", "circuit_location", 
        "circuit_country", "driver_forename", "driver_surname",
        "driver_nationality", "constructor_name", "constructor_nationality",
        "race_position", "qualifying_position"
    }
    missing_cols = required_columns - set(gold_df.columns)
    assert not missing_cols, f"Missing columns in gold table: {missing_cols}"
    
    # Checking correct data ordering
    dates = [row.date for row in gold_df.select("date").collect()]
    assert dates == sorted(dates), "Gold table not ordered by date"
    
    print("✅ Gold layer table validated")

def run_pipeline_tests():
    """Main function to run all tests"""
    try:
        print("Starting Data Pipeline Tests\n")
        
        # First testing catalog and schemas
        if not test_catalog_and_schemas_exist():
            return
            
        # Running remaining tests
        test_tables_created()
        test_silver_transformations()
        test_gold_table()
        
        print("\n All pipeline tests passed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        print("Check the pipeline execution and try again")
        raise

run_pipeline_tests()