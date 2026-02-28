# test_script_serverless.py
# Serverless-Compatible Pipeline Integration Tests

from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

# ==============================
# CONFIGURATION
# ==============================

CATALOG = "ddca_exc4"
RAW_SCHEMA = "default"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
GOLD_TABLE = "race_results_by_date"

tables_primary_keys = {
    "circuits": ["circuitid"],
    "constructor_standings": ["constructorstandingsid", "raceid", "constructorid"],
    "constructors": ["constructorid"],
    "driver_standings": ["driverstandingsid", "raceid", "driverid"],
    "drivers": ["driverid"],
    "lap_times": ["raceid", "driverid", "lap"],
    "pit_stops": ["raceid", "driverid", "stop", "lap"],
    "qualifying": ["qualifyid"],
    "races": ["raceid"],
    "results": ["resultid"],
    "seasons": ["year"],
    "sprint_results": ["resultid"],
    "status": ["statusid"]
}

# ==============================
# HELPER FUNCTIONS
# ==============================

def get_table_names(schema):
    df = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}")
    return [row["tableName"] for row in df.collect()]


def get_schema_names():
    df = spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
    return [row["databaseName"] for row in df.collect()]


def get_catalog_names():
    df = spark.sql("SHOW CATALOGS")
    return [row["catalog"] for row in df.collect()]


def table_exists(schema, table):
    return table in get_table_names(schema)


def assert_primary_key_unique(df, primary_keys, table_name):
    total = df.count()
    distinct = df.select(primary_keys).dropDuplicates().count()
    assert total == distinct, \
        f"[{table_name}] Duplicate rows found based on primary keys."


def assert_metadata_columns(df, table_name):
    expected = {"processed_date", "is_valid"}
    missing = expected - set(df.columns)
    assert not missing, \
        f"[{table_name}] Missing metadata columns: {missing}"


def assert_no_unknown_in_primary_keys(df, primary_keys, table_name):
    for pk in primary_keys:
        unknown_count = df.filter(col(pk) == "Unknown").count()
        assert unknown_count == 0, \
            f"[{table_name}] Primary key column '{pk}' contains 'Unknown' values."


# ==============================
# TESTS
# ==============================

def test_catalog_and_schemas():
    assert CATALOG in get_catalog_names(), \
        f"Catalog '{CATALOG}' does not exist."

    schemas = get_schema_names()
    for schema in [RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
        assert schema in schemas, f"Schema '{schema}' missing."


def test_bronze_tables_exist():
    raw_tables = get_table_names(RAW_SCHEMA)

    for table in raw_tables:
        assert table_exists(BRONZE_SCHEMA, table), \
            f"Bronze table '{table}' missing."


def test_silver_tables_quality():
    for table, primary_keys in tables_primary_keys.items():

        assert table_exists(SILVER_SCHEMA, table), \
            f"Silver table '{table}' missing."

        df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")

        # PK existence
        for pk in primary_keys:
            assert pk in df.columns, \
                f"[{table}] Missing primary key column '{pk}'."

        # No 'Unknown' in PK
        assert_no_unknown_in_primary_keys(df, primary_keys, table)

        # Deduplication validation
        assert_primary_key_unique(df, primary_keys, table)

        # Metadata columns validation
        assert_metadata_columns(df, table)

        # Lowercase column validation
        for column in df.columns:
            assert column == column.lower(), \
                f"[{table}] Column '{column}' is not lowercase."


def test_gold_table_quality():
    assert table_exists(GOLD_SCHEMA, GOLD_TABLE), \
        "Gold table missing."

    df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}")

    required_columns = {
        "date",
        "race_name",
        "circuit_name",
        "circuit_location",
        "circuit_country",
        "driver_forename",
        "driver_surname",
        "driver_nationality",
        "constructor_name",
        "constructor_nationality",
        "race_position",
        "qualifying_position"
    }

    missing = required_columns - set(df.columns)
    assert not missing, \
        f"Gold table missing columns: {missing}"

    # No NULLs in gold
    for column in required_columns:
        null_count = df.filter(col(column).isNull()).count()
        assert null_count == 0, \
            f"[Gold] Column '{column}' contains NULL values."

    # Chronological validation
    window = Window.orderBy("date")
    out_of_order = (
        df.withColumn("prev_date", lag("date").over(window))
          .filter(col("prev_date").isNotNull() & (col("date") < col("prev_date")))
          .count()
    )

    assert out_of_order == 0, \
        "Gold table contains out-of-order dates."


# ==============================
# RUNNER
# ==============================

def run_all_tests():
    print("\nRunning Serverless-Compatible Pipeline Tests...\n")

    test_catalog_and_schemas()
    print("✔ Catalog & schemas validated")

    test_bronze_tables_exist()
    print("✔ Bronze layer validated")

    test_silver_tables_quality()
    print("✔ Silver layer validated")

    test_gold_table_quality()
    print("✔ Gold layer validated")

    print("\n ALL TESTS PASSED SUCCESSFULLY\n")


run_all_tests()
