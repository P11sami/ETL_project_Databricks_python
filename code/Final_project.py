# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import os

catalog = "ddca_exc4"
database = "default"

# COMMAND ----------

def ingest_structured_data(file_path, table_name):

    # Structured data
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    
    # Delta table
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{catalog}.{database}.{table_name}")
    
    print(f"Structured data from {file_path} ingested into {catalog}.{database}.{table_name}")

# COMMAND ----------

def ingest_unstructured_data(file_path, table_name):
    
    # Unsttructured data)
    df = spark.read.format("json") \
        .option("inferSchema", "true") \
        .load(file_path)
    
    # Delta table
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{catalog}.{database}.{table_name}")
    
    print(f"Unstructured data from {file_path} ingested into {catalog}.{database}.{table_name}")

# COMMAND ----------

def ingest_all_files_in_folder(folder_path):

    files = dbutils.fs.ls(folder_path)
    
    for file_info in files:
        file_path = file_info.path
        table_name = os.path.basename(file_path).split(".")[0]
        
        if file_path.endswith(".csv"):
            ingest_structured_data(file_path, table_name)
        elif file_path.endswith(".json"):
            ingest_unstructured_data(file_path, table_name)
        else:
            print(f"Unsupported file format for file: {file_path}")

# Folderpath
folder_path = "/FileStore/tables/f1_data"

ingest_all_files_in_folder(folder_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ddca_exc4.bronze;

# COMMAND ----------

# Bronze schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ddca_exc4.bronze")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS ddca_exc4.bronze")

def create_bronze_table(raw_table_name, bronze_table_name):
   
    raw_df = spark.sql(f"SELECT * FROM {raw_table_name}")
    
    # Bronze delta table
    raw_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(bronze_table_name)
    
    print(f"Bronze table '{bronze_table_name}' created successfully.")

# COMMAND ----------

raw_tables = spark.sql("SHOW TABLES IN ddca_exc4.default").filter("isTemporary == false").select("tableName").rdd.flatMap(lambda x: x).collect()

# Bronze tables
for raw_table in raw_tables:
    raw_table_name = f"ddca_exc4.default.{raw_table}"  
    bronze_table_name = f"ddca_exc4.bronze.{raw_table}" 
    create_bronze_table(raw_table_name, bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ddca_exc4.silver;

# COMMAND ----------

# Silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ddca_exc4.silver")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql import DataFrame

def handle_null_values(df: DataFrame, table_name: str) -> DataFrame:

    #Null values
    for column in df.columns:
        if column == "position" and table_name in ["results", "qualifying"]:
            # Replace both NULL and '\\N' with 'DNF' for position column
            df = df.withColumn(column, when(col(column).isNull() | (col(column) == lit("\\N")), lit("DNF")).otherwise(col(column)))
        else:
            df = df.withColumn(column, when(col(column).isNull(), lit("Unknown")).otherwise(col(column)))
    
    return df

def create_silver_table(bronze_table_name, silver_table_name, primary_keys):
    
    #Silver table
    table_name = bronze_table_name.split(".")[-1]

    bronze_df = spark.read.format("delta").table(bronze_table_name)

    bronze_df = handle_null_values(bronze_df, table_name)

    # Standardizing column names by making them lower case
    silver_df = bronze_df.select([col(c).alias(c.lower()) for c in bronze_df.columns])

    # Deduplication based on primary keys
    if primary_keys:
        silver_df = silver_df.dropDuplicates(primary_keys)

    # Adding metadata columns
    silver_df = silver_df.withColumn("processed_date", current_timestamp()) \
                         .withColumn("is_valid", lit(True))

    # Silver delta table
    silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)

    print(f"Silver table '{silver_table_name}' created successfully.")

bronze_schema = "ddca_exc4.bronze"
silver_schema = "ddca_exc4.silver"

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

# Processing tables
for table, primary_keys in tables_primary_keys.items():
    bronze_table_name = f"{bronze_schema}.{table}"
    silver_table_name = f"{silver_schema}.{table}"
    create_silver_table(bronze_table_name, silver_table_name, primary_keys)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ddca_exc4.gold;

# COMMAND ----------

# Gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ddca_exc4.gold")

# COMMAND ----------

from pyspark.sql import functions as F

def create_race_results_by_date():
    
    # Golden Layer table: race results by date.
    
    # Relevant tables
    results_df = spark.read.format("delta").table("ddca_exc4.silver.results")
    drivers_df = spark.read.format("delta").table("ddca_exc4.silver.drivers")
    constructors_df = spark.read.format("delta").table("ddca_exc4.silver.constructors")
    races_df = spark.read.format("delta").table("ddca_exc4.silver.races")
    circuits_df = spark.read.format("delta").table("ddca_exc4.silver.circuits")
    qualifying_df = spark.read.format("delta").table("ddca_exc4.silver.qualifying")

    # Position columns
    results_df = results_df.withColumn("race_position", F.col("position"))
    qualifying_df = qualifying_df.withColumn("qualifying_position", F.col("position"))

    # Handling conflicting columns
    drivers_df = drivers_df.select(
        "driverid",
        F.col("forename").alias("driver_forename"),
        F.col("surname").alias("driver_surname"),
        F.col("nationality").alias("driver_nationality")
    )

    constructors_df = constructors_df.select(
        "constructorid",
        F.col("name").alias("constructor_name"),
        F.col("nationality").alias("constructor_nationality")
    )

    races_df = races_df.select(
        "raceid", "circuitid", "date", 
        F.col("name").alias("race_name")
    )

    circuits_df = circuits_df.select(
        "circuitid",
        F.col("name").alias("circuit_name"),
        F.col("location").alias("circuit_location"),
        F.col("country").alias("circuit_country")
    )

    qualifying_df = qualifying_df.select(
        "raceid", "driverid", "qualifying_position"
    )

    # Joining tables and replacing new null values with "Unknown" where necessary
    race_results_df = results_df \
        .join(drivers_df, "driverid", "inner") \
        .join(constructors_df, "constructorid", "inner") \
        .join(races_df, "raceid", "inner") \
        .join(circuits_df, "circuitid", "inner") \
        .join(qualifying_df, ["raceid", "driverid"], "left") \
        .select(
            "date",
            "race_name",
            "circuit_name",
            "circuit_location",
            "circuit_country",
            F.coalesce("driver_forename", F.lit("Unknown")).alias("driver_forename"),
            F.coalesce("driver_surname", F.lit("Unknown")).alias("driver_surname"),
            F.coalesce("driver_nationality", F.lit("Unknown")).alias("driver_nationality"),
            F.coalesce("constructor_name", F.lit("Unknown")).alias("constructor_name"),
            F.coalesce("constructor_nationality", F.lit("Unknown")).alias("constructor_nationality"),
            F.coalesce("race_position", F.lit("Unknown")).alias("race_position"),
            F.coalesce("qualifying_position", F.lit("Unknown")).alias("qualifying_position")
        ) \
        .orderBy("date")

    # Golden delta table
    race_results_df.write.format("delta").mode("overwrite").saveAsTable("ddca_exc4.gold.race_results_by_date")

    print("Golden table 'ddca_exc4.gold.race_results_by_date' created successfully.")


create_race_results_by_date()

# COMMAND ----------

