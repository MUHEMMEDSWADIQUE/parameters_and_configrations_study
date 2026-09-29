# =============================================================================
# 02_dynamic_source.py — Dynamic source configuration in Python (SDP)
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Read source paths from spark.conf at runtime
#   2. Construct dynamic Auto Loader sources
#   3. Build fully qualified table names from runtime parameters
#   4. Compare Python dynamic sources vs SQL deploy-time substitution
#
# KEY DIFFERENCE from SQL:
#   In SQL (02_dynamic_source.sql), the source path is baked in at deploy
#   time via ${var.source_path}. In Python, we read the same value at
#   RUNTIME via spark.conf.get("parameter_lab.source_path").
#
#   Both produce the same result, but Python gives you more flexibility:
#     - You can modify the path at runtime (e.g., add date partitions)
#     - You can validate and transform the path before using it
#     - You can use conditional logic based on the processing_mode
# =============================================================================

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# Read runtime configuration
# -----------------------------------------------------------------------------
CATALOG = spark.conf.get("parameter_lab.catalog")
SCHEMA = spark.conf.get("parameter_lab.schema")
SOURCE_PATH = spark.conf.get("parameter_lab.source_path")
PROCESSING_MODE = spark.conf.get("parameter_lab.processing_mode")
MAX_FILES = spark.conf.get("parameter_lab.max_files_per_trigger", "100")
BRONZE_SUFFIX = spark.conf.get("parameter_lab.bronze_suffix", "_bronze")

print(f"[dynamic_source] CATALOG={CATALOG}, SCHEMA={SCHEMA}")
print(f"[dynamic_source] SOURCE_PATH={SOURCE_PATH}")
print(f"[dynamic_source] PROCESSING_MODE={PROCESSING_MODE}")

# -----------------------------------------------------------------------------
# Dynamic Auto Loader source.
# The path is constructed at RUNTIME from spark.conf values.
# Compare with 02_dynamic_source.sql where ${var.source_path} is used.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.py_raw_customers{BRONZE_SUFFIX}",
    comment="Bronze layer: raw customers loaded dynamically via Python Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipeline": "parameter_lab",
    }
)
def py_raw_customers():
    # Build the source path dynamically
    source_path = f"{SOURCE_PATH}/customers"
    print(f"[dynamic_source] Auto Loader path: {source_path}")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES)
        .load(source_path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_processing_mode", lit(PROCESSING_MODE))
    )


# -----------------------------------------------------------------------------
# Conditional source: full vs incremental.
# In `full` mode (prod), read the entire table. In `incremental` mode (dev/test),
# read as a stream. This is a RUNTIME decision based on spark.conf.
# -----------------------------------------------------------------------------
@dlt.view(
    name="conditional_source_view",
    comment="Demonstrates conditional source selection based on processing_mode"
)
def conditional_source_view():
    source_table = f"{CATALOG}.{SCHEMA}.source_orders"

    if PROCESSING_MODE == "full":
        print(f"[dynamic_source] FULL mode: reading batch from {source_table}")
        return spark.read.table(source_table)
    else:
        print(f"[dynamic_source] INCREMENTAL mode: streaming from {source_table}")
        return spark.readStream.table(source_table)


# -----------------------------------------------------------------------------
# Dynamic source with runtime path construction.
# Add date-based partitioning to the source path at runtime.
# This is something SQL deploy-time substitution CANNOT do.
# -----------------------------------------------------------------------------
@dlt.view(
    name="dated_source_view",
    comment="Source with runtime date partitioning (only possible in Python)"
)
def dated_source_view():
    from datetime import date, timedelta

    # Runtime date computation — not possible with deploy-time substitution
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    dated_path = f"{SOURCE_PATH}/orders/dt={yesterday}"
    print(f"[dynamic_source] Runtime dated path: {dated_path}")

    try:
        return spark.read.format("delta").load(dated_path)
    except Exception as e:
        print(f"[dynamic_source] Path not found: {dated_path}. Returning empty. Error: {e}")
        # Return an empty DataFrame with the expected schema
        return spark.createDataFrame([], schema="order_id STRING, amount DOUBLE, order_date STRING")
