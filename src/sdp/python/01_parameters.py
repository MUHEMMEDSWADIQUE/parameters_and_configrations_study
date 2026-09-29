# =============================================================================
# 01_parameters.py — Runtime parameter access in Python (SDP)
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Access pipeline configuration via spark.conf.get()
#   2. Understand the difference between deploy-time and runtime parameters
#   3. Create a config table that SQL can read (Python -> SQL bridge)
#   4. Handle missing parameters gracefully
#
# KEY CONCEPT:
#   The pipeline `configuration:` section in pipelines.yml sets Spark conf
#   keys like `parameter_lab.catalog`. These are available at RUNTIME via:
#     spark.conf.get("parameter_lab.catalog")
#
#   This is DIFFERENT from deploy-time ${var.catalog} substitution:
#     - ${var.catalog} is replaced in the file content BEFORE deployment
#     - spark.conf.get("parameter_lab.catalog") reads the value at RUNTIME
#     - Both resolve to the same value, but through different mechanisms
#
#   This file also demonstrates the Python -> SQL parameter bridge:
#     Python reads spark.conf -> writes to a Delta table -> SQL reads that table
# =============================================================================

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# Helper: safely read a Spark configuration value with a fallback.
# This demonstrates parameter validation and default value handling.
# -----------------------------------------------------------------------------
def get_config(key: str, default: str = None) -> str:
    """
    Read a Spark configuration value.

    If the key is missing and no default is provided, raises an exception.
    This demonstrates failure handling when parameters are missing.
    """
    try:
        value = spark.conf.get(key)
        print(f"  [config] {key} = {value}")
        return value
    except Exception as e:
        if default is not None:
            print(f"  [config] {key} not found, using default: {default}")
            return default
        raise ValueError(
            f"Required pipeline parameter '{key}' is not set. "
            f"Ensure the pipeline configuration includes this key. "
            f"Original error: {e}"
        )


# -----------------------------------------------------------------------------
# Read ALL pipeline configuration values.
# These come from the `configuration:` section of pipelines.yml.
# -----------------------------------------------------------------------------
CATALOG = get_config("parameter_lab.catalog")
SCHEMA = get_config("parameter_lab.schema")
ENVIRONMENT = get_config("parameter_lab.environment")
SOURCE_PATH = get_config("parameter_lab.source_path")
PROCESSING_MODE = get_config("parameter_lab.processing_mode")
MAX_FILES = get_config("parameter_lab.max_files_per_trigger", "100")
ENABLE_QUALITY = get_config("parameter_lab.enable_quality_checks", "true")
RETENTION_DAYS = get_config("parameter_lab.retention_days", "7")
BRONZE_SUFFIX = get_config("parameter_lab.bronze_suffix", "_bronze")
SILVER_SUFFIX = get_config("parameter_lab.silver_suffix", "_silver")
GOLD_SUFFIX = get_config("parameter_lab.gold_suffix", "_gold")
TARGET = get_config("parameter_lab.target", "unknown")

print("=" * 70)
print("PIPELINE CONFIGURATION (runtime spark.conf values)")
print("=" * 70)
print(f"  catalog:          {CATALOG}")
print(f"  schema:           {SCHEMA}")
print(f"  environment:      {ENVIRONMENT}")
print(f"  source_path:      {SOURCE_PATH}")
print(f"  processing_mode:  {PROCESSING_MODE}")
print(f"  max_files:        {MAX_FILES}")
print(f"  quality_checks:   {ENABLE_QUALITY}")
print(f"  retention_days:   {RETENTION_DAYS}")
print(f"  bronze_suffix:    {BRONZE_SUFFIX}")
print(f"  silver_suffix:    {SILVER_SUFFIX}")
print(f"  gold_suffix:      {GOLD_SUFFIX}")
print(f"  target:           {TARGET}")
print("=" * 70)


# -----------------------------------------------------------------------------
# Python -> SQL bridge: create a config table from spark.conf values.
# SQL files can then read this table to access runtime configuration.
# This is THE supported mechanism for passing Python config to SQL in SDP.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.pipeline_config_python",
    comment="Runtime pipeline configuration values from spark.conf (Python->SQL bridge)"
)
def pipeline_config_python():
    config_data = [
        ("catalog", CATALOG),
        ("schema", SCHEMA),
        ("environment", ENVIRONMENT),
        ("source_path", SOURCE_PATH),
        ("processing_mode", PROCESSING_MODE),
        ("max_files_per_trigger", MAX_FILES),
        ("enable_quality_checks", ENABLE_QUALITY),
        ("retention_days", RETENTION_DAYS),
        ("bronze_suffix", BRONZE_SUFFIX),
        ("silver_suffix", SILVER_SUFFIX),
        ("gold_suffix", GOLD_SUFFIX),
        ("target", TARGET),
        ("source", "spark.conf.get()"),
    ]
    return spark.createDataFrame(
        config_data,
        schema=StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), True),
        ])
    )


# -----------------------------------------------------------------------------
# Use runtime parameters to build dynamic table names.
# This demonstrates that Python can construct table names at RUNTIME,
# while SQL can only use deploy-time substitution.
# -----------------------------------------------------------------------------
@dlt.view(
    name="parameter_summary_view",
    comment="Summary of all runtime parameters for inspection"
)
def parameter_summary_view():
    return spark.createDataFrame(
        [(ENVIRONMENT, CATALOG, SCHEMA, PROCESSING_MODE, TARGET)],
        schema=StructType([
            StructField("environment", StringType(), False),
            StructField("catalog", StringType(), False),
            StructField("schema", StringType(), False),
            StructField("processing_mode", StringType(), False),
            StructField("target", StringType(), False),
        ])
    )


# -----------------------------------------------------------------------------
# DATA QUALITY EXPECTATION driven by a runtime parameter.
# If enable_quality_checks is 'true', enforce expectations.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.config_check_results",
    comment="Results of parameter validation checks"
)
@dlt.expect("valid_catalog", "config_value IS NOT NULL AND config_value != ''")
def config_check_results():
    return spark.table(f"{CATALOG}.{SCHEMA}.pipeline_config_python")
