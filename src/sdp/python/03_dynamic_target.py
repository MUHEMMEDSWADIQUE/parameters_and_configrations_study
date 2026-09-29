# =============================================================================
# 03_dynamic_target.py — Dynamic target configuration in Python (SDP)
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Build dynamic target table names from spark.conf values
#   2. Apply data quality expectations conditionally
#   3. Use runtime parameters to control partitioning and write behavior
#   4. Demonstrate the SQL -> Python data flow (reading SQL-created tables)
#
# KEY CONCEPT — SQL -> Python data flow:
#   SQL files (01-05) create tables/views in the pipeline.
#   Python files can READ those tables via spark.table() or dlt.read().
#   This is the supported mechanism for SQL -> Python data passing.
#
#   IMPORTANT: A SQL query RESULT is NOT automatically a Python variable.
#   You cannot do:  my_var = SELECT * FROM some_table
#   Instead, you read the TABLE that the SQL created:  spark.table("table_name")
# =============================================================================

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, count

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# Read runtime configuration
# -----------------------------------------------------------------------------
CATALOG = spark.conf.get("parameter_lab.catalog")
SCHEMA = spark.conf.get("parameter_lab.schema")
ENVIRONMENT = spark.conf.get("parameter_lab.environment")
SILVER_SUFFIX = spark.conf.get("parameter_lab.silver_suffix", "_silver")
GOLD_SUFFIX = spark.conf.get("parameter_lab.gold_suffix", "_gold")
ENABLE_QUALITY = spark.conf.get("parameter_lab.enable_quality_checks", "true")

print(f"[dynamic_target] CATALOG={CATALOG}, SCHEMA={SCHEMA}")
print(f"[dynamic_target] SILVER_SUFFIX={SILVER_SUFFIX}, GOLD_SUFFIX={GOLD_SUFFIX}")

# -----------------------------------------------------------------------------
# SQL -> Python: read a table created by SQL in the same pipeline.
# The SQL file 03_dynamic_target.sql created customers_silver via
# deploy-time substitution. Here, we read it via the live table reference.
#
# This demonstrates: SQL produces data -> Python reads it.
# The table name is known because both SQL and Python use the same
# ${var.catalog}.${var.schema} naming convention.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.py_customers{SILVER_SUFFIX}",
    comment="Silver layer: Python transformation reading SQL-created bronze table",
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_name", "customer_name IS NOT NULL AND customer_name != ''")
def py_customers_silver():
    # Read the bronze table created by SQL (01_parameters.sql / 02_dynamic_source.sql)
    bronze_table = f"{CATALOG}.{SCHEMA}.customers_bronze"
    print(f"[dynamic_target] Reading SQL-created table: {bronze_table}")

    try:
        df = dlt.read(bronze_table)
    except Exception:
        # Fallback: read from the Python-created bronze table
        bronze_table_py = f"{CATALOG}.{SCHEMA}.py_raw_customers_bronze"
        print(f"[dynamic_target] Fallback to Python table: {bronze_table_py}")
        df = dlt.read(bronze_table_py)

    return (
        df.withColumn("processed_by", lit("python"))
        .withColumn("processing_environment", lit(ENVIRONMENT))
        .withColumn("processed_at", current_timestamp())
    )


# -----------------------------------------------------------------------------
# Gold layer: aggregation with dynamic target name.
# The table name is constructed from runtime spark.conf values.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.py_customers{GOLD_SUFFIX}",
    comment="Gold layer: customer aggregation with runtime-derived table name",
    partition_cols=["country"] if ENABLE_QUALITY == "true" else None,
)
def py_customers_gold():
    silver_table = f"{CATALOG}.{SCHEMA}.py_customers{SILVER_SUFFIX}"
    print(f"[dynamic_target] Reading silver table: {silver_table}")

    df = dlt.read(silver_table)

    return (
        df.groupBy("country")
        .agg(
            count("*").alias("customer_count"),
            count("customer_email").alias("unique_emails"),
        )
        .withColumn("source_catalog", lit(CATALOG))
        .withColumn("source_schema", lit(SCHEMA))
        .withColumn("environment", lit(ENVIRONMENT))
    )


# -----------------------------------------------------------------------------
# Conditional quality expectations based on runtime parameter.
# This shows how runtime config controls data quality enforcement.
# -----------------------------------------------------------------------------
if ENABLE_QUALITY == "true":
    @dlt.table(
        name=f"{CATALOG}.{SCHEMA}.py_quality_report",
        comment="Quality report generated only when quality checks are enabled",
    )
    @dlt.expect_all_or_drop({
        "non_null_country": "country IS NOT NULL",
        "valid_count": "customer_count > 0",
    })
    def py_quality_report():
        gold_table = f"{CATALOG}.{SCHEMA}.py_customers{GOLD_SUFFIX}"
        return dlt.read(gold_table)
else:
    @dlt.table(
        name=f"{CATALOG}.{SCHEMA}.py_quality_report",
        comment="Placeholder: quality checks disabled in this environment",
    )
    def py_quality_report():
        return spark.createDataFrame(
            [("disabled", ENVIRONMENT)],
            schema="status STRING, environment STRING",
        )
