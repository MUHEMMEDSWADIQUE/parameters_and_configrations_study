# =============================================================================
# 05_metadata_driven.py — Metadata-driven configuration in Python (SDP)
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Create a metadata configuration Delta table
#   2. Read metadata and dynamically determine processing behavior
#   3. Parse JSON parameters from the metadata table
#   4. Generate dynamic SQL or table operations from metadata
#   5. Demonstrate the full metadata -> Python -> SQL flow
#
# CONCEPT — Metadata-driven configuration:
#   Instead of hard-coding table names, source paths, and load types,
#   store them in a Delta table. Python reads this table at runtime
#   and dynamically creates pipeline datasets.
#
#   This is the most flexible approach:
#     - Add a new domain by INSERTing a row into the metadata table
#     - No code changes needed — Python reads the metadata dynamically
#     - JSON column allows arbitrary key-value parameters per domain
# =============================================================================

import dlt
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# Read runtime configuration
# -----------------------------------------------------------------------------
CATALOG = spark.conf.get("parameter_lab.catalog")
SCHEMA = spark.conf.get("parameter_lab.schema")
ENVIRONMENT = spark.conf.get("parameter_lab.environment")

print(f"[metadata_driven] CATALOG={CATALOG}, SCHEMA={SCHEMA}, ENV={ENVIRONMENT}")

# -----------------------------------------------------------------------------
# Create the metadata configuration table.
# This table drives ALL downstream processing behavior.
#
# Columns:
#   domain              - business domain (e.g., 'finance', 'retail')
#   source_system       - source system name (e.g., 'azure_sql', 'salesforce')
#   source_table        - table name in the source system
#   target_table        - target table name in Databricks
#   source_path         - volume path for file-based sources
#   target_path         - output path (optional, UC tables are preferred)
#   load_type           - incremental | full
#   watermark_column    - column for incremental watermarking
#   primary_key         - primary key for deduplication
#   active_flag         - whether this domain is currently active
#   processing_sequence - order of processing (lower = earlier)
#   parameters          - JSON column with domain-specific params
# -----------------------------------------------------------------------------
metadata_schema = StructType([
    StructField("domain", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("source_path", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("load_type", StringType(), False),
    StructField("watermark_column", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("active_flag", BooleanType(), False),
    StructField("processing_sequence", IntegerType(), False),
    StructField("parameters", StringType(), True),  # JSON string
])

finance_params = json.dumps({"partition_column": "country", "max_files": 100, "quality_check": True})
retail_params = json.dumps({"partition_column": "order_date", "max_files": 200, "quality_check": True})
hr_params = json.dumps({"partition_column": None, "max_files": 50, "quality_check": False})
inventory_params = json.dumps({"partition_column": "category", "max_files": 100, "quality_check": True})

metadata_data = [
    ("finance", "azure_sql", "customers", "customers_bronze",
     f"{CATALOG}.{SCHEMA}.source_customers", None,
     "incremental", "modified_date", "customer_id",
     True, 1, finance_params),
    ("retail", "azure_sql", "orders", "orders_bronze",
     f"{CATALOG}.{SCHEMA}.source_orders", None,
     "incremental", "modified_date", "order_id",
     True, 2, retail_params),
    ("hr", "salesforce", "employees", "employees_bronze",
     f"{CATALOG}.{SCHEMA}.source_employees", None,
     "full", None, "employee_id",
     True, 3, hr_params),
    ("inventory", "azure_sql", "products", "products_bronze",
     f"{CATALOG}.{SCHEMA}.source_products", None,
     "incremental", "updated_at", "product_id",
     False, 4, inventory_params),  # INACTIVE — skipped at runtime
]

@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.parameter_config",
    comment="Metadata configuration table — drives all pipeline processing behavior",
)
def parameter_config():
    return spark.createDataFrame(metadata_data, schema=metadata_schema)


# -----------------------------------------------------------------------------
# Read active metadata and process each domain.
# This view shows which domains will be processed.
# -----------------------------------------------------------------------------
@dlt.view(
    name="active_metadata_view",
    comment="Active domains from metadata table (active_flag = true)",
)
def active_metadata_view():
    config_table = f"{CATALOG}.{SCHEMA}.parameter_config"
    return (
        dlt.read(config_table)
        .filter(col("active_flag") == True)
        .orderBy("processing_sequence")
    )


# -----------------------------------------------------------------------------
# Parse JSON parameters from metadata.
# The 'parameters' column is a JSON string. We parse it into structured columns.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.parsed_metadata",
    comment="Metadata with parsed JSON parameters",
)
def parsed_metadata():
    json_schema = StructType([
        StructField("partition_column", StringType(), True),
        StructField("max_files", IntegerType(), True),
        StructField("quality_check", BooleanType(), True),
    ])

    config_table = f"{CATALOG}.{SCHEMA}.parameter_config"
    return (
        dlt.read(config_table)
        .filter(col("active_flag") == True)
        .withColumn("parsed_params", from_json(col("parameters"), json_schema))
        .select(
            "domain",
            "source_system",
            "source_table",
            "target_table",
            "load_type",
            "watermark_column",
            "primary_key",
            "processing_sequence",
            col("parsed_params.partition_column").alias("partition_column"),
            col("parsed_params.max_files").alias("max_files"),
            col("parsed_params.quality_check").alias("quality_check"),
            col("parameters").alias("raw_json_parameters"),
        )
    )


# -----------------------------------------------------------------------------
# Generate a processing plan from metadata.
# This table summarizes what the pipeline WILL DO based on metadata.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.processing_plan",
    comment="Dynamic processing plan generated from metadata",
)
def processing_plan():
    config_table = f"{CATALOG}.{SCHEMA}.parameter_config"
    df = dlt.read(config_table).filter(col("active_flag") == True)

    return (
        df.withColumn(
            "action",
            when(col("load_type") == "incremental", "STREAMING_LOAD")
            .otherwise("BATCH_FULL_LOAD")
        )
        .withColumn(
            "source_fqn",
            col("source_path")
        )
        .withColumn(
            "target_fqn",
            lit(f"{CATALOG}.{SCHEMA}.") + col("target_table")
        )
        .withColumn(
            "quality_action",
            when(col("load_type").isNotNull(), "CHECK_JSON_PARAMS")
            .otherwise("SKIP_EXPECTATIONS")
        )
        .select(
            "domain",
            "source_system",
            "source_table",
            "target_table",
            "load_type",
            "action",
            "source_fqn",
            "target_fqn",
            "watermark_column",
            "primary_key",
            "processing_sequence",
            "quality_action",
            lit(ENVIRONMENT).alias("environment"),
        )
        .orderBy("processing_sequence")
    )


# -----------------------------------------------------------------------------
# Print the processing plan for visibility.
# -----------------------------------------------------------------------------
print("=" * 70)
print("METADATA-DRIVEN PROCESSING PLAN")
print("=" * 70)
plan_df = spark.createDataFrame(metadata_data, schema=metadata_schema).filter(col("active_flag") == True)
plan_rows = plan_df.collect()
for row in plan_rows:
    print(f"  [{row.processing_sequence}] {row.domain}: {row.source_table} -> {row.target_table} ({row.load_type})")
print("=" * 70)
