# =============================================================================
# 03_task_values.py — Workflow Task C: Advanced Task Value Patterns
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Consume task values from multiple upstream tasks
#   2. Demonstrate all task value types (string, int, dict, list, bool)
#   3. Chain task value transformations across tasks
#   4. Demonstrate the difference between task values and parameters
#   5. Show error handling when task values are missing
#
# TASK VALUE API:
#   dbutils.jobs.taskValues.set(key, value, debug_file=None)
#   dbutils.jobs.taskValues.get(taskKey, key, default=None, debugValue=None)
#
# SUPPORTED VALUE TYPES:
#   - str, int, float, bool
#   - dict, list (JSON-serializable)
#   - None (null)
#
# LIMITATIONS:
#   - Task values are ephemeral — they exist only during the job run.
#   - They are NOT available across different job runs.
#   - They are NOT available to the SDP pipeline (pipeline tasks cannot
#     read task values from upstream workflow tasks).
#   - The maximum size per task value is ~1 MB.
# =============================================================================

import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=" * 70)
print("TASK C: ADVANCED TASK VALUE PATTERNS")
print("=" * 70)

# -----------------------------------------------------------------------------
# 1. Consume values from BOTH Task A and Task B.
# Task C depends on Task B, which depends on Task A.
# Task C can read values from BOTH upstream tasks.
# -----------------------------------------------------------------------------
print("\n--- Consuming task values from Task A ---")

domain = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="domain")
source_table = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
print(f"  [A] domain = {domain}")
print(f"  [A] source_table = {source_table}")

print("\n--- Consuming task values from Task B ---")

target_table = dbutils.jobs.taskValues.get(taskKey="task_b_consumer", key="target_table")
watermark_value = dbutils.jobs.taskValues.get(taskKey="task_b_consumer", key="watermark_value")
processing_config = dbutils.jobs.taskValues.get(taskKey="task_b_consumer", key="processing_config")
print(f"  [B] target_table = {target_table}")
print(f"  [B] watermark_value = {watermark_value}")
print(f"  [B] processing_config = {json.dumps(processing_config, indent=2)}")

# -----------------------------------------------------------------------------
# 2. Demonstrate ALL task value types.
# Publish values of different types to show type preservation.
# -----------------------------------------------------------------------------
print("\n--- Demonstrating all task value types ---")

dbutils.jobs.taskValues.set(key="string_value", value="hello_from_task_c")
dbutils.jobs.taskValues.set(key="int_value", value=42)
dbutils.jobs.taskValues.set(key="float_value", value=3.14159)
dbutils.jobs.taskValues.set(key="bool_value", value=True)
dbutils.jobs.taskValues.set(key="none_value", value=None)
dbutils.jobs.taskValues.set(key="list_value", value=["a", "b", "c"])
dbutils.jobs.taskValues.set(key="dict_value", value={"nested": {"deep": "value"}})

print("  Published: string_value = 'hello_from_task_c' (str)")
print("  Published: int_value = 42 (int)")
print("  Published: float_value = 3.14159 (float)")
print("  Published: bool_value = True (bool)")
print("  Published: none_value = None (null)")
print("  Published: list_value = ['a', 'b', 'c'] (list)")
print("  Published: dict_value = {'nested': {'deep': 'value'}} (dict)")

# -----------------------------------------------------------------------------
# 3. Chain transformations across all three tasks.
# A: domain='finance', source_table='customers'
# B: target_table='main.parameter_lab_dev.customers_bronze'
# C: build final processing manifest
# -----------------------------------------------------------------------------
print("\n--- Building final processing manifest ---")

manifest = {
    "manifest_id": f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{domain}",
    "created_at": datetime.now().isoformat(),
    "domain": domain,
    "source": {
        "table": source_table,
        "path": processing_config["source_path"],
    },
    "target": {
        "table": target_table,
        "catalog": processing_config["catalog"],
        "schema": processing_config["schema"],
    },
    "processing": {
        "load_type": processing_config["load_type"],
        "watermark_column": processing_config["watermark_column"],
        "watermark_value": watermark_value,
        "processing_date": processing_config["processing_date"],
    },
    "lineage": {
        "task_a": "parameter_producer",
        "task_b": "parameter_consumer",
        "task_c": "task_values",
    },
}

dbutils.jobs.taskValues.set(key="processing_manifest", value=manifest)
dbutils.jobs.taskValues.set(key="manifest_id", value=manifest["manifest_id"])

print(f"  Published: processing_manifest (dict with {len(manifest)} top-level keys)")
print(f"  Published: manifest_id = {manifest['manifest_id']}")

# -----------------------------------------------------------------------------
# 4. Error handling: what happens when a task value is missing?
# Use the `default` parameter to handle missing values gracefully.
# -----------------------------------------------------------------------------
print("\n--- Error handling for missing task values ---")

# With default: returns the default if the key doesn't exist
missing_value = dbutils.jobs.taskValues.get(
    taskKey="task_a_producer",
    key="nonexistent_key",
    default="KEY_NOT_FOUND"
)
print(f"  Missing key with default: {missing_value}")

# Without default: raises an exception if the key doesn't exist
# Uncomment to test:
# try:
#     dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="nonexistent_key")
# except Exception as e:
#     print(f"  Missing key without default raises: {e}")

# -----------------------------------------------------------------------------
# 5. SUMMARY: Parameters vs Task Values vs Spark Conf
# -----------------------------------------------------------------------------
print("\n" + "=" * 70)
print("SUMMARY: Parameter Passing Mechanisms")
print("=" * 70)
print("""
  +----------------+------------------+------------------+------------------+
  | Mechanism      | When Set         | Scope            | Type             |
  +----------------+------------------+------------------+------------------+
  | Job Parameters | Before job run   | All tasks        | String only      |
  | (widgets)      | (in jobs.yml)    |                  |                  |
  +----------------+------------------+------------------+------------------+
  | Task Params    | Before job run   | Single task      | String only      |
  | (base_params)  | (in jobs.yml)    |                  |                  |
  +----------------+------------------+------------------+------------------+
  | Task Values    | At RUNTIME       | Downstream tasks | Any JSON type    |
  | (taskValues)   | (in task code)   |                  |                  |
  +----------------+------------------+------------------+------------------+
  | Spark Conf     | Before job run   | Single task's    | String (get)     |
  | (spark_conf)   | (in jobs.yml)    | cluster          |                  |
  +----------------+------------------+------------------+------------------+
  | DAB Variables  | At DEPLOY time   | Bundle resources | String           |
  | (${var.xxx})   | (databricks.yml) | (baked into YML) |                  |
  +----------------+------------------+------------------+------------------+
""")

print("TASK C COMPLETE — manifest published for Task D")
print("=" * 70)
