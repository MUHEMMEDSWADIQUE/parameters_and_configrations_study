# =============================================================================
# 02_parameter_consumer.py — Workflow Task B: Parameter Consumer
# =============================================================================
# LEARNING OBJECTIVES:
#   1. CONSUME task values published by Task A
#   2. Transform task values and publish new ones for Task C
#   3. Understand the difference between reading parameters (widgets)
#      and reading task values
#   4. Demonstrate parameter precedence: job param vs task param
#
# KEY CONCEPT — Reading task values from upstream tasks:
#   dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
#
#   The taskKey MUST match the task_key defined in jobs.yml.
#   The key MUST match what was set by the upstream task.
#
# PARAMETER vs TASK VALUE (recap):
#   - Parameters (widgets): defined before run, available to all tasks,
#     always strings, set in the job definition YAML.
#   - Task values: generated at runtime by a specific task, available to
#     downstream tasks, can be any JSON type, ephemeral (not persisted).
# =============================================================================

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=" * 70)
print("TASK B: PARAMETER CONSUMER")
print("=" * 70)

# -----------------------------------------------------------------------------
# 1. Read JOB PARAMETERS (from job.parameters in jobs.yml).
# These are the same job-level params available to Task A.
# -----------------------------------------------------------------------------
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
print(f"[job_param] catalog = {catalog}")
print(f"[job_param] schema = {schema}")

# -----------------------------------------------------------------------------
# 2. Read TASK PARAMETERS (from task.base_parameters in jobs.yml).
# Task B has catalog and schema in base_parameters, which override job params.
# Since Task B's base_parameters only has catalog and schema (no environment),
# the environment param comes from the job-level parameter.
# -----------------------------------------------------------------------------
try:
    environment = dbutils.widgets.get("environment")
    print(f"[job_param] environment = {environment} (from job-level, not in task params)")
except Exception:
    print("[job_param] environment not available")

# -----------------------------------------------------------------------------
# 3. CONSUME TASK VALUES from Task A.
# This is the PRIMARY mechanism for passing runtime values between tasks.
# -----------------------------------------------------------------------------
print("\n--- Consuming task values from task_a_producer ---")

source_table = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
load_type = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="load_type")
watermark = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="watermark")
processing_date = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="processing_date")
source_path = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_path")
domain = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="domain")

print(f"  [task_value] source_table    = {source_table}")
print(f"  [task_value] load_type       = {load_type}")
print(f"  [task_value] watermark        = {watermark}")
print(f"  [task_value] processing_date  = {processing_date}")
print(f"  [task_value] source_path     = {source_path}")
print(f"  [task_value] domain          = {domain}")

# -----------------------------------------------------------------------------
# 4. Consume COMPLEX task values (dict, list).
# Task A published a dict (full_config) and a list (all_domains).
# These arrive as Python dicts/lists, NOT strings.
# -----------------------------------------------------------------------------
print("\n--- Consuming complex task values ---")

full_config = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="full_config")
all_domains = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="all_domains")

print(f"  [task_value] full_config (type={type(full_config).__name__}):")
print(f"    {json.dumps(full_config, indent=4)}")
print(f"  [task_value] all_domains (type={type(all_domains).__name__}, len={len(all_domains)}):")
for d in all_domains:
    print(f"    - {d['domain']}: {d['source_table']}")

# -----------------------------------------------------------------------------
# 5. TRANSFORM task values and publish new ones for Task C.
# Task B takes the values from Task A, transforms them, and publishes
# new values for downstream consumption.
# -----------------------------------------------------------------------------
print("\n--- Transforming and publishing new task values ---")

# Transform: build a fully qualified table name
target_table = f"{catalog}.{schema}.{source_table}_bronze"
print(f"  Transformed: target_table = {target_table}")

# Transform: determine watermark value (simulated)
watermark_value = f"{processing_date} 00:00:00"
print(f"  Transformed: watermark_value = {watermark_value}")

# Transform: build a processing config dict
processing_config = {
    "source_table": source_table,
    "target_table": target_table,
    "load_type": load_type,
    "watermark_column": watermark,
    "watermark_value": watermark_value,
    "source_path": source_path,
    "domain": domain,
    "catalog": catalog,
    "schema": schema,
    "processing_date": processing_date,
}

# Publish transformed values
dbutils.jobs.taskValues.set(key="target_table", value=target_table)
dbutils.jobs.taskValues.set(key="watermark_value", value=watermark_value)
dbutils.jobs.taskValues.set(key="processing_config", value=processing_config)

print(f"  Published: target_table = {target_table}")
print(f"  Published: watermark_value = {watermark_value}")
print(f"  Published: processing_config (dict with {len(processing_config)} keys)")

# -----------------------------------------------------------------------------
# 6. Read Spark configuration (different from widgets and task values).
# -----------------------------------------------------------------------------
print("\n--- Reading spark.conf (separate from widgets/task values) ---")
try:
    task_name = spark.conf.get("parameter_lab.task")
    print(f"  [spark_conf] parameter_lab.task = {task_name}")
except Exception:
    print("  [spark_conf] parameter_lab.task not set")

print("\n" + "=" * 70)
print("TASK B COMPLETE — consumed Task A values, published transformed values")
print("=" * 70)
