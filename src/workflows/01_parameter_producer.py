# =============================================================================
# 01_parameter_producer.py — Workflow Task A: Parameter Producer
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Read notebook parameters (widgets) set by the job definition
#   2. Read Spark configuration set in the job task definition
#   3. Read a metadata table and produce task values for downstream tasks
#   4. Publish task values using dbutils.jobs.taskValues.set()
#
# PARAMETER SOURCES AVAILABLE IN THIS TASK:
#
#   A) JOB PARAMETERS (from job.parameters in jobs.yml):
#      Access: dbutils.widgets.get("catalog")
#      These are available to ALL tasks in the job.
#
#   B) TASK PARAMETERS (from task.base_parameters in jobs.yml):
#      Access: dbutils.widgets.get("environment")
#      These override job parameters for this specific task.
#      Precedence: task params > job params
#
#   C) SPARK CONFIGURATION (from task.spark_conf in jobs.yml):
#      Access: spark.conf.get("parameter_lab.task")
#      These are Spark-level configs, NOT the same as widget params.
#
#   D) TASK VALUES (set by THIS task, read by DOWNSTREAM tasks):
#      Set: dbutils.jobs.taskValues.set(key="source_table", value="customers")
#      Get (in downstream task): dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
#
# PARAMETER vs TASK VALUE:
#   - Parameters (widgets) are defined BEFORE the job runs (in the job definition).
#   - Task values are generated AT RUNTIME by a task and consumed by downstream tasks.
#   - Parameters are strings; task values can be any JSON-serializable type.
# =============================================================================

import json
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# 1. Read JOB PARAMETERS via widgets.
# These come from job.parameters in jobs.yml.
# -----------------------------------------------------------------------------
print("=" * 70)
print("TASK A: PARAMETER PRODUCER")
print("=" * 70)

# Job-level parameters (available to all tasks)
catalog_job = dbutils.widgets.get("catalog")
schema_job = dbutils.widgets.get("schema")
print(f"[job_param] catalog = {catalog_job}")
print(f"[job_param] schema = {schema_job}")

# -----------------------------------------------------------------------------
# 2. Read TASK PARAMETERS via widgets.
# These come from task.base_parameters in jobs.yml.
# Task params OVERRIDE job params when they share the same name.
# -----------------------------------------------------------------------------
environment = dbutils.widgets.get("environment")
source_path = dbutils.widgets.get("source_path")
print(f"[task_param] environment = {environment}")
print(f"[task_param] source_path = {source_path}")

# -----------------------------------------------------------------------------
# 3. Read SPARK CONFIGURATION.
# These come from task.spark_conf in jobs.yml.
# IMPORTANT: spark.conf is NOT the same as widget parameters.
#   - Widgets: string values, set by the job framework
#   - spark.conf: Spark-level configs, set on the cluster
# -----------------------------------------------------------------------------
try:
    task_name = spark.conf.get("parameter_lab.task")
    print(f"[spark_conf] parameter_lab.task = {task_name}")
except Exception:
    print("[spark_conf] parameter_lab.task not set (may be running outside job)")

try:
    env_from_conf = spark.conf.get("parameter_lab.environment")
    print(f"[spark_conf] parameter_lab.environment = {env_from_conf}")
except Exception:
    print("[spark_conf] parameter_lab.environment not set")

# -----------------------------------------------------------------------------
# 4. Read metadata table to determine processing parameters.
# In a real scenario, this would read from a Delta table.
# Here we simulate it with a static DataFrame for demonstration.
# -----------------------------------------------------------------------------
print("\n--- Reading metadata to determine task values ---")

# Simulated metadata (in production, read from a Delta table)
metadata = [
    {"domain": "finance", "source_table": "customers", "load_type": "incremental",
     "watermark": "modified_date", "processing_date": str(date.today()),
     "source_path": f"{source_path}/customers"},
    {"domain": "retail", "source_table": "orders", "load_type": "incremental",
     "watermark": "modified_date", "processing_date": str(date.today()),
     "source_path": f"{source_path}/orders"},
]

# Select the first active domain for this run
selected = metadata[0]
print(f"  Selected domain: {selected['domain']}")
print(f"  source_table:    {selected['source_table']}")
print(f"  load_type:       {selected['load_type']}")
print(f"  watermark:       {selected['watermark']}")
print(f"  processing_date: {selected['processing_date']}")
print(f"  source_path:     {selected['source_path']}")

# -----------------------------------------------------------------------------
# 5. PUBLISH TASK VALUES for downstream tasks.
# These are RUNTIME values — NOT parameters. They are set by this task
# and can be read by any downstream task via dbutils.jobs.taskValues.get().
#
# dbutils.jobs.taskValues.set(key, value):
#   key:   string name of the value
#   value: any JSON-serializable type (string, int, dict, list, etc.)
# -----------------------------------------------------------------------------
print("\n--- Publishing task values ---")

# Individual scalar values
dbutils.jobs.taskValues.set(key="source_table", value=selected["source_table"])
dbutils.jobs.taskValues.set(key="load_type", value=selected["load_type"])
dbutils.jobs.taskValues.set(key="watermark", value=selected["watermark"])
dbutils.jobs.taskValues.set(key="processing_date", value=selected["processing_date"])
dbutils.jobs.taskValues.set(key="source_path", value=selected["source_path"])
dbutils.jobs.taskValues.set(key="domain", value=selected["domain"])

# You can also publish complex types (dict, list)
dbutils.jobs.taskValues.set(key="full_config", value=selected)
dbutils.jobs.taskValues.set(key="all_domains", value=metadata)

print(f"  Published: source_table={selected['source_table']}")
print(f"  Published: load_type={selected['load_type']}")
print(f"  Published: watermark={selected['watermark']}")
print(f"  Published: processing_date={selected['processing_date']}")
print(f"  Published: source_path={selected['source_path']}")
print(f"  Published: domain={selected['domain']}")
print(f"  Published: full_config={json.dumps(selected, indent=2)}")
print(f"  Published: all_domains ({len(metadata)} domains)")

# -----------------------------------------------------------------------------
# 6. Also write results to a Delta table for persistence.
# Task values are ephemeral (only available during the job run).
# Writing to a table makes them persistent.
# -----------------------------------------------------------------------------
print("\n--- Writing metadata to Delta table for persistence ---")
target_table = f"{catalog_job}.{schema_job}.task_a_output"
print(f"  Target table: {target_table}")

# In a real scenario, you would write to the table here.
# df = spark.createDataFrame([selected])
# df.write.mode("overwrite").saveAsTable(target_table)

print("\n" + "=" * 70)
print("TASK A COMPLETE — task values published for downstream tasks")
print("=" * 70)
