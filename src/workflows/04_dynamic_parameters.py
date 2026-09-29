# =============================================================================
# 04_dynamic_parameters.py — Workflow Task D: Dynamic JSON Parameters
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Parse JSON configuration passed as a notebook parameter
#   2. Generate dynamic parameters from JSON at runtime
#   3. Pass dynamic parameters through to the SDP pipeline (via task values)
#   4. Demonstrate the full flow: JSON -> Python -> parsed config -> task values
#
# CONCEPT — Dynamic JSON Parameters:
#   A JSON string is passed as a notebook parameter (widget) from the
#   job definition. Python parses it into a dict, which can then be used
#   to drive dynamic behavior.
#
#   The JSON can contain nested structures, arrays, and any valid JSON type.
#   This is more flexible than individual string parameters because:
#     - You can pass complex configurations in a single parameter
#     - The structure can evolve without changing the job definition
#     - Python can validate and transform the JSON before using it
#
# FLOW:
#   JSON (in jobs.yml) -> widget parameter -> json.loads() -> Python dict
#   -> validation -> transformation -> taskValues.set() -> downstream pipeline
# =============================================================================

import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=" * 70)
print("TASK D: DYNAMIC JSON PARAMETERS")
print("=" * 70)

# -----------------------------------------------------------------------------
# 1. Read the JSON config parameter.
# This comes from jobs.yml -> job.parameters -> json_config
# or from task.base_parameters -> json_config
# The value is a STRING (all widget params are strings).
# -----------------------------------------------------------------------------
json_config_str = dbutils.widgets.get("json_config")
print(f"[widget] json_config (string, length={len(json_config_str)}):")
print(f"  {json_config_str}")

# -----------------------------------------------------------------------------
# 2. Parse the JSON string into a Python dict.
# This is the key transformation: string -> structured data.
# -----------------------------------------------------------------------------
try:
    config = json.loads(json_config_str)
    print(f"\n[parsed] JSON parsed successfully into dict with keys: {list(config.keys())}")
except json.JSONDecodeError as e:
    print(f"\n[ERROR] Failed to parse JSON: {e}")
    raise

print(f"  source:           {config.get('source')}")
print(f"  schema:           {config.get('schema')}")
print(f"  table:            {config.get('table')}")
print(f"  load_type:        {config.get('load_type')}")
print(f"  watermark_column: {config.get('watermark_column')}")

# -----------------------------------------------------------------------------
# 3. Read other task parameters.
# -----------------------------------------------------------------------------
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
print(f"\n[widget] catalog = {catalog}")
print(f"[widget] schema = {schema}")

# -----------------------------------------------------------------------------
# 4. Consume task values from Task C (the processing manifest).
# -----------------------------------------------------------------------------
print("\n--- Consuming processing manifest from Task C ---")
try:
    manifest = dbutils.jobs.taskValues.get(taskKey="task_c_task_values", key="processing_manifest")
    manifest_id = dbutils.jobs.taskValues.get(taskKey="task_c_task_values", key="manifest_id")
    print(f"  manifest_id = {manifest_id}")
    print(f"  manifest domain = {manifest.get('domain')}")
except Exception as e:
    print(f"  Could not read manifest from Task C: {e}")
    manifest = None
    manifest_id = None

# -----------------------------------------------------------------------------
# 5. Merge JSON config with task values to create a unified configuration.
# This demonstrates combining multiple parameter sources.
# -----------------------------------------------------------------------------
print("\n--- Merging JSON config with task values ---")

unified_config = {
    # From JSON parameter
    "json_source": config.get("source"),
    "json_schema": config.get("schema"),
    "json_table": config.get("table"),
    "json_load_type": config.get("load_type"),
    "json_watermark_column": config.get("watermark_column"),
    # From widget parameters
    "widget_catalog": catalog,
    "widget_schema": schema,
    # From task values (upstream)
    "manifest_id": manifest_id,
    "manifest_domain": manifest.get("domain") if manifest else None,
    "manifest_target": manifest.get("target", {}).get("table") if manifest else None,
    # Metadata
    "generated_at": datetime.now().isoformat(),
    "config_source": "json_parameter + widget_params + task_values",
}

print(f"  Unified config keys: {list(unified_config.keys())}")

# -----------------------------------------------------------------------------
# 6. Validate the merged configuration.
# Check for required fields and consistency.
# -----------------------------------------------------------------------------
print("\n--- Validating unified configuration ---")

validation_errors = []

if not unified_config["widget_catalog"]:
    validation_errors.append("widget_catalog is empty")
if not unified_config["widget_schema"]:
    validation_errors.append("widget_schema is empty")
if not unified_config["json_table"]:
    validation_errors.append("json_table is empty")
if unified_config["json_load_type"] not in ("incremental", "full", None):
    validation_errors.append(f"json_load_type has invalid value: {unified_config['json_load_type']}")

if validation_errors:
    print(f"  VALIDATION FAILED: {validation_errors}")
    # In production, you might raise an exception or publish an error task value
    dbutils.jobs.taskValues.set(key="validation_status", value="FAILED")
    dbutils.jobs.taskValues.set(key="validation_errors", value=validation_errors)
else:
    print("  VALIDATION PASSED")
    dbutils.jobs.taskValues.set(key="validation_status", value="PASSED")

# -----------------------------------------------------------------------------
# 7. Build dynamic SQL from the configuration.
# Demonstrate that JSON parameters can drive SQL generation.
# This is NOT executed here — it's published as a task value for reference.
# -----------------------------------------------------------------------------
print("\n--- Generating dynamic SQL from configuration ---")

target_table_name = f"{catalog}.{schema}.{config.get('table', 'unknown')}_bronze"
watermark_clause = f"WHERE {config.get('watermark_column', 'updated_at')} > '{datetime.now().date()}'" if config.get("load_type") == "incremental" else ""

dynamic_sql = f"""
-- Generated dynamically from JSON configuration
CREATE OR REFRESH STREAMING TABLE {target_table_name}
AS
SELECT *
FROM STREAM({catalog}.{schema}.source_{config.get('table', 'unknown')})
{watermark_clause};
""".strip()

print(f"  Generated SQL:\n{dynamic_sql}")

# Publish the dynamic SQL as a task value
dbutils.jobs.taskValues.set(key="dynamic_sql", value=dynamic_sql)
dbutils.jobs.taskValues.set(key="unified_config", value=unified_config)
dbutils.jobs.taskValues.set(key="target_table_name", value=target_table_name)

# -----------------------------------------------------------------------------
# 8. Publish pipeline parameters for Task E (the pipeline trigger).
# Pipeline tasks cannot read task values directly, but we publish them
# anyway for logging and potential future use.
# -----------------------------------------------------------------------------
print("\n--- Publishing pipeline launch parameters ---")

pipeline_params = {
    "catalog": catalog,
    "schema": schema,
    "source_table": config.get("table"),
    "target_table": target_table_name,
    "load_type": config.get("load_type", "incremental"),
    "watermark_column": config.get("watermark_column"),
    "manifest_id": manifest_id,
    "validation_status": unified_config.get("validation_status", "PASSED"),
}

dbutils.jobs.taskValues.set(key="pipeline_params", value=pipeline_params)
print(f"  Published pipeline_params: {json.dumps(pipeline_params, indent=2)}")

print("\n" + "=" * 70)
print("TASK D COMPLETE — dynamic parameters generated and published")
print("=" * 70)
