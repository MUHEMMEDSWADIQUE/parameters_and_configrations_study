# How `03_task_values.py` Works

## Purpose

Workflow Task C — demonstrates advanced task value patterns: consuming from multiple upstream tasks, publishing all value types, building a processing manifest, and error handling for missing values.

## Key Concepts Demonstrated

- **Consuming from multiple upstream tasks** — Task C reads values from BOTH Task A and Task B
- **All task value types** — string, int, float, bool, None, list, dict
- **Complex object construction** — building a nested processing manifest dict from multiple sources
- **Error handling** — using the `default` parameter for missing task values
- **Parameters vs task values summary** — printed as a comparison table

## How It Works

### Reading from Multiple Upstream Tasks
```python
# From Task A
domain = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="domain")
source_table = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")

# From Task B
target_table = dbutils.jobs.taskValues.get(taskKey="task_b_consumer", key="target_table")
processing_config = dbutils.jobs.taskValues.get(taskKey="task_b_consumer", key="processing_config")
```

Task C depends on Task B (which depends on Task A), so both upstream tasks have completed. Task C can read values from both.

### All Value Types
```python
dbutils.jobs.taskValues.set(key="string_value", value="hello")        # str
dbutils.jobs.taskValues.set(key="int_value", value=42)               # int
dbutils.jobs.taskValues.set(key="float_value", value=3.14)            # float
dbutils.jobs.taskValues.set(key="bool_value", value=True)            # bool
dbutils.jobs.taskValues.set(key="none_value", value=None)            # None
dbutils.jobs.taskValues.set(key="list_value", value=["a", "b"])     # list
dbutils.jobs.taskValues.set(key="dict_value", value={"k": "v"})    # dict
```

### Building a Processing Manifest
```python
manifest = {
    "manifest_id": f"{timestamp}_{domain}",
    "source": {"table": source_table, "path": processing_config["source_path"]},
    "target": {"table": target_table, "catalog": catalog, "schema": schema},
    "processing": {"load_type": load_type, "watermark_column": ...},
    "lineage": {"task_a": "producer", "task_b": "consumer", "task_c": "task_values"}
}
dbutils.jobs.taskValues.set(key="processing_manifest", value=manifest)
```

This demonstrates combining values from multiple upstream tasks into a single structured manifest.

### Error Handling for Missing Values
```python
missing = dbutils.jobs.taskValues.get(
    taskKey="task_a_producer", key="nonexistent", default="KEY_NOT_FOUND"
)
```

With `default`, missing values return the default instead of raising. Without `default`, a missing value raises an exception.

### Limitations Documented

- Task values are **ephemeral** — only exist during the job run
- They are **not available** across different job runs
- They are **not available** to the SDP pipeline (pipeline tasks can't read them)
- Maximum size per task value is ~1 MB
