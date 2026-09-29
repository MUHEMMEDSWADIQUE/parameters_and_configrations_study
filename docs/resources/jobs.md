# How `resources/jobs.yml` Works

## Purpose

Defines the `parameter_lab_job` resource — a Databricks Workflow with 5 chained tasks demonstrating parameter passing and task values.

## Key Concepts Demonstrated

- **Job parameters** (`parameters:` section) — available to all tasks as widgets
- **Task parameters** (`base_parameters:`) — per-task widget overrides
- **Spark configuration** (`spark_conf:`) — cluster-level configs per task
- **Task dependencies** (`depends_on:`) — defines execution order
- **Pipeline task** — triggers the SDP pipeline from within the workflow
- **Deploy-time substitution** — `${var.xxx}` in the YAML

## Task Flow

```
Task A (task_a_producer)
  → reads metadata, publishes task values
  ↓
Task B (task_b_consumer)
  → consumes Task A values, transforms, publishes new values
  ↓
Task C (task_c_task_values)
  → advanced task value patterns, builds processing manifest
  ↓
Task D (task_d_dynamic_params)
  → parses JSON config, generates dynamic parameters
  ↓
Task E (task_e_pipeline)
  → triggers the SDP pipeline
```

## How Parameter Passing Works in This Job

### Job Parameters (level 1)
Defined in the `parameters:` section. Every task sees these via `dbutils.widgets.get()`.
```yaml
parameters:
  - name: catalog
    default: ${var.catalog}
```
In Python: `catalog = dbutils.widgets.get("catalog")`

### Task Parameters (level 2 — overrides job params)
Defined per-task in `base_parameters:`. If a task defines the same param name as a job param, the task value wins.
```yaml
notebook_task:
  base_parameters:
    catalog: ${var.catalog}
    environment: ${var.environment}
```

### Spark Configuration (level 3 — separate from widgets)
Defined per-task in `spark_conf:`. Accessed via `spark.conf.get()`, NOT `dbutils.widgets.get()`.
```yaml
spark_conf:
  parameter_lab.task: task_a_producer
  parameter_lab.environment: ${var.environment}
```
In Python: `task_name = spark.conf.get("parameter_lab.task")`

### Task Values (runtime — not in YAML)
Set and consumed at runtime by Python code. Not declared in YAML — the YAML only defines dependencies.
```python
# Set in Task A
dbutils.jobs.taskValues.set(key="source_table", value="customers")
# Get in Task B
dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
```

## Pipeline Task Limitation

Task E triggers the pipeline via `pipeline_task:`. Pipeline tasks **cannot** read task values or notebook parameters directly. The pipeline gets its configuration from the `configuration:` section in `pipelines.yml`, not from upstream workflow tasks.
