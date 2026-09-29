# How `01_parameter_producer.py` Works

## Purpose

Workflow Task A — reads parameters from multiple sources (widgets, spark.conf), reads a metadata table, and publishes task values for downstream tasks.

## Key Concepts Demonstrated

- **Three parameter sources in one task**: job parameters (widgets), task parameters (widgets), spark configuration
- **`dbutils.widgets.get()`** — reads notebook parameters from the job definition
- **`spark.conf.get()`** — reads spark_conf from the task definition
- **`dbutils.jobs.taskValues.set()`** — publishes runtime values for downstream tasks
- **Complex task value types** — publishes dicts and lists, not just strings
- **Parameters vs task values** — parameters are pre-defined; task values are runtime-generated

## How It Works

### Reading Job Parameters (Level 1)
```python
catalog_job = dbutils.widgets.get("catalog")  # from job.parameters in jobs.yml
schema_job = dbutils.widgets.get("schema")
```
These come from the `parameters:` section of `jobs.yml` and are available to ALL tasks.

### Reading Task Parameters (Level 2 — overrides job params)
```python
environment = dbutils.widgets.get("environment")  # from base_parameters
source_path = dbutils.widgets.get("source_path")
```
These come from the `base_parameters:` section of the task in `jobs.yml`. If a task param has the same name as a job param, the task param wins.

### Reading Spark Configuration (Level 3 — separate from widgets)
```python
task_name = spark.conf.get("parameter_lab.task")  # from spark_conf in jobs.yml
```
This is a Spark-level config, NOT a notebook widget. Accessed via `spark.conf.get()`, not `dbutils.widgets.get()`.

### Publishing Task Values (Runtime — for downstream tasks)
```python
dbutils.jobs.taskValues.set(key="source_table", value="customers")
dbutils.jobs.taskValues.set(key="full_config", value={"domain": "finance", ...})
dbutils.jobs.taskValues.set(key="all_domains", value=[{...}, {...}])
```

Task values can be ANY JSON type: strings, dicts, lists, ints, bools, None. They are ephemeral — they exist only during this job run.

### Parameter Source Comparison

| Source | Access Method | Type | When Set |
|---|---|---|---|
| Job parameters | `dbutils.widgets.get()` | String | Before job run (YAML) |
| Task parameters | `dbutils.widgets.get()` | String | Before job run (YAML) |
| Spark config | `spark.conf.get()` | String | Before job run (YAML) |
| Task values | `dbutils.jobs.taskValues.set/get()` | Any JSON | At runtime |
