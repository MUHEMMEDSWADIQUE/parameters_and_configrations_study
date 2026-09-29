# How `02_parameter_consumer.py` Works

## Purpose

Workflow Task B — consumes task values published by Task A, transforms them, and publishes new task values for Task C.

## Key Concepts Demonstrated

- **Consuming task values** — `dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")`
- **Type preservation** — task values arrive as their original Python type (dict, list, string, int)
- **Transforming task values** — building fully qualified table names, computing watermark values
- **Publishing transformed values** — chaining transformations across tasks
- **Reading values from multiple upstream tasks** — Task B reads from Task A only, but demonstrates the pattern

## How It Works

### Consuming Scalar Task Values
```python
source_table = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="source_table")
load_type = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="load_type")
```

The `taskKey` must match the `task_key` defined in `jobs.yml`. The `key` must match what was set by the upstream task.

### Consuming Complex Task Values (dict, list)
```python
full_config = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="full_config")
# full_config is a dict: {"domain": "finance", "source_table": "customers", ...}
print(full_config["domain"])  # "finance"

all_domains = dbutils.jobs.taskValues.get(taskKey="task_a_producer", key="all_domains")
# all_domains is a list of dicts
for d in all_domains:
    print(d["domain"])
```

Task values preserve their Python type. A dict set by Task A arrives as a dict in Task B — no JSON parsing needed.

### Transforming and Publishing New Values
```python
target_table = f"{catalog}.{schema}.{source_table}_bronze"  # transform
watermark_value = f"{processing_date} 00:00:00"             # transform

processing_config = {                                       # aggregate into dict
    "source_table": source_table,
    "target_table": target_table,
    "load_type": load_type,
    ...
}

dbutils.jobs.taskValues.set(key="target_table", value=target_table)
dbutils.jobs.taskValues.set(key="processing_config", value=processing_config)
```

This demonstrates the **chaining pattern**: Task A produces raw values → Task B transforms them → Task B publishes new values → Task C consumes them.
