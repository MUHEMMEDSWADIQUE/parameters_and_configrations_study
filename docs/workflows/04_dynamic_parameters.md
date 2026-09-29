# How `04_dynamic_parameters.py` Works

## Purpose

Workflow Task D — parses a JSON parameter passed as a notebook widget, merges it with task values from upstream tasks, validates the unified configuration, and generates dynamic SQL.

## Key Concepts Demonstrated

- **JSON parameter parsing** — `json.loads()` on a widget string value
- **Merging multiple parameter sources** — JSON config + widget params + task values → unified config
- **Configuration validation** — checking required fields and valid values
- **Dynamic SQL generation** — building SQL statements from parsed JSON parameters
- **Publishing dynamic results** — generated SQL and unified config as task values

## How It Works

### Parsing the JSON Parameter
```python
json_config_str = dbutils.widgets.get("json_config")  # string from widget
config = json.loads(json_config_str)                   # now a dict
source = config.get("source")   # "azure_sql"
table = config.get("table")     # "customer"
load_type = config.get("load_type")  # "incremental"
```

The JSON string comes from `jobs.yml` → `base_parameters: json_config: ${var.json_config}`. DAB substitutes the `${var.json_config}` variable (a multi-line YAML block scalar) into the job definition.

### Merging Parameter Sources
```python
unified_config = {
    "json_source": config.get("source"),      # from JSON parameter
    "json_table": config.get("table"),         # from JSON parameter
    "widget_catalog": catalog,                 # from widget parameter
    "manifest_id": manifest_id,                 # from task value (Task C)
    "manifest_domain": manifest.get("domain"), # from task value (Task C)
}
```

This demonstrates combining **three parameter sources** into one configuration object:
1. JSON widget parameter (structured config)
2. Scalar widget parameters (catalog, schema)
3. Task values from upstream tasks (manifest from Task C)

### Validation
```python
validation_errors = []
if not unified_config["widget_catalog"]:
    validation_errors.append("widget_catalog is empty")
if unified_config["json_load_type"] not in ("incremental", "full", None):
    validation_errors.append(f"invalid load_type: {unified_config['json_load_type']}")
```

### Dynamic SQL Generation
```python
target_table_name = f"{catalog}.{schema}.{config.get('table')}_bronze"
watermark_clause = f"WHERE {config.get('watermark_column')} > '{date}'"
dynamic_sql = f"CREATE STREAMING TABLE {target_table_name} AS SELECT * FROM STREAM(...) {watermark_clause}"
dbutils.jobs.taskValues.set(key="dynamic_sql", value=dynamic_sql)
```

The SQL is generated from JSON parameters at runtime — the table name, watermark column, and WHERE clause are all derived from the parsed JSON config. This SQL is published as a task value (not executed here — it would be executed by a downstream task or the pipeline).

### Flow Summary
```
JSON (jobs.yml) → widget parameter → json.loads() → Python dict
                                                        ↓
                                                    merge with task values
                                                        ↓
                                                    validate
                                                        ↓
                                                    generate dynamic SQL
                                                        ↓
                                                    publish as task value
```
