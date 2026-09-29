# How `01_parameters.py` Works

## Purpose

Demonstrates runtime parameter access in Python SDP via `spark.conf.get()` and creates the Python → SQL config bridge.

## Key Concepts

- **`spark.conf.get()`** — reads pipeline configuration set in `pipelines.yml` `configuration:` section
- **Deploy-time vs runtime** — `${var.catalog}` (deploy) vs `spark.conf.get("parameter_lab.catalog")` (runtime); both resolve to the same value through different mechanisms
- **Python → SQL bridge** — creates a table (`pipeline_config_python`) that SQL can read to access runtime values
- **Default value handling** — `get_config()` helper that falls back to a default or raises if required
- **SDP expectations** — `@dlt.expect()` for data-level validation

## How It Works

### Reading Pipeline Configuration
```python
CATALOG = spark.conf.get("parameter_lab.catalog")  # returns "main"
SCHEMA = spark.conf.get("parameter_lab.schema")     # returns "parameter_lab_dev" (dev target)
```

These values come from the `configuration:` section of `pipelines.yml`, where DAB substituted `${var.catalog}` and `${var.schema}` at deploy time.

### The `get_config()` Helper
```python
def get_config(key, default=None):
    try:
        return spark.conf.get(key)
    except Exception:
        if default is not None:
            return default
        raise ValueError(f"Required parameter '{key}' is not set")
```

This demonstrates optional parameters (with defaults) and failure handling (raises when required params are missing).

### Python → SQL Bridge Table
```python
@dlt.table(name=f"{CATALOG}.{SCHEMA}.pipeline_config_python")
def pipeline_config_python():
    return spark.createDataFrame([
        ("catalog", CATALOG),
        ("schema", SCHEMA),
        ...
    ], schema="config_key STRING, config_value STRING")
```

This table is the bridge: Python writes spark.conf values into a Delta table, and SQL files (like `05_parameter_metadata.sql`) can read this table to access those values. This is the **only** supported mechanism for passing Python runtime values to SQL in SDP.
