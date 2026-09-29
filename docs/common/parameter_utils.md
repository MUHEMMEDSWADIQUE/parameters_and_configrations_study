# How `parameter_utils.py` Works

## Purpose

Provides utility functions for parameter manipulation across the project — parsing, type conversion, safe retrieval, name construction, and config merging.

## Key Concepts Demonstrated

- **Safe parameter retrieval** — `safe_get_config()`, `safe_get_widget()`, `safe_get_task_value()` with defaults
- **JSON parameter parsing** — `parse_json_parameter()` for widget strings
- **Type conversion** — `convert_param_type()` (string to bool/int/float)
- **Dynamic name construction** — `build_fqtn()`, `build_layered_name()`
- **Config merging with precedence** — `merge_configs()` (later overrides earlier)
- **JSON field extraction** — `extract_json_field()` with dot-notation path

## Functions

### Safe Retrieval (prevents exceptions)
```python
# Spark config
catalog = safe_get_config(spark, "parameter_lab.catalog", default="main")

# Widget
schema = safe_get_widget(dbutils, "schema", default="parameter_lab")

# Task value
domain = safe_get_task_value(dbutils, "task_a_producer", "domain", default=None)
```

All three functions return the default if the source is unavailable, rather than raising an exception. This demonstrates **optional parameters** with graceful fallback.

### JSON Parsing
```python
craw_string = dbutils.widgets.get("json_config")
config = parse_json_parameter(raw_string)  # returns dict
```

### Type Conversion
```python
enable_qc = convert_param_type("true", bool)    # True
max_files = convert_param_type("100", int)       # 100
```

### Name Construction
```python
fqtn = build_fqtn("main", "parameter_lab_dev", "customers_bronze")
# "main.parameter_lab_dev.customers_bronze"

layered = build_layered_name("customers", "_silver")
# "customers_silver"
```

### Config Merging (demonstrates precedence)
```python
merged = merge_configs(
    {"schema": "default"},    # lowest priority
    {"schema": "override"},   # higher priority
)
# {"schema": "override"}
```

This simulates how DAB merges bundle defaults with target overrides — later dictionaries win.
