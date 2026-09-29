# How `resources/environments/prod.yml` Works

## Purpose

Defines the `prod` target — production environment with full reload mode, higher resources, and long retention.

## Key Concepts Demonstrated

- **Different processing mode** — `full` instead of `incremental` (same code, different runtime behavior)
- **Higher resource allocation** — min 2, max 5 instances
- **Long retention** — 365 days
- **How runtime config adapts** — `spark.conf.get("parameter_lab.processing_mode")` returns `full` in prod

## What Changes in Prod

| Variable | Bundle Default | Prod Override |
|---|---|---|
| schema | parameter_lab | parameter_lab_prod |
| environment | dev | prod |
| processing_mode | incremental | **full** |
| max_files_per_trigger | 100 | 500 |
| retention_days | *(none)* | 365 |

## How Processing Mode Affects Runtime

The Python SDP file `02_dynamic_source.py` reads `processing_mode` at runtime:
```python
PROCESSING_MODE = spark.conf.get("parameter_lab.processing_mode")
```

In prod, this returns `"full"`, which triggers batch loading:
```python
if PROCESSING_MODE == "full":
    return spark.read.table(source_table)      # batch
else:
    return spark.readStream.table(source_table)  # streaming
```

**No code change needed** — the same pipeline file behaves differently in prod vs dev because the configuration value is different.
