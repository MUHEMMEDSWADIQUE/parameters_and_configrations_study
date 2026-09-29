# How `resources/pipelines.yml` Works

## Purpose

Defines the `parameter_lab_pipeline` SDP resource — a Lakeflow Declarative Pipeline mixing SQL and Python datasets.

## Key Concepts Demonstrated

- **Pipeline configuration** (`configuration:` section) — Spark conf keys set at runtime
- **Deploy-time substitution** — `${var.xxx}` in library paths and configuration values
- **Mixed SQL + Python libraries** — both file types loaded by the pipeline
- **Cluster configuration** — autoscale settings (overridden per target)
- **Development vs production mode** — `${var.environment == "dev"}` expression

## How the Configuration Section Works

The `configuration:` section sets Spark conf key-value pairs on every cluster in the pipeline:

```yaml
configuration:
  parameter_lab.catalog: ${var.catalog}
  parameter_lab.schema: ${var.schema}
  parameter_lab.environment: ${var.environment}
```

At deploy time, `${var.catalog}` is replaced with the literal value (e.g. `main`). At runtime, Python accesses these via:
```python
spark.conf.get("parameter_lab.catalog")  # returns "main"
```

SQL files in the pipeline **cannot** read these spark.conf values — this is a documented limitation. SQL files rely on deploy-time `${var.xxx}` substitution instead.

## Library Loading Order

The pipeline loads files in this order (by file name within each directory):

1. SQL files: `01_parameters.sql` → `02_dynamic_source.sql` → `03_dynamic_target.sql` → `04_parameter_expressions.sql` → `05_parameter_metadata.sql`
2. Python files: `01_parameters.py` → `02_dynamic_source.py` → `03_dynamic_target.py` → `04_parameter_validation.py` → `05_metadata_driven.py`

## Target-Specific Behavior

| Setting | dev | test | prod |
|---|---|---|---|
| schema | parameter_lab_dev | parameter_lab_test | parameter_lab_prod |
| processing_mode | incremental | incremental | full |
| max_instances | 2 | 3 | 5 |
| development mode | true | false | false |

The `development: ${var.environment == "dev"}` expression evaluates to `true` for dev and `false` for test/prod.
