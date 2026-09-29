# How `logging.py` Works

## Purpose

Provides `ParameterLabLogger` — a structured logger that tags every log message with parameter context (environment, catalog, schema, task name), demonstrating that logging itself can be parameterized.

## Key Concepts Demonstrated

- **Parameterized logging** — log output adapts based on runtime environment
- **Context tags** — `[dev|main.parameter_lab_dev|pipeline]` prefix on every message
- **Multiple factory methods** — `from_spark_conf()` (SDP), `from_widgets()` (workflow)
- **Config dump** — `config_dump()` method to log all configuration values

## How It Works

### Creating a Logger from Spark Conf (SDP)
```python
from parameter_lab_common.logging import ParameterLabLogger
logger = ParameterLabLogger.from_spark_conf(spark, task="pipeline")
logger.info("Starting pipeline")
# Output: [dev|main.parameter_lab_dev|pipeline] Starting pipeline
```

### Creating a Logger from Widgets (Workflow)
```python
logger = ParameterLabLogger.from_widgets(dbutils, task="task_a_producer")
logger.info("Publishing task values")
# Output: [dev|main.parameter_lab_dev|task_a_producer] Publishing task values
```

### Configuration Dump
```python
logger.config_dump({"catalog": "main", "schema": "parameter_lab_dev", ...})
# Output:
# [dev|main.parameter_lab_dev|pipeline] ============ CONFIGURATION DUMP ============
# [dev|main.parameter_lab_dev|pipeline]   catalog = main
# [dev|main.parameter_lab_dev|pipeline]   schema = parameter_lab_dev
# ...
```

### Why Context Tags Matter

When debugging a multi-environment pipeline, the context tag immediately tells you:
- **Which environment** the log came from (dev/test/prod)
- **Which catalog.schema** was active
- **Which task** produced the log

This makes it trivial to grep logs by environment or task when troubleshooting.

### How the Logger Gets Its Context

The logger reads the same parameter sources as the rest of the pipeline:
- `from_spark_conf()` calls `spark.conf.get("parameter_lab.environment")` etc.
- `from_widgets()` calls `dbutils.widgets.get("environment")` etc.

This demonstrates that **logging configuration is also parameterized** — the log format adapts to the runtime environment without code changes.
