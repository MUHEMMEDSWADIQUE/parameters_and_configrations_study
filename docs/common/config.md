# How `config.py` Works

## Purpose

Provides the `PipelineConfig` dataclass — a unified configuration object that reads from multiple sources (spark.conf, widgets, environment variables) with clear parameter precedence.

## Key Concepts Demonstrated

- **Multiple configuration sources** — spark.conf, dbutils.widgets, environment variables
- **Parameter precedence** — spark.conf > widget > env var > default
- **Typed configuration** — dataclass with typed fields (str, int, bool)
- **Factory methods** — `from_spark_conf()`, `from_widgets()`, `from_env()`
- **Dynamic table name builders** — `bronze_table()`, `silver_table()`, `gold_table()`, `fully_qualified()`

## How It Works

### Building Config from Spark Conf (SDP pipelines)
```python
from parameter_lab_common.config import PipelineConfig
config = PipelineConfig.from_spark_conf(spark)
# config.catalog = "main"
# config.schema = "parameter_lab_dev"
# config.processing_mode = "incremental"
```

Reads from `spark.conf.get("parameter_lab.catalog")` etc., falling back to defaults.

### Building Config from Widgets (Workflow tasks)
```python
config = PipelineConfig.from_widgets(dbutils)
# config.catalog = dbutils.widgets.get("catalog")
```

### Building Config from Environment (local testing)
```python
config = PipelineConfig.from_env()
# config.catalog = os.environ.get("PARAMETER_LAB_CATALOG", "main")
```

### Dynamic Table Name Construction
```python
config = PipelineConfig(catalog="main", schema="parameter_lab_dev")
config.bronze_table("customers")  # "main.parameter_lab_dev.customers_bronze"
config.silver_table("customers")  # "main.parameter_lab_dev.customers_silver"
config.gold_table("customers")    # "main.parameter_lab_dev.customers_gold"
config.fully_qualified("my_table") # "main.parameter_lab_dev.my_table"
```

### Why This Is Useful

Instead of repeating `f"{catalog}.{schema}.{table}{suffix}"` across all files, you create a `PipelineConfig` once and use its methods. The config object encapsulates all the parameter logic in one place, making it easy to maintain and test.

### Packaging

This module is packaged as a wheel via `setup.py` and installed on pipeline/job clusters. Imported as:
```python
from parameter_lab_common.config import PipelineConfig
```
