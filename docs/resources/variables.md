# How `resources/variables.yml` Works

## Purpose

Defines additional bundle variables beyond those in `databricks.yml`. Included via `include: - resources/*.yml`.

## Key Concepts Demonstrated

- **JSON-typed variables** — passed as multi-line string, parsed at runtime
- **Numeric variables** — string in DAB, converted at runtime
- **Boolean-like variables** — string ("true"/"false"), parsed in Python
- **Variables with no default** — must be provided at deploy time or in target overrides
- **Variables referencing other variables** — `${var.catalog}` inside a variable default
- **Suffix variables** — for layered table naming (bronze/silver/gold)

## Variables Defined

| Variable | Default | Notes |
|---|---|---|
| `json_config` | JSON string | Multi-line YAML block scalar; parsed via `json.loads()` in Python |
| `max_files_per_trigger` | "100" | String; converted to int in Python |
| `enable_quality_checks` | "true" | String; compared as `.lower() == "true"` in Python |
| `retention_days` | *(none)* | No default — must be provided via target override or CLI |
| `full_source_path` | `/Volumes/${var.catalog}/...` | References another variable in its default |
| `bronze_suffix` | `_bronze` | Used for dynamic table names |
| `silver_suffix` | `_silver` | Used for dynamic table names |
| `gold_suffix` | `_gold` | Used for dynamic table names |

## How the JSON Variable Works

The `json_config` variable is defined as a multi-line YAML block scalar:
```yaml
json_config:
  default: |
    {
      "source": "azure_sql",
      "schema": "retail",
      "table": "customer"
    }
```

At deploy time, DAB substitutes this as a string into the job's `base_parameters`. At runtime, Python parses it:
```python
craw_string = dbutils.widgets.get("json_config")
config = json.loads(raw_string)  # now a dict
```

## How the No-Default Variable Works

`retention_days` has no default. If you deploy without a target override or CLI flag:
```bash
databricks bundle deploy -t dev  # FAILS: retention_days not set
databricks bundle deploy -t dev --var="retention_days=7"  # OK
```
Each target file (`dev.yml`, `test.yml`, `prod.yml`) provides a value, so normal deploys work.
