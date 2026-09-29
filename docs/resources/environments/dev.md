# How `resources/environments/dev.yml` Works

## Purpose

Defines the `dev` target — overrides bundle-level variables for the development environment.

## Key Concepts Demonstrated

- **Target-specific variable overrides** — higher priority than bundle defaults
- **Parameter precedence** — bundle default < target override < CLI flag
- **Target-specific workspace paths** — dev gets its own root_path subdirectory
- **Target-specific cluster sizing** — smaller autoscale for dev

## How It Works

This file is included via `include: - resources/environments/*.yml` in `databricks.yml`. It defines:

```yaml
targets:
  dev:
    variables:
      schema: parameter_lab_dev      # overrides "parameter_lab"
      processing_mode: incremental
      retention_days: "7"
```

When you run `databricks bundle deploy -t dev`, every `${var.schema}` reference across the entire bundle resolves to `parameter_lab_dev` instead of the bundle default `parameter_lab`.

## What Changes in Dev

| Variable | Bundle Default | Dev Override |
|---|---|---|
| schema | parameter_lab | parameter_lab_dev |
| environment | dev | dev |
| processing_mode | incremental | incremental |
| max_files_per_trigger | 100 | 50 |
| retention_days | *(none)* | 7 |

## Same Code, Different Behavior

The SQL file `03_dynamic_target.sql` contains:
```sql
CREATE MATERIALIZED VIEW ${var.catalog}.${var.schema}.customers_silver AS ...
```

- Deploy to **dev**: creates `main.parameter_lab_dev.customers_silver`
- Deploy to **prod**: creates `main.parameter_lab_prod.customers_silver`

The **same SQL text** produces different tables — this is the power of deploy-time substitution with target overrides.
