# How `04_parameter_expressions.sql` Works

## Purpose

Demonstrates advanced DAB expression usage inside SQL files — combining multiple `${var.xxx}` references, using `${bundle.target}` and `${bundle.name}`, and building conditional logic from deploy-time values.

## Key Concepts

- **Concatenating deploy-time values** — `CONCAT('${var.catalog}', '.', '${var.schema}')`
- **Built-in bundle expressions** — `${bundle.target}`, `${bundle.name}`
- **Conditional logic from deploy-time values** — `CASE '${var.processing_mode}' WHEN 'full' THEN ...`
- **Type casting of deploy-time strings** — `CAST('${var.retention_days}' AS INT)`

## How It Works

### Building Fully Qualified Names
```sql
SELECT
  CONCAT('${var.catalog}', '.', '${var.schema}') AS fully_qualified_schema,
  CONCAT('${var.catalog}', '.', '${var.schema}', '.customers', '${var.bronze_suffix}') AS bronze_fqtn
```
After deploy to dev, this becomes literal SQL:
```sql
SELECT
  CONCAT('main', '.', 'parameter_lab_dev') AS fully_qualified_schema,
  CONCAT('main', '.', 'parameter_lab_dev', '.customers', '_bronze') AS bronze_fqtn
```

### Conditional Logic Based on Processing Mode
```sql
CASE '${var.processing_mode}'
  WHEN 'full'       THEN 'Full reload — all data reprocessed'
  WHEN 'incremental' THEN 'Incremental — only new data processed'
END AS processing_description
```
This is a deploy-time decision — the CASE expression is baked in with the literal value. It's NOT a runtime check.

### Views Created

1. **`parameter_expressions_demo`** — shows all deploy-time values side by side
2. **`quality_dashboard`** — demonstrates conditional quality check status from deploy-time variable

## Key Insight

All values in this file are resolved at deploy time. If you change the processing mode, you must redeploy the bundle — the SQL cannot adapt at runtime. For runtime adaptability, use Python (`spark.conf.get()`).
