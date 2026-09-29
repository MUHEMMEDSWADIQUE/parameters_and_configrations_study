# How `02_dynamic_source.sql` Works

## Purpose

Demonstrates dynamic source configuration in SQL using deploy-time substitution.

## Key Concepts

- **Dynamic source paths** — `${var.source_path}` becomes a literal path at deploy time
- **Dynamic catalog/schema in source table references** — `${var.catalog}.${var.schema}.source_orders`
- **Auto Loader with dynamic path** — `cloud_files('${var.source_path}/customers', 'json', ...)`
- **Source summary view** — captures deploy-time values for inspection

## How It Works

### Auto Loader Source
```sql
CREATE STREAMING TABLE ${var.catalog}.${var.schema}.raw_customers${var.bronze_suffix} AS
SELECT * FROM cloud_files('${var.source_path}/customers', 'json', ...)
```
After deploy to dev, `${var.source_path}` becomes `/Volumes/main/parameter_lab/raw`, so the SQL reads from that literal path.

### Dynamic Source Table Reference
```sql
CREATE STREAMING TABLE ${var.catalog}.${var.schema}.raw_orders${var.bronze_suffix} AS
SELECT * FROM STREAM(${var.catalog}.${var.schema}.source_orders)
```
The source table name changes per target — dev reads from `main.parameter_lab_dev.source_orders`, prod reads from `main.parameter_lab_prod.source_orders`.

### Comparison with Python

The Python equivalent (`02_dynamic_source.py`) reads the same values at runtime via `spark.conf.get()`. The key difference: Python can construct paths dynamically at runtime (e.g., adding date partitions), while SQL can only use deploy-time-substituted values.
