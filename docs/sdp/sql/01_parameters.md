# How `01_parameters.sql` Works

## Purpose

Demonstrates deploy-time variable substitution in SQL and documents what SQL can and cannot access at runtime.

## Key Concepts

### Deploy-Time Substitution vs Runtime Configuration

| Mechanism | When Resolved | SQL Access | Python Access |
|---|---|---|---|
| `${var.catalog}` | Deploy time | Yes (baked into text) | N/A |
| `spark.conf.get("parameter_lab.catalog")` | Runtime | **No** | Yes |
| `${param}` (SQL widget) | Runtime | **Not in SDP** | N/A |

### What This File Creates

1. **`pipeline_config_sql`** — a materialized view storing all deploy-time values as key-value pairs. Python can read this to see what was baked in at deploy time.
2. **`customers_bronze`** — a streaming table with a dynamic name (`${var.catalog}.${var.schema}.customers${var.bronze_suffix}`).
3. A `SET VAR` statement demonstrating SQL-local variables (not shared with other files or Python).

### How the Config View Bridges SQL → Python

```sql
CREATE MATERIALIZED VIEW ${var.catalog}.${var.schema}.pipeline_config_sql AS
SELECT 'catalog' AS config_key, '${var.catalog}' AS config_value
UNION ALL ...
```

After deploy to dev, the SQL text becomes:
```sql
SELECT 'catalog' AS config_key, 'main' AS config_value
```

Python reads this table at runtime:
```python
spark.table("main.parameter_lab_dev.pipeline_config_sql")
```

### Why SQL Cannot Read spark.conf

SQL in SDP has no equivalent of `spark.conf.get()`. The `${parameter_lab.catalog}` syntax is NOT a Spark config reference — it would be interpreted as a SQL parameter marker, which SDP does not support. This is why the table-based bridge is the primary mechanism for passing configuration to SQL.
