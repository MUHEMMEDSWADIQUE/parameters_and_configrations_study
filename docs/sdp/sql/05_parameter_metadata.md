# How `05_parameter_metadata.sql` Works

## Purpose

Demonstrates the Python → SQL parameter bridge: reading a metadata table created by Python, parsing JSON columns in SQL, and combining runtime metadata with deploy-time values.

## Key Concepts

- **Python → SQL bridge** — Python creates a table, SQL reads it (the ONLY supported mechanism)
- **JSON column access in SQL** — `parameters :partition_column` (variant type)
- **Combining deploy-time and runtime data** — subqueries from both `pipeline_config_sql` (deploy-time) and `active_domains` (runtime metadata)
- **Why a SQL query result is NOT a Python variable** — documented explicitly

## How It Works

### Reading the Metadata Table (created by Python)
```sql
CREATE MATERIALIZED VIEW ${var.catalog}.${var.schema}.active_domains AS
SELECT domain, source_table, target_table, load_type, parameters
FROM ${var.catalog}.${var.schema}.parameter_config
WHERE active_flag = true
```

The `parameter_config` table is created by `src/sdp/python/05_metadata_driven.py`. SQL cannot create it — it depends on Python running first.

### Parsing JSON in SQL
```sql
SELECT
  parameters :partition_column        AS partition_column,
  CAST(parameters :max_files AS INT)   AS max_files_int,
  CAST(parameters :quality_check AS BOOLEAN) AS quality_check_bool
FROM active_domains
```

The `:` operator accesses JSON fields using Databricks variant type. This is the SQL equivalent of Python's `json.loads()`.  

### Combining Deploy-Time and Runtime Values
```sql
CREATE MATERIALIZED VIEW full_config_snapshot AS
SELECT
  (SELECT config_value FROM pipeline_config_sql WHERE config_key = 'catalog') AS deploy_catalog,
  COUNT(d.domain) AS active_domain_count
FROM active_domains d
```

This view shows BOTH deploy-time values (from the SQL-created `pipeline_config_sql`) and runtime metadata (from the Python-created `active_domains`) in a single result.

## Key Takeaway

The **table** is the universal bridge between Python and SQL in SDP:
- Python writes config/data → table → SQL reads it
- SQL writes deploy-time values → table → Python reads it
- Neither can directly access the other's variables or configuration
