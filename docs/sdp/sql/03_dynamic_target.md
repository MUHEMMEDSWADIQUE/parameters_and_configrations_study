# How `03_dynamic_target.sql` Works

## Purpose

Demonstrates dynamic target table names using suffix variables for the bronze/silver/gold layering pattern.

## Key Concepts

- **Layered naming convention** — `${base_name}${var.bronze_suffix}` → `customers_bronze`
- **Dynamic table names** — same SQL creates different tables per target
- **Environment tagging in data** — `${var.environment}` and `${bundle.target}` become literal values in the data rows
- **Dynamic retention** — `${var.retention_days}` substituted at deploy time (shown as a comment because SDP doesn't support ALTER TABLE directly)

## Tables Created

| Table | Suffix Variable | Dev Name | Prod Name |
|---|---|---|---|
| customers_silver | `${var.silver_suffix}` | main.parameter_lab_dev.customers_silver | main.parameter_lab_prod.customers_silver |
| customers_gold | `${var.gold_suffix}` | main.parameter_lab_dev.customers_gold | main.parameter_lab_prod.customers_gold |

## How the Silver Layer Works

```sql
CREATE MATERIALIZED VIEW ${var.catalog}.${var.schema}.customers${var.silver_suffix} AS
SELECT
  customer_id, customer_name, ...,
  '${var.environment}' AS environment_tag,
  '${bundle.target}'   AS target_tag
FROM ${var.catalog}.${var.schema}.customers${var.bronze_suffix}
```

The `environment_tag` column is a literal string baked in at deploy time. In dev it's `'dev'`, in prod it's `'prod'`.

## How the Gold Layer Works

The gold layer aggregates the silver layer and includes catalog/schema as data columns:
```sql
SELECT country, COUNT(*) AS customer_count, ...
FROM ${var.catalog}.${var.schema}.customers${var.silver_suffix}
GROUP BY country, '${var.catalog}', '${var.schema}'
```

This demonstrates that deploy-time values can appear both in DDL (table names) and in DML (data columns).
