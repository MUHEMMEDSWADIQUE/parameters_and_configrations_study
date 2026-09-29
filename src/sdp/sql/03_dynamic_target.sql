-- =============================================================================
-- 03_dynamic_target.sql — Dynamic target configuration (SDP)
-- =============================================================================
-- LEARNING OBJECTIVES:
--   1. Dynamic target table names using ${var.xxx} substitution
--   2. Dynamic schema and catalog names in CREATE statements
--   3. How the same SQL creates different tables per target
--   4. Using suffix variables for layered naming (bronze/silver/gold)
--
-- CONCEPT:
--   ${var.bronze_suffix} -> '_bronze'
--   ${var.silver_suffix} -> '_silver'
--   ${var.gold_suffix}   -> '_gold'
--   Combined with ${var.catalog}.${var.schema}, each target gets fully
--   isolated table sets with consistent naming.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver layer: cleaned and deduplicated data.
-- Table name: main.parameter_lab_dev.customers_silver (dev)
--             main.parameter_lab_prod.customers_silver (prod)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.customers${var.silver_suffix}
COMMENT 'Silver layer: cleaned customers'
AS
SELECT
  customer_id,
  customer_name,
  customer_email,
  country,
  CAST(created_date AS TIMESTAMP) AS created_date,
  '${var.environment}' AS environment_tag,
  '${bundle.target}'   AS target_tag
FROM ${var.catalog}.${var.schema}.customers${var.bronze_suffix}
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Gold layer: aggregated business metrics.
-- This table name uses TWO variable substitutions for the suffix.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.customers${var.gold_suffix}
COMMENT 'Gold layer: customer metrics'
AS
SELECT
  country,
  COUNT(*)                          AS customer_count,
  COUNT(DISTINCT customer_email)   AS unique_emails,
  '${var.catalog}'                 AS source_catalog,
  '${var.schema}'                  AS source_schema
FROM ${var.catalog}.${var.schema}.customers${var.silver_suffix}
GROUP BY country, '${var.catalog}', '${var.schema}';

-- -----------------------------------------------------------------------------
-- Dynamic target: environment-specific retention.
-- ${var.retention_days} is substituted at deploy time.
--   dev:  7 days
--   test: 30 days
--   prod: 365 days
-- -----------------------------------------------------------------------------
-- Note: ALTER TABLE with dynamic retention would be:
-- ALTER TABLE ${var.catalog}.${var.schema}.customers${var.gold_suffix}
-- SET TBLPROPERTIES (delta.logRetentionDuration = 'interval ${var.retention_days} days');
-- This is shown as a comment because SDP does not support ALTER TABLE
-- directly in the pipeline definition. Apply it post-deployment or via
-- a Python notebook task.
