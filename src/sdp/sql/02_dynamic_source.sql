-- =============================================================================
-- 02_dynamic_source.sql — Dynamic source configuration (SDP)
-- =============================================================================
-- LEARNING OBJECTIVES:
--   1. Dynamic source paths using deploy-time substitution
--   2. Dynamic catalog/schema/table names in source definitions
--   3. How the same SQL file reads from different locations per target
--
-- CONCEPT:
--   ${var.source_path} is replaced at deploy time with the literal path.
--   ${var.catalog}.${var.schema} becomes the fully qualified namespace.
--   This means the SAME SQL file reads from different schemas in dev vs prod.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Dynamic source: Auto Loader from a volume path.
-- ${var.source_path} is substituted at deploy time.
--   dev:  /Volumes/main/parameter_lab/raw
--   prod: /Volumes/main/parameter_lab/raw (same path, different schema for output)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ${var.catalog}.${var.schema}.raw_customers${var.bronze_suffix}
COMMENT 'Bronze layer: raw customers from Auto Loader'
AS
SELECT *
FROM cloud_files(
  '${var.source_path}/customers',
  'json',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.useNotifications', 'false'
  )
);

-- -----------------------------------------------------------------------------
-- Dynamic source: read from an existing Delta table with dynamic name.
-- The source table name changes per target because ${var.schema} changes.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ${var.catalog}.${var.schema}.raw_orders${var.bronze_suffix}
COMMENT 'Bronze layer: raw orders from Delta source'
AS
SELECT *
FROM STREAM(${var.catalog}.${var.schema}.source_orders);

-- -----------------------------------------------------------------------------
-- Dynamic source: combining multiple dynamic references.
-- The full table name is assembled from multiple ${var.xxx} substitutions.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.source_summary AS
SELECT
  '${var.catalog}'                     AS source_catalog,
  '${var.schema}'                      AS source_schema,
  '${var.source_path}'                 AS source_volume_path,
  '${var.processing_mode}'            AS processing_mode,
  '${bundle.target}'                   AS deploy_target,
  current_timestamp()                  AS captured_at;
