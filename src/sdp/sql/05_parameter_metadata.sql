-- =============================================================================
-- 05_parameter_metadata.sql — Metadata-driven SQL (SDP)
-- =============================================================================
-- LEARNING OBJECTIVES:
--   1. Read a metadata configuration table from SQL
--   2. Use metadata to drive SQL transformations
--   3. Demonstrate the Python -> SQL parameter flow via tables
--
-- CONCEPT — Python -> SQL parameter passing:
--   SQL cannot read Python variables or spark.conf values directly.
--   The supported mechanism is:
--     1. Python writes configuration into a Delta table/view
--     2. SQL reads that table/view
--   This is NOT the same as passing a parameter — it is sharing data
--   through a table. But it achieves the same goal: Python-generated
--   values become available to SQL.
--
-- In this file, we read from:
--   ${var.catalog}.${var.schema}.parameter_config  (created by Python)
--   ${var.catalog}.${var.schema}.pipeline_config_sql (created in 01_parameters.sql)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Read the metadata-driven configuration table.
-- This table is created by src/sdp/python/05_metadata_driven.py.
-- SQL reads it to discover which domains to process.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.active_domains AS
SELECT
  domain,
  source_system,
  source_table,
  target_table,
  load_type,
  watermark_column,
  primary_key,
  processing_sequence,
  parameters  -- JSON column
FROM ${var.catalog}.${var.schema}.parameter_config
WHERE active_flag = true
ORDER BY processing_sequence;

-- -----------------------------------------------------------------------------
-- Use the JSON parameters column in SQL.
-- Databricks SQL supports JSON extraction with variant type.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.domain_parameters AS
SELECT
  domain,
  source_table,
  target_table,
  -- Extract JSON fields using from_json / try_variant functions
  parameters :partition_column        AS partition_column,
  parameters :max_files                AS max_files,
  parameters :quality_check            AS quality_check,
  -- Use variant_extract for nested access
  CAST(parameters :max_files AS INT)   AS max_files_int,
  CAST(parameters :quality_check AS BOOLEAN) AS quality_check_bool
FROM ${var.catalog}.${var.schema}.active_domains;

-- -----------------------------------------------------------------------------
-- Combine deploy-time values with runtime metadata.
-- This view shows BOTH the deploy-time config (from pipeline_config_sql)
-- and the runtime metadata (from active_domains) side by side.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.full_config_snapshot AS
SELECT
  -- Deploy-time values (from 01_parameters.sql)
  (SELECT config_value FROM ${var.catalog}.${var.schema}.pipeline_config_sql
   WHERE config_key = 'catalog')         AS deploy_catalog,
  (SELECT config_value FROM ${var.catalog}.${var.schema}.pipeline_config_sql
   WHERE config_key = 'schema')          AS deploy_schema,
  (SELECT config_value FROM ${var.catalog}.${var.schema}.pipeline_config_sql
   WHERE config_key = 'environment')    AS deploy_environment,
  (SELECT config_value FROM ${var.catalog}.${var.schema}.pipeline_config_sql
   WHERE config_key = 'processing_mode') AS deploy_processing_mode,
  -- Runtime metadata
  COUNT(d.domain)                        AS active_domain_count,
  COLLECT_LIST(d.domain)                 AS active_domains,
  COLLECT_LIST(d.target_table)          AS target_tables
FROM ${var.catalog}.${var.schema}.active_domains d;

-- -----------------------------------------------------------------------------
-- KEY TAKEAWAY:
--   Deploy-time substitution (${var.xxx}) -> baked into SQL text at deploy
--   Runtime config (spark.conf) -> accessible in Python, NOT in SQL
--   Runtime metadata (Delta tables) -> accessible in BOTH Python and SQL
--   Task values -> accessible across workflow tasks, NOT in SDP
--
-- The TABLE is the universal bridge between Python and SQL in SDP.
-- =============================================================================
