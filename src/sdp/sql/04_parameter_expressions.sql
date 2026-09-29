-- =============================================================================
-- 04_parameter_expressions.sql — Advanced parameter expressions (SDP)
-- =============================================================================
-- LEARNING OBJECTIVES:
--   1. Combine multiple ${var.xxx} in a single expression
--   2. Use ${bundle.target} and ${bundle.name} for dynamic naming
--   3. Conditional logic based on deploy-time values
--   4. Build fully qualified names from components
--
-- CONCEPT:
--   DAB supports expressions inside ${...}. You can use:
--     ${var.xxx}                    — a bundle variable
--     ${bundle.target}             — current target name
--     ${bundle.name}                — bundle name
--     ${workspace.current_user.name} — current user
--     ${var.environment == 'dev'}  — boolean expression
--   These are ALL resolved at deploy time, not runtime.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Build a fully qualified name from multiple variables.
-- After deployment, this becomes a literal string like:
--   'main.parameter_lab_dev'
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.parameter_expressions_demo AS
SELECT
  -- Concatenating deploy-time values
  CONCAT('${var.catalog}', '.', '${var.schema}') AS fully_qualified_schema,
  CONCAT('${var.catalog}', '.', '${var.schema}', '.customers', '${var.bronze_suffix}') AS bronze_fqtn,
  CONCAT('${var.catalog}', '.', '${var.schema}', '.customers', '${var.silver_suffix}') AS silver_fqtn,
  CONCAT('${var.catalog}', '.', '${var.schema}', '.customers', '${var.gold_suffix}') AS gold_fqtn,
  -- Environment and target info
  '${var.environment}'   AS env_name,
  '${bundle.target}'     AS target_name,
  '${bundle.name}'       AS bundle_name,
  -- Processing mode affects behavior
  CASE '${var.processing_mode}'
    WHEN 'full'       THEN 'Full reload — all data reprocessed'
    WHEN 'incremental' THEN 'Incremental — only new data processed'
    ELSE 'Unknown mode'
  END AS processing_description,
  -- Retention days
  CAST('${var.retention_days}' AS INT) AS retention_days_value;

-- -----------------------------------------------------------------------------
-- Demonstrate that the same SQL produces different DDL per target.
-- When deployed to `dev`:
--   CREATE VIEW main.parameter_lab_dev.parameter_expressions_demo ...
-- When deployed to `prod`:
--   CREATE VIEW main.parameter_lab_prod.parameter_expressions_demo ...
--
-- The Python files in this pipeline can read the pipeline_config_sql view
-- (created in 01_parameters.sql) to see these same values at runtime.
-- This is how SQL "passes" its deploy-time values to Python at runtime:
--   SQL writes deploy-time values into a table/view -> Python reads that table.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- CONDITIONAL DDL: different columns based on target.
-- ${var.enable_quality_checks} is 'true' in all targets, but this shows
-- the pattern. Note: SQL does not have a preprocessor #if — we use CASE
-- in the data instead and rely on DAB for structural differences.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.quality_dashboard AS
SELECT
  '${var.environment}' AS environment,
  '${var.processing_mode}' AS mode,
  -- Quality check flag from deploy-time variable
  CASE WHEN '${var.enable_quality_checks}' = 'true'
       THEN 'Quality checks ENABLED'
       ELSE 'Quality checks DISABLED'
  END AS quality_status,
  CAST('${var.max_files_per_trigger}' AS INT) AS max_files,
  CAST('${var.retention_days}' AS INT) AS retention_days;
