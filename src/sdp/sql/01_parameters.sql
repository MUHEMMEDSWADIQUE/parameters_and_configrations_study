-- =============================================================================
-- 01_parameters.sql — Parameter access in SQL (SDP)
-- =============================================================================
-- LEARNING OBJECTIVES:
--   1. Understand DEPLOY-TIME variable substitution: ${var.xxx}
--   2. Understand what SQL CAN and CANNOT access at runtime
--   3. Create a config view so Python can read deploy-time values
--
-- CRITICAL CONCEPT — Two kinds of "parameters" in SQL:
--
--   A) DEPLOY-TIME SUBSTITUTION (${var.xxx}):
--      DAB replaces ${var.catalog} with the literal value (e.g. 'main')
--      BEFORE the file is deployed. The deployed SQL file contains
--      the literal string — it is NOT a runtime parameter.
--      This works for ANY file in the bundle (SQL, Python, YAML).
--
--   B) RUNTIME SPARK CONFIGURATION:
--      Pipeline `configuration:` sets Spark conf keys like
--      `parameter_lab.catalog`. In Python you access these via
--      spark.conf.get("parameter_lab.catalog").
--      In SQL, these are NOT directly accessible — there is no
--      spark.conf.get() equivalent in SQL.
--      WORKAROUND: create a view/table in Python that exposes these
--      values, then read that view from SQL (see 05_parameter_metadata.sql).
--
--   C) SQL ${param} MARKERS (Databricks SQL widgets):
--      In Databricks SQL notebooks, ${param} creates a widget.
--      In SDP SQL files, this syntax is NOT supported — SDP files are
--      not notebooks and do not have widget support.
--
-- WHAT THIS FILE DEMONSTRATES:
--   - ${var.catalog}  ->  substituted at deploy time to 'main'
--   - ${var.schema}   ->  substituted at deploy time to 'parameter_lab_dev' (or _test/_prod)
--   - ${bundle.target} ->  substituted at deploy time to 'dev'/'test'/'prod'
--   - A config view that Python can read to discover deploy-time values
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create a config view that captures deploy-time values.
-- Python can query this view to see what values were baked in at deploy time.
-- This bridges the gap between deploy-time substitution and runtime access.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ${var.catalog}.${var.schema}.pipeline_config_sql (
  config_key   STRING,
  config_value STRING
);

-- Populate with deploy-time-substituted values.
-- After deployment, the literal values appear here (not ${var.xxx}).
APPLY CHANGES INTO LIVE TABLE ${var.catalog}.${var.schema}.pipeline_config_sql;

-- Since we cannot use APPLY CHANGES without a source, use a simpler approach:
-- Create a materialized view instead.
CREATE OR REFRESH MATERIALIZED VIEW ${var.catalog}.${var.schema}.pipeline_config_sql AS
SELECT 'catalog'          AS config_key, '${var.catalog}'    AS config_value
UNION ALL
SELECT 'schema',           '${var.schema}'
UNION ALL
SELECT 'environment',      '${var.environment}'
UNION ALL
SELECT 'source_path',      '${var.source_path}'
UNION ALL
SELECT 'processing_mode',  '${var.processing_mode}'
UNION ALL
SELECT 'target',           '${bundle.target}'
UNION ALL
SELECT 'bronze_suffix',    '${var.bronze_suffix}'
UNION ALL
SELECT 'silver_suffix',   '${var.silver_suffix}'
UNION ALL
SELECT 'gold_suffix',      '${var.gold_suffix}';

-- -----------------------------------------------------------------------------
-- Create a bronze landing table using deploy-time substitution.
-- The table name is DYNAMIC: it changes per target.
--   dev:  main.parameter_lab_dev.customers_bronze
--   test: main.parameter_lab_test.customers_bronze
--   prod: main.parameter_lab_prod.customers_bronze
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ${var.catalog}.${var.schema}.customers${var.bronze_suffix};

-- -----------------------------------------------------------------------------
-- IMPORTANT: The following SQL does NOT work in SDP:
--
--   SELECT ${parameter_lab.catalog}           -- NOT a Spark config reference
--   SELECT spark.conf.get('parameter_lab.catalog')  -- NOT valid SQL syntax
--   SET parameter_lab.catalog = 'main'       -- This sets a Spark conf but
--                                              SQL cannot read it back
--
-- This is the key limitation: SQL in SDP cannot read runtime Spark
-- configuration values. Deploy-time ${var.xxx} substitution is the primary
-- mechanism for passing parameters into SQL files.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- SQL variables (SET statements) work within a single SQL file but are
-- NOT shared across files or with Python.
-- -----------------------------------------------------------------------------
SET VAR my_local_var = 'hello_from_sql';

-- This creates a SQL script variable — usable only in THIS file.
SELECT '${var.catalog}.${var.schema}' AS full_qualified_name,
       '${bundle.target}'            AS current_target;
