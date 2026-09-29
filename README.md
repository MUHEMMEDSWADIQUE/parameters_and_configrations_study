# Parameter Lab — Databricks Asset Bundle Project

A production-style **learning laboratory** for advanced parameter passing, configuration management, dynamic configuration, and parameter propagation across **SQL**, **Python**, **Lakeflow Declarative Pipelines (SDP)**, and **Databricks Workflows**.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Parameter Passing Mechanisms](#parameter-passing-mechanisms)
6. [DAB Bundle Configuration](#dab-bundle-configuration)
7. [SDP Pipeline](#sdp-pipeline)
8. [Workflow Parameter Passing](#workflow-parameter-passing)
9. [Metadata-Driven Configuration](#metadata-driven-configuration)
10. [Dynamic JSON Parameters](#dynamic-json-parameters)
11. [Testing](#testing)
12. [Concept Reference](#concept-reference)

---

## Overview

This project demonstrates **42 parameter-related concepts** across the Databricks platform. Each concept can be executed and observed independently through the corresponding files.

### Core Learning Objectives

| # | Concept | Demonstrated In |
|---|--------|-----------------|
| 1 | DAB bundle variables | `databricks.yml`, `resources/variables.yml` |
| 2 | Target-specific variables | `resources/environments/dev.yml`, `test.yml`, `prod.yml` |
| 3 | Variable substitution (`${var.xxx}`) | All YAML, SQL, and Python files |
| 4 | Environment-specific configuration | `resources/environments/*.yml` |
| 5 | Pipeline configuration | `resources/pipelines.yml` |
| 6 | Pipeline parameters (spark.conf) | `src/sdp/python/01_parameters.py` |
| 7 | SQL parameter access | `src/sdp/sql/01_parameters.sql` |
| 8 | Python parameter access | `src/sdp/python/01_parameters.py` |
| 9 | SQL → Python parameter passing | `src/sdp/sql/01_parameters.sql` → `src/sdp/python/03_dynamic_target.py` |
| 10 | Python → SQL parameter passing | `src/sdp/python/05_metadata_driven.py` → `src/sdp/sql/05_parameter_metadata.sql` |
| 11 | Workflow task parameter passing | `src/workflows/01-04*.py` |
| 12 | Task values (set/get) | `src/workflows/01_parameter_producer.py`, `02_parameter_consumer.py` |
| 13 | Dynamic task values | `src/workflows/03_task_values.py` |
| 14 | Job parameters | `resources/jobs.yml` (`parameters:` section) |
| 15 | Task parameters | `resources/jobs.yml` (`base_parameters:` sections) |
| 16 | Notebook parameters (widgets) | `src/workflows/*.py` (`dbutils.widgets.get()`) |
| 17 | Spark configuration parameters | `resources/jobs.yml` (`spark_conf:` sections) |
| 18 | `spark.conf.get()` | `src/sdp/python/01_parameters.py` |
| 19 | SQL `${parameter}` substitution | `src/sdp/sql/01_parameters.sql` |
| 20 | Dynamic table names | `src/sdp/sql/03_dynamic_target.sql` |
| 21 | Dynamic schema names | `resources/environments/*.yml` |
| 22 | Dynamic catalog names | `databricks.yml` (`${var.catalog}`) |
| 23 | Dynamic paths | `src/sdp/sql/02_dynamic_source.sql` |
| 24 | Dynamic source configuration | `src/sdp/python/02_dynamic_source.py` |
| 25 | JSON configuration | `src/metadata/pipeline_config.json` |
| 26 | Metadata-driven configuration | `src/sdp/python/05_metadata_driven.py` |
| 27 | Environment-specific config | `resources/environments/*.yml` |
| 28 | Parameter precedence | `databricks.yml` (bundle default < target override < CLI) |
| 29 | Parameter validation | `src/sdp/python/04_parameter_validation.py`, `src/common/validation.py` |
| 30 | Default values | `databricks.yml`, `src/common/config.py` |
| 31 | Optional parameters | `src/common/parameter_utils.py` (`safe_get_config`) |
| 32 | Runtime parameters | `src/workflows/*.py` (task values) |
| 33 | Parameters vs configuration | This README (see Concept Reference) |
| 34 | Parameters vs task values | `src/workflows/03_task_values.py` |
| 35 | Parameters vs widgets | `src/workflows/01_parameter_producer.py` |
| 36 | Parameters vs Spark config | `src/workflows/01_parameter_producer.py` |
| 37 | Parameters vs secrets | (documented in concept reference) |
| 38 | Secure configuration | (documented — use Databricks secrets) |
| 39 | Dynamic config from metadata | `src/sdp/python/05_metadata_driven.py` |
| 40 | Multi-layer parameter propagation | SQL → Python → SQL → Python (SDP files) |
| 41 | Passing params through pipeline layers | Bronze → Silver → Gold (all SDP files) |
| 42 | Failure handling for missing/invalid params | `src/sdp/python/04_parameter_validation.py` |

---

## Project Structure

```text
parameter-lab/
|
|-- databricks.yml                      # Main bundle config: variables, workspace, targets, includes
|
|-- resources/
|   |-- jobs.yml                        # Workflow job definition with 5 tasks
|   |-- pipelines.yml                   # SDP pipeline definition (SQL + Python)
|   |-- variables.yml                   # Additional bundle variables (JSON, numeric, etc.)
|   |-- environments/
|       |-- dev.yml                      # Dev target: schema=parameter_lab_dev
|       |-- test.yml                     # Test target: schema=parameter_lab_test
|       |-- prod.yml                     # Prod target: schema=parameter_lab_prod
|
|-- src/
|   |-- sdp/
|   |   |-- sql/
|   |   |   |-- 01_parameters.sql        # Deploy-time substitution, config views
|   |   |   |-- 02_dynamic_source.sql    # Dynamic source paths and tables
|   |   |   |-- 03_dynamic_target.sql    # Dynamic target table names (bronze/silver/gold)
|   |   |   |-- 04_parameter_expressions.sql  # Advanced ${var.xxx} expressions
|   |   |   |-- 05_parameter_metadata.sql # Reading metadata tables in SQL
|   |   |
|   |   |-- python/
|   |       |-- 01_parameters.py         # spark.conf.get() runtime access
|   |       |-- 02_dynamic_source.py     # Dynamic Auto Loader with runtime paths
|   |       |-- 03_dynamic_target.py     # SQL→Python data flow, dynamic targets
|   |       |-- 04_parameter_validation.py  # Validation, failure handling
|   |       |-- 05_metadata_driven.py    # Metadata-driven pipeline config
|   |
|   |-- workflows/
|   |   |-- 01_parameter_producer.py     # Task A: reads metadata, publishes task values
|   |   |-- 02_parameter_consumer.py     # Task B: consumes task values, transforms
|   |   |-- 03_task_values.py           # Task C: advanced task value patterns
|   |   |-- 04_dynamic_parameters.py     # Task D: JSON parsing, dynamic SQL gen
|   |
|   |-- common/
|   |   |-- config.py                    # PipelineConfig dataclass
|   |   |-- parameter_utils.py           # Parameter manipulation utilities
|   |   |-- validation.py               # Parameter validation framework
|   |   |-- logging.py                   # Parameterized logging
|   |   |-- setup.py                    # Wheel packaging for common utils
|   |
|   |-- metadata/
|       |-- parameter_config.json        # Domain metadata configuration
|       |-- pipeline_config.json        # Full pipeline + environment config
|
|-- tests/
|   |-- test_parameters.py              # Unit tests for parameter utilities
|   |-- test_config.py                  # Unit tests for PipelineConfig
|
|-- README.md                            # This file
```

---

## Prerequisites

- Databricks CLI v0.200+ with Bundle support
- Unity Catalog enabled workspace
- Permission to create catalogs/schemas/tables
- Python 3.8+ (for local tests and wheel build)

---

## Quick Start

### 1. Validate the bundle

```bash
databricks bundle validate -t dev
```

### 2. Deploy to dev

```bash
databricks bundle deploy -t dev
```

### 3. Deploy to test or prod

```bash
databricks bundle deploy -t test
databricks bundle deploy -t prod
```

### 4. Override a variable at deploy time

```bash
databricks bundle deploy -t dev --var="processing_mode=full"
```

### 5. Run the workflow job

```bash
databricks bundle run parameter_lab_job -t dev
```

### 6. Run unit tests locally

```bash
cd parameter-lab
pip install pytest
pytest tests/ -v
```

---

## Parameter Passing Mechanisms

This project demonstrates **six distinct parameter passing mechanisms**. Understanding when to use each is the core learning objective.

### 1. DAB Bundle Variables (`${var.xxx}`) — DEPLOY TIME

**Where**: `databricks.yml`, `resources/variables.yml`, all YAML files, SQL files, Python files

DAB variables are resolved at **deploy time** — before any code runs. The deployed files contain literal values, not variable references.

```yaml
# In databricks.yml
variables:
  catalog:
    default: main

# In any YAML/SQL/Python file (after deploy-time substitution)
# ${var.catalog} becomes 'main' (or the target override value)
```

**Precedence**: CLI `--var` > target override > bundle default

### 2. Pipeline Configuration (`spark.conf.get()`) — RUNTIME

**Where**: `resources/pipelines.yml` → `configuration:` section

Spark configuration keys set on every cluster in the pipeline. Accessible at **runtime** via:

```python
# Python (SDP and workflow tasks)
catalog = spark.conf.get("parameter_lab.catalog")
```

```sql
-- SQL: NOT directly accessible. This is a documented limitation.
-- Workaround: Python writes config to a table, SQL reads that table.
```

### 3. Notebook Parameters / Widgets (`dbutils.widgets.get()`) — RUNTIME

**Where**: `resources/jobs.yml` → `parameters:` and `base_parameters:`

Job-level parameters are available to all tasks as widgets. Task-level parameters override job-level for that task.

```python
# In a workflow task Python file
catalog = dbutils.widgets.get("catalog")  # Always returns a string
```

**Precedence**: task `base_parameters` > job `parameters` > default

### 4. Task Values (`dbutils.jobs.taskValues`) — RUNTIME

**Where**: `src/workflows/01-04*.py`

Task values are runtime values set by one task and read by downstream tasks. They are **ephemeral** (exist only during the job run) and can be **any JSON type**.

```python
# Set (in Task A)
dbutils.jobs.taskValues.set(key="domain", value="finance")
dbutils.jobs.taskValues.set(key="config", value={"nested": "dict"})

# Get (in Task B)
domain = dbutils.jobs.taskValues.get(taskKey="task_a", key="domain")
config = dbutils.jobs.taskValues.get(taskKey="task_a", key="config")
```

### 5. Spark Configuration (`spark_conf` in jobs.yml) — RUNTIME

**Where**: `resources/jobs.yml` → `spark_conf:` sections

Different from both widgets and pipeline configuration. These are Spark-level configs set on the task's cluster.

```python
# Access via spark.conf.get() (same API as pipeline config)
task_name = spark.conf.get("parameter_lab.task")
```

### 6. Table-Based Bridge (Python ↔ SQL) — RUNTIME

**Where**: `src/sdp/python/01_parameters.py` → `src/sdp/sql/05_parameter_metadata.sql`

The universal bridge between Python and SQL in SDP. Python writes config/data to a Delta table; SQL reads it.

```python
# Python writes config to a table
@dlt.table
def pipeline_config_python():
    return spark.createDataFrame([("catalog", "main"), ...])
```

```sql
-- SQL reads that table
SELECT config_value FROM ${var.catalog}.${var.schema}.pipeline_config_python
WHERE config_key = 'catalog';
```

---

## DAB Bundle Configuration

### Variables and Precedence

```text
Priority (lowest → highest):
  1. Bundle-level default (databricks.yml → variables:)
  2. Target-level override (resources/environments/dev.yml → variables:)
  3. CLI flag (--var="key=value")
```

### Target-Specific Behavior

| Target | Schema | Processing Mode | Retention | Max Files |
|--------|--------|---------------|----------|-----------|
| dev | `parameter_lab_dev` | incremental | 7 days | 50 |
| test | `parameter_lab_test` | incremental | 30 days | 100 |
| prod | `parameter_lab_prod` | full | 365 days | 500 |

The **same code** (SQL, Python) deploys differently per target because `${var.xxx}` is substituted at deploy time.

---

## SDP Pipeline

The pipeline mixes SQL and Python datasets to demonstrate parameter flow:

```text
SQL (01_parameters.sql)        →  Creates config views with deploy-time values
  ↓
SQL (02_dynamic_source.sql)    →  Dynamic source definitions
  ↓
Python (01_parameters.py)      →  Reads spark.conf, creates config table
  ↓
Python (02_dynamic_source.py)  →  Dynamic Auto Loader with runtime paths
  ↓
SQL (03_dynamic_target.sql)    →  Bronze → Silver → Gold with dynamic names
  ↓
Python (03_dynamic_target.py)  →  Reads SQL-created tables, transforms
  ↓
Python (04_parameter_validation.py) →  Validates all parameters
  ↓
Python (05_metadata_driven.py) →  Creates metadata table, drives processing
  ↓
SQL (05_parameter_metadata.sql) →  Reads metadata table created by Python
```

### SQL ↔ Python Parameter Flow

**SQL → Python**: SQL creates tables/views with deploy-time values. Python reads those tables via `dlt.read()` or `spark.table()`. A SQL query result is NOT a Python variable — you read the TABLE that SQL created.

**Python → SQL**: Python writes configuration into a Delta table. SQL reads that table. This is the **only** supported mechanism — SQL cannot read `spark.conf` or Python variables directly.

---

## Workflow Parameter Passing

### Task Flow

```text
Task A (producer)
  |
  |  Reads: job params (widgets), task params (widgets), spark.conf
  |  Publishes: source_table, load_type, watermark, processing_date,
  |            source_path, domain, full_config (dict), all_domains (list)
  ↓
Task B (consumer)
  |
  |  Consumes: task values from Task A
  |  Publishes: target_table, watermark_value, processing_config (dict)
  ↓
Task C (task_values)
  |
  |  Consumes: task values from Task A AND Task B
  |  Publishes: processing_manifest (dict), manifest_id
  |  Demonstrates: all task value types (str, int, float, bool, dict, list, None)
  ↓
Task D (dynamic_params)
  |
  |  Consumes: task values from Task C, JSON config from widget
  |  Publishes: dynamic_sql, unified_config, pipeline_params
  ↓
Task E (pipeline)
     Triggers the SDP pipeline
```

### Parameters vs Task Values

| Aspect | Parameters (Widgets) | Task Values |
|--------|---------------------|------------|
| When set | Before job run (in YAML) | At runtime (in task code) |
| Scope | All tasks (job) or one task | Downstream tasks only |
| Type | String only | Any JSON type |
| Persistence | Defined in job config | Ephemeral (job run only) |
| Size limit | N/A | ~1 MB per value |
| Available in SDP | No | No (pipeline tasks can't read them) |

---

## Metadata-Driven Configuration

The `parameter_config` table drives all pipeline processing:

| domain | source_system | source_table | target_table | load_type | active_flag |
|--------|--------------|-------------|-------------|-----------|-------------|
| finance | azure_sql | customers | customers_bronze | incremental | true |
| retail | azure_sql | orders | orders_bronze | incremental | true |
| hr | salesforce | employees | employees_bronze | full | true |
| inventory | azure_sql | products | products_bronze | incremental | **false** |

The `parameters` column contains JSON:
```json
{
  "partition_column": "country",
  "max_files": 100,
  "quality_check": true
}
```

Python reads this metadata and dynamically generates a processing plan. SQL then reads the metadata table to discover which domains to process.

---

## Dynamic JSON Parameters

JSON configuration is passed as a notebook parameter (string) and parsed in Python:

```json
{
  "source": "azure_sql",
  "schema": "retail",
  "table": "customer",
  "load_type": "incremental",
  "watermark_column": "modified_date"
}
```

Flow: JSON (jobs.yml) → widget parameter → `json.loads()` → Python dict → validation → transformation → task values → downstream pipeline.

---

## Testing

```bash
# Run all tests
cd parameter-lab
pip install pytest
pytest tests/ -v

# Run specific test file
pytest tests/test_parameters.py -v
pytest tests/test_config.py -v
```

Tests cover:
- JSON parameter parsing
- Dynamic name building (FQTN, layered names)
- Config merging and precedence
- Type conversion
- Parameter validation (required, choice, range, type)
- Environment-specific configuration behavior

---

## Concept Reference

### Parameters vs Configuration

- **Parameters** are values passed INTO a pipeline/job at definition or run time. They control *what* the code does.
- **Configuration** is the set of all settings that define the environment and behavior. Parameters are one source of configuration.

### Parameters vs Task Values

- **Parameters** are defined before the job runs (in YAML). They are static within a single job run.
- **Task values** are generated at runtime by a task. They enable dynamic, data-driven workflows.

### Parameters vs Widgets

- In Databricks Workflows, notebook parameters ARE widgets. `dbutils.widgets.get("param_name")` reads both job-level and task-level parameters.
- In standalone notebooks, widgets are interactive UI elements. In job tasks, they are pre-populated from the job definition.

### Parameters vs Spark Configuration

- **Spark configuration** (`spark.conf`) is set at the cluster/session level. It affects Spark's behavior globally.
- **Parameters** (widgets) are notebook-level values. They do not affect Spark's engine behavior.
- Both are accessed differently: `dbutils.widgets.get()` vs `spark.conf.get()`.

### Parameters vs Secrets

- **Secrets** are sensitive values stored in Databricks secret scopes. They are NEVER logged or displayed.
- **Parameters** are non-sensitive configuration values. They are visible in job definitions and logs.
- Access: `dbutils.secrets.get(scope="my_scope", key="my_key")` vs `dbutils.widgets.get("my_param")`.
- For secure configuration (passwords, tokens), ALWAYS use secrets, never parameters.

### Deploy-Time vs Runtime

| Aspect | Deploy-Time (`${var.xxx}`) | Runtime (`spark.conf.get()`) |
|--------|--------------------------|----------------------------|
| When resolved | At `databricks bundle deploy` | At pipeline/job execution |
| Where used | File content (SQL, Python, YAML) | Python code only |
| SQL access | Yes (baked into SQL text) | No (not accessible from SQL) |
| Can change at runtime | No (requires redeploy) | Yes (via pipeline config) |
| Use case | Table names, schema names, static config | Dynamic behavior, conditional logic |

---

## License

This is a learning project. Use freely for educational purposes.
