# How `04_parameter_validation.py` Works

## Purpose

Demonstrates parameter validation and failure handling at two levels: config-level (before any data processing) and data-level (via SDP expectations).

## Key Concepts

- **Config-level validation** — checking pipeline parameters before processing starts
- **Required vs optional parameters** — `validate_required()` and `validate_optional()` helpers
- **Valid value enumeration** — e.g., `environment` must be `dev`, `test`, or `prod`
- **Type validation** — `retention_days` must be a positive integer
- **SDP expectations** — `@dlt.expect()`, `@dlt.expect_all_or_fail()` for data-level validation
- **Fail-fast pattern** — pipeline stops immediately if required parameters are invalid

## How It Works

### Config-Level Validation (runs first)
```python
CATALOG = validate_required("parameter_lab.catalog", spark.conf.get("parameter_lab.catalog", None))
ENVIRONMENT = validate_required("parameter_lab.environment", spark.conf.get("parameter_lab.environment", None),
                                valid_values=["dev", "test", "prod"])
```

If `catalog` is missing or `environment` is `staging` (not in the valid list), `ParameterValidationError` is raised and the pipeline fails immediately — before any `@dlt` function runs.

### Validation Results Table
```python
@dlt.table(name=f"{CATALOG}.{SCHEMA}.validation_results")
def validation_results():
    return spark.createDataFrame([
        ("parameter_lab.catalog", CATALOG, "required", "PASS", ""),
        ("parameter_lab.environment", ENVIRONMENT, "required", "PASS", ""),
        ...
    ])
```

This records all validation results as a table for auditability.

### Data-Level Validation (SDP Expectations)
```python
@dlt.table(name=f"{CATALOG}.{SCHEMA}.critical_validation")
@dlt.expect_all_or_fail({
    "catalog_not_null": "config_value IS NOT NULL",
    "catalog_not_empty": "config_value != ''",
})
def critical_validation():
    return dlt.read(config_table).filter(col("config_key") == "catalog")
```

`expect_all_or_fail` stops the pipeline if any expectation fails. Compare with `expect` (logs but continues) and `expect_all_or_drop` (drops failing rows).

### Two-Level Validation Summary

| Level | What | When | Failure Mode |
|---|---|---|---|
| Config-level | Are parameters present and valid? | Before any @dlt function | Python exception (fail fast) |
| Data-level | Does data meet expectations? | During each micro-batch | SDP expectation (drop/fail/warn) |
