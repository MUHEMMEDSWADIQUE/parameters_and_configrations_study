# How `validation.py` Works

## Purpose

Provides a parameter validation framework with typed result objects, validation functions for different constraints, and a batch validation runner.

## Key Concepts Demonstrated

- **Required parameter validation** — `validate_required()` checks for non-null, non-empty
- **Choice validation** — `validate_choice()` checks against an allowed value list
- **Type validation** — `validate_type()` checks isinstance
- **Range validation** — `validate_range()` checks numeric bounds
- **Custom validation** — `validate_custom()` with a callable predicate
- **Batch validation** — `validate_all()` collects all errors and raises if any fail
- **Pipeline config validation** — `validate_pipeline_config()` validates a complete config dict

## How It Works

### Individual Validators
```python
from parameter_lab_common.validation import validate_required, validate_choice

result = validate_required("catalog", "main")
# ValidationResult(param_name="catalog", param_value="main", is_valid=True, message="OK")

result = validate_choice("environment", "staging", ["dev", "test", "prod"])
# ValidationResult(is_valid=False, message="'staging' is not in valid values: ['dev', 'test', 'prod"])
```

### Batch Validation (fail-fast with full error report)
```python
results = [
    validate_required("catalog", "main"),
    validate_required("schema", None),       # FAIL
    validate_choice("environment", "dev", ["dev", "test", "prod"]),
]
validate_all(results)
# Raises ParameterValidationError with ALL failures listed:
# Parameter validation failed with 1 error(s):
#   - [FAIL] schema=None — Required parameter 'schema' is missing or empty
```

### Complete Pipeline Config Validation
```python
config_dict = {
    "catalog": "main",
    "schema": "parameter_lab_dev",
    "environment": "dev",
    "processing_mode": "incremental",
    "retention_days": 30,
    "max_files_per_trigger": 100,
    "enable_quality_checks": True,
}
results = validate_pipeline_config(config_dict)
# Returns list of ValidationResult objects
```

This function validates all required fields, choice constraints, and numeric ranges in one call. Used by `04_parameter_validation.py` in the SDP pipeline.

### ValidationResult Dataclass

Each validation returns a `ValidationResult` with:
- `param_name` — the parameter key
- `param_value` — the value checked
- `is_valid` — boolean pass/fail
- `message` — human-readable status

This allows building a validation results table in the pipeline for auditability.
