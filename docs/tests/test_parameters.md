# How `test_parameters.py` Works

## Purpose

Unit tests for the parameter utility functions in `src/common/parameter_utils.py`. Tests pure Python logic without requiring a live Databricks cluster.

## Key Concepts Demonstrated

- **JSON parameter parsing** — valid JSON, empty string, invalid JSON
- **Dynamic name construction** — `build_fqtn()`, `build_layered_name()`
- **Config merging and precedence** — later configs override earlier ones
- **Type conversion** — string to bool/int/float
- **JSON field extraction** — dot-notation paths, missing fields
- **Validation framework** — required, choice, range, batch validation

## Test Classes

| Class | What It Tests |
|---|---|
| `TestParseJsonParameter` | JSON string parsing (valid, empty, None, invalid) |
| `TestBuildFqtn` | Fully qualified table name construction |
| `TestBuildLayeredName` | Bronze/silver/gold suffix application |
| `TestMergeConfigs` | Config merging with precedence |
| `TestConvertParamType` | String-to-bool/int/float conversion |
| `TestExtractJsonField` | Dot-notation JSON field extraction |
| `TestValidationRequired` | Required parameter validation (valid, None, empty) |
| `TestValidationChoice` | Choice validation (valid, invalid) |
| `TestValidationRange` | Numeric range validation (within, below, above) |
| `TestValidateAll` | Batch validation (all pass, some fail) |

## How to Run

```bash
cd parameter-lab
pip install pytest
pytest tests/test_parameters.py -v
```

## Key Test Examples

### Config Precedence Test
```python
def test_precedence(self):
    result = merge_configs({"schema": "default"}, {"schema": "override"})
    assert result["schema"] == "override"
```

This verifies that later configs override earlier ones — the same precedence model used by DAB.

### Type Conversion Test
```python
def test_to_bool_true(self):
    assert convert_param_type("true", bool) is True
    assert convert_param_type("1", bool) is True
```

This verifies that string widget values are correctly converted to Python bools.

### Batch Validation Test
```python
def test_some_fail(self):
    results = [validate_required("catalog", "main"), validate_required("schema", None)]
    with pytest.raises(ParameterValidationError):
        validate_all(results)
```

This verifies that `validate_all` raises when any validation fails, collecting ALL errors.
