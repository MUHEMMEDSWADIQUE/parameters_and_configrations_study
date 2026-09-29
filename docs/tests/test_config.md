# How `test_config.py` Works

## Purpose

Unit tests for the `PipelineConfig` dataclass in `src/common/config.py`. Tests defaults, table name builders, environment-based config, and pipeline config validation.

## Key Concepts Demonstrated

- **Default value testing** — verifying all defaults in `PipelineConfig`
- **Dynamic table name testing** — bronze/silver/gold table name construction
- **Environment-specific config testing** — different configs for dev vs prod
- **Config validation testing** — `validate_pipeline_config()` with valid and invalid inputs
- **Same code, different config** — proving the same class produces different results per environment

## Test Classes

| Class | What It Tests |
|---|---|
| `TestPipelineConfigDefaults` | All default values in the dataclass |
| `TestPipelineConfigTableBuilders` | `fully_qualified()`, `bronze_table()`, `silver_table()`, `gold_table()` |
| `TestPipelineConfigFromEnv` | Building config from environment variables (with defaults and overrides) |
| `TestPipelineConfigToDict` | Serialization to dictionary |
| `TestPipelineConfigRepr` | String representation |
| `TestValidatePipelineConfig` | Full config validation (valid, invalid environment, invalid retention) |
| `TestEnvironmentSpecificConfig` | Dev vs prod config comparison |

## Key Test Examples

### Same Code, Different Config (the core demo)
```python
def test_same_code_different_config(self):
    dev_config = PipelineConfig(catalog="main", schema="parameter_lab_dev")
    prod_config = PipelineConfig(catalog="main", schema="parameter_lab_prod")

    dev_table = dev_config.bronze_table("customers")
    prod_table = prod_config.bronze_table("customers")

    assert dev_table == "main.parameter_lab_dev.customers_bronze"
    assert prod_table == "main.parameter_lab_prod.customers_bronze"
    assert dev_table != prod_table
```

This test is the **essence of the entire project**: the same code (PipelineConfig) produces different results based on parameters (schema name). This is exactly what DAB deploy-time substitution achieves, but demonstrated in a unit test.

### Environment Variable Override Test
```python
def test_from_env_override(self, monkeypatch):
    monkeypatch.setenv("PARAMETER_LAB_CATALOG", "my_catalog")
    config = PipelineConfig.from_env()
    assert config.catalog == "my_catalog"
```

This demonstrates that environment variables can override defaults — another parameter source.

### Invalid Config Validation
```python
def test_invalid_environment(self):
    config_dict = {"environment": "staging", ...}
    results = validate_pipeline_config(config_dict)
    env_results = [r for r in results if r.param_name == "environment"]
    assert any(not r.is_valid for r in env_results)
```

This verifies that `staging` is rejected as an invalid environment value.

## How to Run

```bash
cd parameter-lab
pip install pytest
pytest tests/test_config.py -v
```
