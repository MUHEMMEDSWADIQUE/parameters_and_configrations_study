# =============================================================================
# test_config.py — Tests for PipelineConfig and configuration management
# =============================================================================
# Run with:  pytest tests/test_config.py -v
#
# Tests the PipelineConfig dataclass and configuration utilities.
# Demonstrates testing of parameterized configuration objects.
# =============================================================================

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "common"))

from config import PipelineConfig
from validation import validate_pipeline_config


class TestPipelineConfigDefaults:
    def test_default_values(self):
        config = PipelineConfig()
        assert config.catalog == "main"
        assert config.schema == "parameter_lab"
        assert config.environment == "dev"
        assert config.processing_mode == "incremental"
        assert config.max_files_per_trigger == 100
        assert config.enable_quality_checks is True
        assert config.retention_days == 7


class TestPipelineConfigTableBuilders:
    def test_fully_qualified(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_dev")
        assert config.fully_qualified("my_table") == "main.parameter_lab_dev.my_table"

    def test_bronze_table(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_dev")
        assert config.bronze_table("customers") == "main.parameter_lab_dev.customers_bronze"

    def test_silver_table(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_test")
        assert config.silver_table("orders") == "main.parameter_lab_test.orders_silver"

    def test_gold_table(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_prod")
        assert config.gold_table("metrics") == "main.parameter_lab_prod.metrics_gold"


class TestPipelineConfigFromEnv:
    def test_from_env_defaults(self, monkeypatch):
        monkeypatch.delenv("PARAMETER_LAB_CATALOG", raising=False)
        config = PipelineConfig.from_env()
        assert config.catalog == "main"

    def test_from_env_override(self, monkeypatch):
        monkeypatch.setenv("PARAMETER_LAB_CATALOG", "my_catalog")
        config = PipelineConfig.from_env()
        assert config.catalog == "my_catalog"


class TestPipelineConfigToDict:
    def test_to_dict(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_dev", environment="dev")
        d = config.to_dict()
        assert d["catalog"] == "main"
        assert d["schema"] == "parameter_lab_dev"
        assert d["environment"] == "dev"
        assert "processing_mode" in d
        assert "bronze_suffix" in d


class TestPipelineConfigRepr:
    def test_repr(self):
        config = PipelineConfig(catalog="main", schema="parameter_lab_dev", environment="dev")
        repr_str = repr(config)
        assert "main" in repr_str
        assert "parameter_lab_dev" in repr_str
        assert "dev" in repr_str


class TestValidatePipelineConfig:
    def test_valid_config(self):
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
        assert all(r.is_valid for r in results)

    def test_invalid_environment(self):
        config_dict = {
            "catalog": "main",
            "schema": "parameter_lab_dev",
            "environment": "staging",
            "processing_mode": "incremental",
            "retention_days": 30,
            "max_files_per_trigger": 100,
            "enable_quality_checks": True,
        }
        results = validate_pipeline_config(config_dict)
        env_results = [r for r in results if r.param_name == "environment"]
        assert any(not r.is_valid for r in env_results)

    def test_invalid_retention(self):
        config_dict = {
            "catalog": "main",
            "schema": "parameter_lab_dev",
            "environment": "dev",
            "processing_mode": "incremental",
            "retention_days": -1,
            "max_files_per_trigger": 100,
            "enable_quality_checks": True,
        }
        results = validate_pipeline_config(config_dict)
        retention_results = [r for r in results if r.param_name == "retention_days"]
        assert any(not r.is_valid for r in retention_results)


class TestEnvironmentSpecificConfig:
    """Tests that demonstrate environment-specific configuration behavior."""

    def test_dev_config(self):
        config = PipelineConfig(
            catalog="main",
            schema="parameter_lab_dev",
            environment="dev",
            processing_mode="incremental",
            retention_days=7,
        )
        assert config.schema.endswith("_dev")
        assert config.processing_mode == "incremental"

    def test_prod_config(self):
        config = PipelineConfig(
            catalog="main",
            schema="parameter_lab_prod",
            environment="prod",
            processing_mode="full",
            retention_days=365,
        )
        assert config.schema.endswith("_prod")
        assert config.processing_mode == "full"
        assert config.retention_days == 365

    def test_same_code_different_config(self):
        """Demonstrate that the same PipelineConfig class produces
        different table names based on environment parameters."""
        dev_config = PipelineConfig(catalog="main", schema="parameter_lab_dev")
        prod_config = PipelineConfig(catalog="main", schema="parameter_lab_prod")

        dev_table = dev_config.bronze_table("customers")
        prod_table = prod_config.bronze_table("customers")

        assert dev_table == "main.parameter_lab_dev.customers_bronze"
        assert prod_table == "main.parameter_lab_prod.customers_bronze"
        assert dev_table != prod_table
