# =============================================================================
# test_parameters.py — Tests for parameter handling utilities
# =============================================================================
# Run with:  pytest tests/test_parameters.py -v
#
# These tests verify the parameter utilities in src/common/ without
# requiring a live Databricks cluster. They test pure Python logic.
# =============================================================================

import json
import sys
import os
import pytest

# Add src/common to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "common"))

from parameter_utils import (
    parse_json_parameter,
    build_fqtn,
    build_layered_name,
    merge_configs,
    convert_param_type,
    extract_json_field,
)
from validation import (
    validate_required,
    validate_choice,
    validate_range,
    validate_type,
    validate_all,
    ParameterValidationError,
)


class TestParseJsonParameter:
    def test_valid_json(self):
        result = parse_json_parameter('{"source": "azure_sql", "table": "customers"}')
        assert result["source"] == "azure_sql"
        assert result["table"] == "customers"

    def test_empty_string(self):
        result = parse_json_parameter("")
        assert result == {}

    def test_none_input(self):
        result = parse_json_parameter(None)
        assert result == {}

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_json_parameter("{invalid json}")


class TestBuildFqtn:
    def test_standard(self):
        assert build_fqtn("main", "parameter_lab_dev", "customers_bronze") == "main.parameter_lab_dev.customers_bronze"

    def test_empty_parts(self):
        assert build_fqtn("", "", "") == ".."


class TestBuildLayeredName:
    def test_bronze(self):
        assert build_layered_name("customers", "_bronze") == "customers_bronze"

    def test_silver(self):
        assert build_layered_name("customers", "_silver") == "customers_silver"

    def test_gold(self):
        assert build_layered_name("customers", "_gold") == "customers_gold"


class TestMergeConfigs:
    def test_basic_merge(self):
        result = merge_configs({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_precedence(self):
        result = merge_configs({"schema": "default"}, {"schema": "override"})
        assert result["schema"] == "override"

    def test_empty_dicts(self):
        result = merge_configs({}, {}, {})
        assert result == {}


class TestConvertParamType:
    def test_to_bool_true(self):
        assert convert_param_type("true", bool) is True
        assert convert_param_type("True", bool) is True
        assert convert_param_type("1", bool) is True

    def test_to_bool_false(self):
        assert convert_param_type("false", bool) is False
        assert convert_param_type("0", bool) is False

    def test_to_int(self):
        assert convert_param_type("42", int) == 42

    def test_to_float(self):
        assert convert_param_type("3.14", float) == 3.14


class TestExtractJsonField:
    def test_simple_field(self):
        json_str = json.dumps({"source": "azure_sql"})
        assert extract_json_field(json_str, "source") == "azure_sql"

    def test_nested_field(self):
        json_str = json.dumps({"options": {"partition_column": "country"}})
        assert extract_json_field(json_str, "options.partition_column") == "country"

    def test_missing_field(self):
        json_str = json.dumps({"source": "azure_sql"})
        assert extract_json_field(json_str, "nonexistent") is None


class TestValidationRequired:
    def test_valid_value(self):
        result = validate_required("catalog", "main")
        assert result.is_valid is True

    def test_none_value(self):
        result = validate_required("catalog", None)
        assert result.is_valid is False

    def test_empty_string(self):
        result = validate_required("schema", "")
        assert result.is_valid is False


class TestValidationChoice:
    def test_valid_choice(self):
        result = validate_choice("environment", "dev", ["dev", "test", "prod"])
        assert result.is_valid is True

    def test_invalid_choice(self):
        result = validate_choice("environment", "staging", ["dev", "test", "prod"])
        assert result.is_valid is False


class TestValidationRange:
    def test_within_range(self):
        result = validate_range("retention_days", 30, min_val=1, max_val=365)
        assert result.is_valid is True

    def test_below_minimum(self):
        result = validate_range("retention_days", 0, min_val=1, max_val=365)
        assert result.is_valid is False

    def test_above_maximum(self):
        result = validate_range("retention_days", 500, min_val=1, max_val=365)
        assert result.is_valid is False


class TestValidateAll:
    def test_all_pass(self):
        results = [
            validate_required("catalog", "main"),
            validate_required("schema", "parameter_lab"),
        ]
        assert validate_all(results) is True

    def test_some_fail(self):
        results = [
            validate_required("catalog", "main"),
            validate_required("schema", None),
        ]
        with pytest.raises(ParameterValidationError):
            validate_all(results)
