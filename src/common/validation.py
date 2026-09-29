# =============================================================================
# validation.py — Parameter and configuration validation
# =============================================================================
# Provides validation utilities for pipeline parameters.
# Demonstrates:
#   - Required parameter checking
#   - Valid value enumeration
#   - Type validation
#   - Custom validation rules
#   - Failure handling with informative errors
# =============================================================================

from typing import Any, List, Optional, Callable
from dataclasses import dataclass


class ParameterValidationError(Exception):
    """Raised when a required parameter is missing or invalid."""
    pass


@dataclass
class ValidationResult:
    """Result of a validation check."""
    param_name: str
    param_value: Any
    is_valid: bool
    message: str = ""

    def __repr__(self) -> str:
        status = "PASS" if self.is_valid else "FAIL"
        return f"[{status}] {self.param_name}={self.param_value} — {self.message}"


def validate_required(key: str, value: Any) -> ValidationResult:
    """Validate that a required parameter is present and non-empty."""
    if value is None or (isinstance(value, str) and value == ""):
        return ValidationResult(
            param_name=key,
            param_value=value,
            is_valid=False,
            message=f"Required parameter '{key}' is missing or empty"
        )
    return ValidationResult(
        param_name=key,
        param_value=value,
        is_valid=True,
        message="OK"
    )


def validate_choice(key: str, value: Any, valid_values: List[Any]) -> ValidationResult:
    """Validate that a parameter value is in a list of allowed values."""
    if value not in valid_values:
        return ValidationResult(
            param_name=key,
            param_value=value,
            is_valid=False,
            message=f"'{value}' is not in valid values: {valid_values}"
        )
    return ValidationResult(
        param_name=key,
        param_value=value,
        is_valid=True,
        message="OK"
    )


def validate_type(key: str, value: Any, expected_type: type) -> ValidationResult:
    """Validate that a parameter value is of the expected type."""
    if not isinstance(value, expected_type):
        return ValidationResult(
            param_name=key,
            param_value=value,
            is_valid=False,
            message=f"Expected type {expected_type.__name__}, got {type(value).__name__}"
        )
    return ValidationResult(
        param_name=key,
        param_value=value,
        is_valid=True,
        message="OK"
    )


def validate_range(key: str, value: Any, min_val: Any = None, max_val: Any = None) -> ValidationResult:
    """Validate that a numeric parameter is within a range."""
    try:
        num_val = type(min_val or max_val or 0)(value)
    except (ValueError, TypeError):
        return ValidationResult(
            param_name=key,
            param_value=value,
            is_valid=False,
            message=f"Cannot convert '{value}' to a number"
        )

    if min_val is not None and num_val < min_val:
        return ValidationResult(
            param_name=key, param_value=value, is_valid=False,
            message=f"{num_val} is below minimum {min_val}"
        )
    if max_val is not None and num_val > max_val:
        return ValidationResult(
            param_name=key, param_value=value, is_valid=False,
            message=f"{num_val} is above maximum {max_val}"
        )
    return ValidationResult(
        param_name=key, param_value=value, is_valid=True, message="OK"
    )


def validate_custom(key: str, value: Any, validator: Callable[[Any], bool],
                     error_msg: str = "Custom validation failed") -> ValidationResult:
    """Validate using a custom validation function."""
    if not validator(value):
        return ValidationResult(
            param_name=key, param_value=value, is_valid=False, message=error_msg
        )
    return ValidationResult(
        param_name=key, param_value=value, is_valid=True, message="OK"
    )


def validate_all(results: List[ValidationResult]) -> bool:
    """
    Check if all validation results are valid.

    Raises ParameterValidationError if any validation failed,
    with a summary of all failures.
    """
    failures = [r for r in results if not r.is_valid]
    if failures:
        error_messages = "\n".join(f"  - {r}" for r in failures)
        raise ParameterValidationError(
            f"Parameter validation failed with {len(failures)} error(s):\n{error_messages}"
        )
    return True


def validate_pipeline_config(config_dict: dict) -> List[ValidationResult]:
    """
    Validate a complete pipeline configuration dictionary.

    This is the main entry point for validating pipeline parameters.
    It checks all required and optional parameters.

    Args:
        config_dict: Dictionary of configuration key-value pairs

    Returns:
        List of ValidationResult objects
    """
    results = []

    # Required parameters
    for key in ["catalog", "schema", "environment"]:
        results.append(validate_required(key, config_dict.get(key)))

    # Choice validation
    results.append(
        validate_choice("environment", config_dict.get("environment"), ["dev", "test", "prod"])
    )
    results.append(
        validate_choice("processing_mode", config_dict.get("processing_mode", "incremental"),
                        ["incremental", "full"])
    )

    # Range validation
    results.append(
        validate_range("retention_days", config_dict.get("retention_days", 7), min_val=1, max_val=3650)
    )
    results.append(
        validate_range("max_files_per_trigger", config_dict.get("max_files_per_trigger", 100),
                        min_val=1, max_val=10000)
    )

    # Type validation for boolean
    quality = config_dict.get("enable_quality_checks", True)
    results.append(validate_type("enable_quality_checks", quality, bool))

    return results
