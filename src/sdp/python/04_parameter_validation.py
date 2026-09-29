# =============================================================================
# 04_parameter_validation.py — Parameter validation & failure handling (SDP)
# =============================================================================
# LEARNING OBJECTIVES:
#   1. Validate parameters before processing starts
#   2. Define valid values and enforce them
#   3. Handle missing, invalid, or empty parameters gracefully
#   4. Use SDP expectations for data-level validation
#   5. Demonstrate failure handling patterns
#
# CONCEPT — Parameter validation happens at TWO levels:
#   1. CONFIG-LEVEL: Are the pipeline parameters present and valid?
#      (checked in Python before any @dlt function runs)
#   2. DATA-LEVEL: Does the data conform to expectations?
#      (checked via @dlt.expect / @dlt.expect_all decorators)
# =============================================================================

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count

spark = SparkSession.builder.getOrCreate()

# -----------------------------------------------------------------------------
# VALIDATION FRAMEWORK
# -----------------------------------------------------------------------------

class ParameterValidationError(Exception):
    """Raised when a required parameter is missing or invalid."""
    pass


def validate_required(key: str, value: str, valid_values: list = None) -> str:
    """
    Validate a required parameter.

    Args:
        key:          The configuration key (for error messages)
        value:        The value to validate
        valid_values: Optional list of allowed values. If None, any non-empty
                      string is accepted.

    Returns:
        The validated value.

    Raises:
        ParameterValidationError: If the value is missing or invalid.
    """
    if value is None or value == "":
        raise ParameterValidationError(
            f"Parameter '{key}' is required but not set or empty. "
            f"Check the pipeline configuration in pipelines.yml."
        )

    if valid_values is not None and value not in valid_values:
        raise ParameterValidationError(
            f"Parameter '{key}' has invalid value '{value}'. "
            f"Valid values: {valid_values}"
        )

    print(f"  [VALID] {key} = {value}")
    return value


def validate_optional(key: str, value: str, default: str, valid_values: list = None) -> str:
    """
    Validate an optional parameter with a default value.

    Returns the value if present and valid, or the default if missing.
    """
    if value is None or value == "":
        print(f"  [OPTIONAL] {key} not set, using default: {default}")
        return default

    if valid_values is not None and value not in valid_values:
        print(f"  [WARNING] {key} has value '{value}' not in {valid_values}. Using default: {default}")
        return default

    print(f"  [VALID] {key} = {value}")
    return value


# -----------------------------------------------------------------------------
# VALIDATE ALL PARAMETERS
# This runs BEFORE any @dlt function executes, validating the pipeline config.
# If any required parameter is missing/invalid, the pipeline fails fast.
# -----------------------------------------------------------------------------
print("=" * 70)
print("PARAMETER VALIDATION")
print("=" * 70)

CATALOG = validate_required(
    "parameter_lab.catalog",
    spark.conf.get("parameter_lab.catalog", None),
)

SCHEMA = validate_required(
    "parameter_lab.schema",
    spark.conf.get("parameter_lab.schema", None),
)

ENVIRONMENT = validate_required(
    "parameter_lab.environment",
    spark.conf.get("parameter_lab.environment", None),
    valid_values=["dev", "test", "prod"],
)

PROCESSING_MODE = validate_optional(
    "parameter_lab.processing_mode",
    spark.conf.get("parameter_lab.processing_mode", None),
    default="incremental",
    valid_values=["incremental", "full"],
)

RETENTION_DAYS = validate_optional(
    "parameter_lab.retention_days",
    spark.conf.get("parameter_lab.retention_days", None),
    default="7",
)

# Validate retention_days is a positive integer
try:
    retention_int = int(RETENTION_DAYS)
    if retention_int <= 0:
        raise ParameterValidationError(
            f"parameter_lab.retention_days must be positive, got {retention_int}"
        )
except ValueError:
    raise ParameterValidationError(
        f"parameter_lab.retention_days must be an integer, got '{RETENTION_DAYS}'"
    )

print("=" * 70)
print("ALL PARAMETERS VALIDATED SUCCESSFULLY")
print("=" * 70)


# -----------------------------------------------------------------------------
# Create a validation results table.
# This table records which parameters were checked and their status.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.validation_results",
    comment="Results of parameter validation checks"
)
def validation_results():
    results = [
        ("parameter_lab.catalog", CATALOG, "required", "PASS", ""),
        ("parameter_lab.schema", SCHEMA, "required", "PASS", ""),
        ("parameter_lab.environment", ENVIRONMENT, "required", "PASS", ""),
        ("parameter_lab.processing_mode", PROCESSING_MODE, "optional", "PASS", ""),
        ("parameter_lab.retention_days", str(retention_int), "optional", "PASS", ""),
    ]
    return spark.createDataFrame(
        results,
        schema="param_name STRING, param_value STRING, param_type STRING, status STRING, message STRING",
    )


# -----------------------------------------------------------------------------
# DATA-LEVEL validation with conditional expectations.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.validated_config",
    comment="Pipeline config with data-level quality expectations",
)
@dlt.expect("non_null_key", "config_key IS NOT NULL")
@dlt.expect("non_null_value", "config_value IS NOT NULL")
@dlt.expect("non_empty_key", "config_key != ''")
def validated_config():
    # Read the config table created by 01_parameters.py
    config_table = f"{CATALOG}.{SCHEMA}.pipeline_config_python"
    return dlt.read(config_table)


# -----------------------------------------------------------------------------
# FAILURE HANDLING: demonstrate what happens with invalid data.
# This table uses expect_all_or_fail to STOP the pipeline if data is invalid.
# -----------------------------------------------------------------------------
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.critical_validation",
    comment="Critical validation — pipeline FAILS if expectations are not met",
)
@dlt.expect_all_or_fail({
    "catalog_not_null": "config_value IS NOT NULL",
    "catalog_not_empty": "config_value != ''",
})
def critical_validation():
    config_table = f"{CATALOG}.{SCHEMA}.pipeline_config_python"
    return (
        dlt.read(config_table)
        .filter(col("config_key") == "catalog")
    )
