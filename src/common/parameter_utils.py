# =============================================================================
# parameter_utils.py — Parameter manipulation utilities
# =============================================================================
# Utility functions for parameter handling across the project.
# Demonstrates:
#   - Parameter parsing and type conversion
#   - JSON parameter handling
#   - Dynamic name construction
#   - Parameter merging and precedence resolution
# =============================================================================

import json
from typing import Any, Dict, Optional, Union


def parse_json_parameter(json_str: str) -> Dict[str, Any]:
    """
    Parse a JSON string parameter (from a widget or spark.conf) into a dict.

    Widget parameters are always strings. When passing JSON as a parameter,
    you must parse it in Python to get structured data.

    Args:
        json_str: JSON string from a parameter source

    Returns:
        Parsed dictionary

    Raises:
        json.JSONDecodeError: If the string is not valid JSON
    """
    if not json_str or json_str.strip() == "":
        return {}
    return json.loads(json_str)


def safe_get_config(spark, key: str, default: Any = None) -> Any:
    """
    Safely read a Spark configuration value with a default.

    This is the recommended pattern for reading pipeline configuration.
    It handles the case where the key is not set without raising an exception.

    Args:
        spark: SparkSession instance
        key: Configuration key (e.g., 'parameter_lab.catalog')
        default: Default value if key is not found

    Returns:
        The configuration value or the default
    """
    try:
        return spark.conf.get(key)
    except Exception:
        return default


def safe_get_widget(dbutils, key: str, default: Any = None) -> Any:
    """
    Safely read a notebook widget parameter with a default.

    Args:
        dbutils: dbutils instance
        key: Widget name
        default: Default value if widget is not found

    Returns:
        The widget value or the default
    """
    try:
        return dbutils.widgets.get(key)
    except Exception:
        return default


def safe_get_task_value(dbutils, task_key: str, key: str, default: Any = None) -> Any:
    """
    Safely read a task value from an upstream task.

    Task values are runtime values set by upstream tasks in a Databricks job.
    They are NOT the same as parameters (widgets) — they are generated at
    runtime, not defined in the job YAML.

    Args:
        dbutils: dbutils instance
        task_key: The task_key of the producing task (from jobs.yml)
        key: The key that was set by the producing task
        default: Default value if the task value is not found

    Returns:
        The task value or the default
    """
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=default)
    except Exception:
        return default


def build_fqtn(catalog: str, schema: str, table: str) -> str:
    """
    Build a fully qualified table name.

    Args:
        catalog: Unity Catalog catalog name
        schema: Schema name
        table: Table name

    Returns:
        'catalog.schema.table'
    """
    return f"{catalog}.{schema}.{table}"


def build_layered_name(base: str, suffix: str) -> str:
    """
    Build a layered table name by appending a suffix.

    Args:
        base: Base table name (e.g., 'customers')
        suffix: Layer suffix (e.g., '_bronze', '_silver', '_gold')

    Returns:
        'customers_bronze'
    """
    return f"{base}{suffix}"


def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple configuration dictionaries.

    Later configs override earlier ones (parameter precedence).
    This simulates how DAB merges bundle defaults with target overrides.

    Args:
        *configs: Configuration dictionaries in priority order (low to high)

    Returns:
        Merged configuration dictionary
    """
    result = {}
    for config in configs:
        if config:
            result.update(config)
    return result


def convert_param_type(value: str, target_type: type) -> Any:
    """
    Convert a string parameter to the target type.

    Widget parameters are always strings. This utility converts them
    to the appropriate Python type.

    Args:
        value: String value from a parameter
        target_type: Desired Python type (str, int, float, bool)

    Returns:
        Converted value
    """
    if target_type == bool:
        return value.lower() in ("true", "1", "yes")
    elif target_type == int:
        return int(value)
    elif target_type == float:
        return float(value)
    else:
        return str(value)


def extract_json_field(json_str: str, field_path: str) -> Any:
    """
    Extract a field from a JSON parameter string using dot notation.

    Args:
        json_str: JSON string parameter
        field_path: Dot-separated path (e.g., 'source.table')

    Returns:
        The field value or None if not found
    """
    try:
        data = json.loads(json_str)
        for key in field_path.split("."):
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data
    except (json.JSONDecodeError, AttributeError):
        return None
