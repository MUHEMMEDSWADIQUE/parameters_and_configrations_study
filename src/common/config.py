# =============================================================================
# config.py — Centralized configuration management
# =============================================================================
# This module provides a unified configuration interface that reads from
# multiple sources, demonstrating parameter precedence:
#
#   1. Environment variables (OS level)
#   2. Spark configuration (spark.conf.get)
#   3. Notebook widgets / job parameters (dbutils.widgets.get)
#   4. Default values (hardcoded fallbacks)
#
# PRECEDENCE (highest to lowest):
#   spark.conf > widget > env var > default
#
# This module is packaged as a wheel (via setup.py) and installed on
# pipeline and job clusters. It can be imported as:
#   from parameter_lab_common.config import PipelineConfig
# =============================================================================

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    """
    Centralized configuration object for the parameter_lab pipeline.

    Reads values from spark.conf, dbutils.widgets, and environment variables
    in order of precedence. Provides a typed, validated config object.
    """
    catalog: str = "main"
    schema: str = "parameter_lab"
    environment: str = "dev"
    source_path: str = "/Volumes/main/parameter_lab/raw"
    processing_mode: str = "incremental"
    max_files_per_trigger: int = 100
    enable_quality_checks: bool = True
    retention_days: int = 7
    bronze_suffix: str = "_bronze"
    silver_suffix: str = "_silver"
    gold_suffix: str = "_gold"

    @classmethod
    def from_spark_conf(cls, spark) -> "PipelineConfig":
        """
        Build config from Spark configuration (pipeline configuration section).

        This is the primary method for SDP pipelines where configuration
        is set in the pipeline's `configuration:` section.
        """
        def _get(key: str, default: str) -> str:
            try:
                return spark.conf.get(key)
            except Exception:
                return default

        return cls(
            catalog=_get("parameter_lab.catalog", "main"),
            schema=_get("parameter_lab.schema", "parameter_lab"),
            environment=_get("parameter_lab.environment", "dev"),
            source_path=_get("parameter_lab.source_path", "/Volumes/main/parameter_lab/raw"),
            processing_mode=_get("parameter_lab.processing_mode", "incremental"),
            max_files_per_trigger=int(_get("parameter_lab.max_files_per_trigger", "100")),
            enable_quality_checks=_get("parameter_lab.enable_quality_checks", "true").lower() == "true",
            retention_days=int(_get("parameter_lab.retention_days", "7")),
            bronze_suffix=_get("parameter_lab.bronze_suffix", "_bronze"),
            silver_suffix=_get("parameter_lab.silver_suffix", "_silver"),
            gold_suffix=_get("parameter_lab.gold_suffix", "_gold"),
        )

    @classmethod
    def from_widgets(cls, dbutils) -> "PipelineConfig":
        """
        Build config from notebook widgets / job parameters.

        This is used in workflow tasks where parameters are passed as
        notebook widgets (base_parameters).
        """
        def _get(key: str, default: str) -> str:
            try:
                return dbutils.widgets.get(key)
            except Exception:
                return default

        return cls(
            catalog=_get("catalog", "main"),
            schema=_get("schema", "parameter_lab"),
            environment=_get("environment", "dev"),
            source_path=_get("source_path", "/Volumes/main/parameter_lab/raw"),
            processing_mode=_get("processing_mode", "incremental"),
        )

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """
        Build config from environment variables.

        Useful for local development and testing outside Databricks.
        """
        def _get(key: str, default: str) -> str:
            return os.environ.get(key.upper().replace(".", "_"), default)

        return cls(
            catalog=_get("PARAMETER_LAB_CATALOG", "main"),
            schema=_get("PARAMETER_LAB_SCHEMA", "parameter_lab"),
            environment=_get("PARAMETER_LAB_ENVIRONMENT", "dev"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for logging and serialization."""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "environment": self.environment,
            "source_path": self.source_path,
            "processing_mode": self.processing_mode,
            "max_files_per_trigger": self.max_files_per_trigger,
            "enable_quality_checks": self.enable_quality_checks,
            "retention_days": self.retention_days,
            "bronze_suffix": self.bronze_suffix,
            "silver_suffix": self.silver_suffix,
            "gold_suffix": self.gold_suffix,
        }

    def fully_qualified(self, table_name: str) -> str:
        """Build a fully qualified table name: catalog.schema.table_name"""
        return f"{self.catalog}.{self.schema}.{table_name}"

    def bronze_table(self, base_name: str) -> str:
        """Build a bronze-layer table name: base_name + bronze_suffix"""
        return f"{self.catalog}.{self.schema}.{base_name}{self.bronze_suffix}"

    def silver_table(self, base_name: str) -> str:
        """Build a silver-layer table name"""
        return f"{self.catalog}.{self.schema}.{base_name}{self.silver_suffix}"

    def gold_table(self, base_name: str) -> str:
        """Build a gold-layer table name"""
        return f"{self.catalog}.{self.schema}.{base_name}{self.gold_suffix}"

    def __repr__(self) -> str:
        return f"PipelineConfig(catalog={self.catalog}, schema={self.schema}, env={self.environment})"
