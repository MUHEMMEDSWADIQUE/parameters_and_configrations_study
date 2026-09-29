# =============================================================================
# logging.py — Structured logging for parameter_lab
# =============================================================================
# Provides a structured logging utility that tags log messages with
# parameter context (environment, catalog, schema, task name).
#
# This demonstrates that logging itself can be parameterized — the log
# output adapts based on the runtime environment.
# =============================================================================

import logging
import sys
from typing import Optional


class ParameterLabLogger:
    """
    A structured logger that includes parameter context in every message.

    Usage:
        logger = ParameterLabLogger(environment="dev", catalog="main", schema="parameter_lab_dev")
        logger.info("Starting pipeline")
        # Output: [INFO] [dev|main.parameter_lab_dev] Starting pipeline
    """

    _instance: Optional["ParameterLabLogger"] = None

    def __init__(
        self,
        environment: str = "unknown",
        catalog: str = "unknown",
        schema: str = "unknown",
        task: str = "unknown",
        level: int = logging.INFO,
    ):
        self.environment = environment
        self.catalog = catalog
        self.schema = schema
        self.task = task
        self._logger = logging.getLogger(f"parameter_lab.{task}")
        self._logger.setLevel(level)

        # Avoid duplicate handlers
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    @property
    def context_tag(self) -> str:
        """Build a context tag for log messages."""
        return f"[{self.environment}|{self.catalog}.{self.schema}|{self.task}]"

    def _format(self, msg: str) -> str:
        return f"{self.context_tag} {msg}"

    def debug(self, msg: str) -> None:
        self._logger.debug(self._format(msg))

    def info(self, msg: str) -> None:
        self._logger.info(self._format(msg))

    def warning(self, msg: str) -> None:
        self._logger.warning(self._format(msg))

    def error(self, msg: str) -> None:
        self._logger.error(self._format(msg))

    def critical(self, msg: str) -> None:
        self._logger.critical(self._format(msg))

    def config_dump(self, config_dict: dict) -> None:
        """Log all configuration values for debugging."""
        self.info("=" * 60)
        self.info("CONFIGURATION DUMP")
        self.info("=" * 60)
        for key, value in sorted(config_dict.items()):
            self.info(f"  {key} = {value}")
        self.info("=" * 60)

    @classmethod
    def from_spark_conf(cls, spark, task: str = "pipeline") -> "ParameterLabLogger":
        """
        Create a logger from Spark configuration values.

        This demonstrates that even logging configuration is parameterized.
        """
        try:
            env = spark.conf.get("parameter_lab.environment")
            catalog = spark.conf.get("parameter_lab.catalog")
            schema = spark.conf.get("parameter_lab.schema")
        except Exception:
            env, catalog, schema = "unknown", "unknown", "unknown"

        return cls(environment=env, catalog=catalog, schema=schema, task=task)

    @classmethod
    def from_widgets(cls, dbutils, task: str = "workflow") -> "ParameterLabLogger":
        """
        Create a logger from notebook widget parameters.
        """
        try:
            env = dbutils.widgets.get("environment")
            catalog = dbutils.widgets.get("catalog")
            schema = dbutils.widgets.get("schema")
        except Exception:
            env, catalog, schema = "unknown", "unknown", "unknown"

        return cls(environment=env, catalog=catalog, schema=schema, task=task)
