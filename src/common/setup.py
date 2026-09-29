# =============================================================================
# setup.py — Wheel packaging for parameter_lab_common
# =============================================================================
# This file enables the `artifacts` section in databricks.yml to build
# a Python wheel containing the common utilities.
#
# The wheel is installed on pipeline and job clusters, allowing:
#   from parameter_lab_common.config import PipelineConfig
#   from parameter_lab_common.validation import validate_required
#   from parameter_lab_common.logging import ParameterLabLogger
#   from parameter_lab_common.parameter_utils import build_fqtn
#
# Build:  python setup.py bdist_wheel
# =============================================================================

from setuptools import setup, find_packages

setup(
    name="parameter_lab_common",
    version="1.0.0",
    description="Common utilities for the Parameter Lab DAB project",
    author="Parameter Lab",
    packages=find_packages(),
    py_modules=[
        "config",
        "parameter_utils",
        "validation",
        "logging",
    ],
    install_requires=[],
    python_requires=">=3.8",
)
