"""
Baal Orchestration - Dagster Definitions

This module contains the main Dagster definitions for the orchestration repository.
"""

from dagster import Definitions

from orchestration.assets import all_assets
from orchestration.jobs import all_jobs


defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
)
