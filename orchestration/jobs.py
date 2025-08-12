"""
Jobs module for Baal orchestration.

This module contains all Dagster job definitions for OpenAlex data extraction pipeline.
"""

from dagster import define_asset_job, AssetSelection

from orchestration.assets import all_assets, openalex_raw_data, openalex_extracted_data

# Job to sync raw OpenAlex data only
sync_openalex_data_job = define_asset_job(
    name="sync_openalex_data",
    selection=AssetSelection.assets(openalex_raw_data),
    description="Job that syncs raw OpenAlex data from S3"
)

# Job to run extraction only (assumes raw data is already available)
extract_openalex_data_job = define_asset_job(
    name="extract_openalex_data", 
    selection=AssetSelection.assets(openalex_extracted_data),
    description="Job that extracts and processes OpenAlex data"
)

# Job that runs the complete pipeline (sync + extract)
complete_openalex_pipeline_job = define_asset_job(
    name="complete_openalex_pipeline",
    selection=AssetSelection.assets(*all_assets),
    description="Job that runs the complete OpenAlex data pipeline (sync + extract)"
)

# Collect all jobs for easy import
all_jobs = [
    sync_openalex_data_job,
    extract_openalex_data_job,
    complete_openalex_pipeline_job,
]
