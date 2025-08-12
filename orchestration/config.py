"""
Development configuration for Dagster.

This module contains development-specific configurations for OpenAlex pipeline.
"""

from dagster import EnvVar

# OpenAlex pipeline configuration
OPENALEX_CONFIG = {
    "data_dir": EnvVar("OPENALEX_DATA_DIR").get_value("E://openalex-works-snapshot"),
    "s3_bucket_path": EnvVar("OPENALEX_S3_BUCKET").get_value("s3://openalex/data/works"),
    "batch_size": int(EnvVar("OPENALEX_BATCH_SIZE").get_value("5000")),
    "max_files": EnvVar("OPENALEX_MAX_FILES").get_value(None),
    "resume_processing": EnvVar("OPENALEX_RESUME").get_value("true").lower() == "true",
    "num_processes": int(EnvVar("OPENALEX_NUM_PROCESSES").get_value("10")),
    "force_reextraction": EnvVar("OPENALEX_FORCE_REEXTRACTION").get_value("false").lower() == "true",
}
