"""
Assets module for Baal orchestration.

This module contains all Dagster assets definitions for OpenAlex data extraction pipeline.
"""

from dagster import asset, Config
from pathlib import Path
import subprocess
from typing import Optional

# Import the tanit extractor (will be available after pip install)
try:
    from tanit.data_extraction.openalex_extractor import OpenAlexExtractor, ExtractionConfig
except ImportError:
    # Fallback for development when tanit is not yet installed
    OpenAlexExtractor = None
    ExtractionConfig = None


class OpenAlexConfig(Config):
    """Configuration for OpenAlex pipeline."""
    data_dir: str = "E://openalex-works-snapshot"
    s3_bucket_path: str = "s3://openalex/data/works"
    batch_size: int = 5000
    max_files: Optional[int] = None
    resume_processing: bool = True
    tables_to_extract: Optional[list] = None
    num_processes: int = 10
    force_reextraction: bool = False


@asset
def openalex_raw_data(config: OpenAlexConfig) -> str:
    """
    Download raw OpenAlex data from S3 bucket.
    Syncs the OpenAlex works data to local storage.
    """
    # Ensure the target directory exists
    target_dir = Path(config.data_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Run AWS S3 sync command
    cmd = [
        "aws", "s3", "sync", 
        config.s3_bucket_path, 
        config.data_dir, 
        "--no-sign-request"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"S3 sync failed: {result.stderr}")
    
    return f"Successfully synced OpenAlex data to {config.data_dir}"


@asset
def openalex_extracted_data(openalex_raw_data: str, config: OpenAlexConfig) -> str:
    """
    Extract and process OpenAlex data using the tanit extractor.
    Depends on the raw data being available.
    """
    # Create extraction config
    extraction_config = ExtractionConfig.from_env(
        data_dir=config.data_dir,
        batch_size=config.batch_size,
        max_files=config.max_files,
        resume_processing=config.resume_processing,
        tables_to_extract=config.tables_to_extract,
        num_processes=config.num_processes,
        force_reextraction=config.force_reextraction
    )
    
    # Create and run extractor
    extractor = OpenAlexExtractor(extraction_config)
    extractor.run(max_files=config.max_files, resume=config.resume_processing)
    
    return f"Successfully extracted OpenAlex data from {config.data_dir}"


# Collect all assets for easy import
all_assets = [
    openalex_raw_data,
    openalex_extracted_data,
]
