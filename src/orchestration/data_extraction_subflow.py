import os
import s3fs
import logging
import gzip
import json
from pathlib import Path
from prefect import task, flow
from prefect.futures import wait
from data_extraction.pubmed_updater import PubMedDatabaseUpdater
from data_extraction.config import ExtractionConfig
from data_extraction.openalex_extractor import OpenAlexExtractor

# --- Constants ---
# S3 bucket for the OpenAlex snapshot
OPENALEX_S3_BUCKET = "s3://openalex/data/works"
# Local directory to store the downloaded snapshot
OPENALEX_LOCAL_FOLDER = "E://openalex-works-snapshot"

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# NOTE: openalex S3 bucket is public and doesn't require sign in
class DataExtractor:
    def __init__(self):
        """Initializes the DataExtractor and the S3 file system."""
        self.s3 = s3fs.S3FileSystem(anon=True)  # `anon=True` for public S3 buckets
        self.logger = logging.getLogger(__name__)

    @task
    def list_new_openalex_files(self) -> list[str]:
        self.logger.info("Checking for new OpenAlex files...")

        s3_files = self.s3.glob(f"{OPENALEX_S3_BUCKET}/*/*/*.gz")

        local_files_paths = Path(OPENALEX_LOCAL_FOLDER).rglob("*.gz")
        local_filenames = {p.name for p in local_files_paths}

        new_files_to_download = [
            s3_path
            for s3_path in s3_files
            if os.path.basename(s3_path) not in local_filenames
        ]

        if new_files_to_download:
            self.logger.info(
                f"Found {len(new_files_to_download)} new files to download."
            )
        else:
            self.logger.info("No new files found. Local data is up-to-date.")

        return new_files_to_download

    @task
    def download_openalex_file(self, s3_file_path: str):
        """
        Downloads a single file from S3 to the corresponding local directory.
        """
        local_file_path = s3_file_path.replace("openalex", OPENALEX_LOCAL_FOLDER)

        local_dir = os.path.dirname(local_file_path)
        os.makedirs(local_dir, exist_ok=True)

        self.logger.info(f"Downloading {s3_file_path} to {local_file_path}...")
        try:
            self.s3.get(s3_file_path, local_file_path)
            self.logger.info(
                f"Successfully downloaded {os.path.basename(s3_file_path)}."
            )
        except Exception as e:
            self.logger.error(f"Failed to download {s3_file_path}: {e}")

    @task
    def run_openalex_extractor(self):
        self.logger.info("Starting openalex extraction...")
        config = ExtractionConfig.from_env(
            data_dir="E:\\openalex-works-snapshot",  # Update path as needed
            batch_size=5000,
            max_files=None,
            resume_processing=True,
            tables_to_extract=None,
            num_processes=4,
            force_reextraction=False,
        )
        extractor = OpenAlexExtractor(config)
        extractor.run(max_files=None, resume=True)

    @task
    def run_pubmed_update(self):
        """
        A task to trigger the PubMed database update process.
        """
        self.logger.info("Starting PubMed update...")
        PubMedDatabaseUpdater().run_pubmed_update()
        self.logger.info("PubMed update finished.")

    @flow
    def flow(self):

        new_files_list = self.list_new_openalex_files()

        download_futures = self.download_openalex_file.map(new_files_list)
        pubmed_future = self.run_pubmed_update.submit()

        all_futures = download_futures + [pubmed_future]

        wait(all_futures)
        if len(new_files_list)>0:
            self.run_openalex_extractor()


if __name__ == "__main__":
    DataExtractor().flow()
