import os
import s3fs
import logging
import gzip
import json
import requests
from pathlib import Path
from prefect import task, flow
from prefect.futures import wait
from data_extraction.pubmed_updater import PubMedDatabaseUpdater
from data_extraction.config import ExtractionConfig
from data_extraction.openalex_extractor import (
    OpenAlexExtractor)
from data_extraction.semantic_scholar_extraction_from_datasets import download_s2_files
from data_extraction.semantic_scholar_extractor import (
    extract_papers,
    extract_other_data,
)
from data_utils.s3_utils import download_s3_file
from config.credentials import S2_API_KEY
from tqdm import tqdm
from time import sleep 
#from orchestration.utils import execute_query

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
OPENALEX_LOCAL_FOLDER = "E://raw_data/openalex/"
OPENALEX_S3_BUCKET = "openalex"
OPENALEX_WORKS_S3_PREFIX = "data/works"
S2_LOCAL_FOLDER = r"E:\\raw_data\semantic_scholar"


# NOTE: openalex S3 bucket is public and doesn't require sign in
class DataExtractor:
    def __init__(self):
        self.s3fs_client = s3fs.S3FileSystem(
            anon=True
        )  # `anon=True` for public S3 buckets
        self.dataset = 'opendatasets'
        self.logger = logging.getLogger(__name__)

    @task
    def download_openalex_file(
        self, s3_file_path: str, local_folder=OPENALEX_LOCAL_FOLDER
    ):
        download_s3_file(
            s3_file_path=s3_file_path,
            local_file_path=Path(local_folder) / s3_file_path,
            s3fs_client=self.s3fs_client,
            s3_bucket=OPENALEX_S3_BUCKET,
            s3_prefix=OPENALEX_WORKS_S3_PREFIX,
        )

    @task
    def run_openalex_extractor(self):
        self.logger.info("Starting openalex extraction...")
        config = ExtractionConfig.from_env(
            data_dir=OPENALEX_LOCAL_FOLDER,
            batch_size=5000,
            max_files=None,
            resume_processing=False,
            tables_to_extract=None,
            num_processes=2,
            force_reextraction=True,
        )
        extractor = OpenAlexExtractor(config)
        extractor.run(max_files=None, resume=True)

    @task
    def run_pubmed_update(self):
        self.logger.info("Starting PubMed update...")
        PubMedDatabaseUpdater().run_pubmed_update()
        self.logger.info("PubMed update finished.")

    @task
    def download_s2_data(self):
        self.logger.info("Starting s2 download...")
        # pub_files = requests.get(
        #     "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/publication-venues",
        #     headers={"x-api-key": S2_API_KEY},
        # ).json()["files"]
        tldr_files = requests.get(
            "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/tldrs",
            headers={"x-api-key": S2_API_KEY},
        ).json()["files"]
        sleep(5)
        # ids_files = requests.get(
        #     "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/paper-ids",
        #     headers={"x-api-key": S2_API_KEY},
        # ).json()["files"]
        papers_files = requests.get(
            "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/papers",
            headers={"x-api-key": S2_API_KEY},
        ).json()["files"]
        sleep(5)
        s2orc_v2_files = requests.get(
            "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/s2orc_v2",
            headers={"x-api-key": S2_API_KEY},
        ).json()["files"]
        sleep(5)
        download_s2_files(
         tldr_files  + papers_files + s2orc_v2_files
        )

    @task
    def run_s2_extractor(self):

        self.logger.info("Starting s2 extraction...")
        self.logger.info("Papers extraction...")
        extract_papers(data_dir=S2_LOCAL_FOLDER)
        self.logger.info("TLDR extraction...")
        extract_other_data(
            data_dir=S2_LOCAL_FOLDER, tables_to_extract=["semanticscholar_tldr"]
        )
    
    @task()
    def move_pubmed_metadata(self):
        execute_query("move_pubmed_metadata.sql",self.dataset)
    @flow
    def flow(self):

        self.run_openalex_extractor()
        self.run_pubmed_update()
        self.move_pubmed_metadata()
        self.download_s2_data()
        self.run_s2_extractor()


if __name__ == "__main__":
    DataExtractor().flow()
