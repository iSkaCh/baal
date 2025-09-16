from prefect import flow
from orchestration.data_extraction_subflow import DataExtractor
from orchestration.data_preprocessing_subflow import ProprocessFlow


@flow
def full_flow():
    DataExtractor().flow()
    ProprocessFlow().flow()

if __name__ == "__main__":
    full_flow()