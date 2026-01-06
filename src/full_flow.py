from prefect import flow
from orchestration.data_extraction_subflow import DataExtractor
from orchestration.data_preprocessing_subflow import PreprocessFlow
from orchestration.data_processing_subflow import ProcessFlow


@flow
def full_flow():
    # DataExtractor().flow()
    # PreprocessFlow().flow()
    ProcessFlow().flow()

if __name__ == "__main__":
    full_flow()