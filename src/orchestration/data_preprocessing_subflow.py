from pathlib import Path
from prefect import task, flow
from prefect.assets import materialize
from prefect.futures import wait
from data_utils.sql_db_tools import get_sql_con,transfer_table_from_od_to_eshmun
from data_preprocessing.compute_article_links_metrics import compute_linking_scores
from data_preprocessing.compute_score import (
    compute_journals_data,
    compute_authors_score,
    compute_final_score,
)
from data_preprocessing.move_textual_data import move_textual_data
from data_preprocessing import get_query

CACHE_DIR = Path("E:/tanit_cache")
AUTHOR_SCORES_PATH = (CACHE_DIR / "author_scores.parquet").as_uri()
JOURNALS_DATA_PATH = (CACHE_DIR / "journals_data.parquet").as_uri()
LINKING_METRICS_PATH = (CACHE_DIR / "linking_metrics.parquet").as_uri()


class PreprocessFlow:
    def __init__(self):
        self.dataset = "opendatasets"
        self.server_ip = 'localhost:5432'
    def execute_query (self, query): 
        with get_sql_con (self.dataset,self.server_ip)as conn: 
            conn.execute(query)
            conn.commit()
    @materialize(
        "postgres://opendatasets/idx_map_table",
        "postgres://opendatasets/idx_map_table_backup",
        asset_deps=["postgres://opendatasets/pubmed_metadata"],
    )
    def create_idx_map_table(self):
        
        query = get_query("generate_idx_map_table.sql")
        self.execute_query(query) 

    @materialize(
        "postgres://opendatasets/opendata_combined",
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def combine_articles_data(self):
        query = get_query("combine_articles_data.sql")
        self.execute_query(query) 

    @materialize(
        "postgres://opendatasets/article_type_and_fields",
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_fields_types(self):
        query = get_query("move_fields_types.sql")
        self.execute_query(query) 

    @task(
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_pubmed_authors(self):
        query = get_query("move_pubmed_authors.sql")
        self.execute_query(query) 

    @task(
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_pubmed_journals(self):
        query = get_query("move_pubmed_journals.sql")
        self.execute_query(query) 
    @materialize(
        LINKING_METRICS_PATH,
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def compute_linking_scores(self):
        compute_linking_scores()
    @materialize(
        JOURNALS_DATA_PATH,
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def compute_journals_data(self):
        compute_journals_data(JOURNALS_DATA_PATH)

    @materialize(
        AUTHOR_SCORES_PATH,
        asset_deps=["postgres://opendatasets/idx_map_table", LINKING_METRICS_PATH],
    )
    def compute_authors_score(self):
        compute_authors_score(AUTHOR_SCORES_PATH)

    @materialize(
        "postgres://opendatasets/articles_scores",
        asset_deps=[
            "postgres://opendatasets/opendata_combined",
            LINKING_METRICS_PATH,
            AUTHOR_SCORES_PATH,
            JOURNALS_DATA_PATH,
        ],
    )
    def compute_final_score(self):
        compute_final_score(
            author_scores_path=AUTHOR_SCORES_PATH,
            journals_score_path=JOURNALS_DATA_PATH,
            linking_metrics_path=LINKING_METRICS_PATH,
        )
    @materialize(
        "postgres://eshmun/preprocessed_text",
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_textual_data (self):
        move_textual_data ()
    @materialize(
        "postgres://opendatasets/articles_data",
        asset_deps=["postgres://opendatasets/opendata_combined",
                    LINKING_METRICS_PATH,
                    "postgres://opendatasets/article_type_and_fields"],
    )
    @task()
    def join_article_data_tables (self):
        query = get_query("join_article_data_tables.sql")
        self.execute_query(query) 
    @task()
    def move_table_to_eshmun (self,table_name):
        transfer_table_from_od_to_eshmun(table_name)
    @flow(name="Preprocessing Flow")
    def flow(self):
        self.create_idx_map_table()
        self.combine_articles_data()
        self.move_fields_types()# 3h30
        self.move_pubmed_authors()
        self.move_pubmed_journals()
        self.compute_linking_scores()
        self.compute_journals_data()
        self.compute_authors_score()
        self.compute_final_score()
        self.join_article_data_tables()
        self.move_table_to_eshmun('articles_data')
        self.move_textual_data.submit()

        self.move_table_to_eshmun('openalex_authors')


if __name__ == "__main__":
    PreprocessFlow().flow()
