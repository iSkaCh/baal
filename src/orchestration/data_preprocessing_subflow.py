from pathlib import Path
from prefect import task, flow
from prefect.assets import materialize
from prefect.futures import wait
from data_utils.sql_db_tools import get_sql_con,transfer_table_from_od_to_eshmun
from data_preprocessing import get_query
from data_preprocessing.compute_article_links_metrics import compute_linking_scores
from data_preprocessing.compute_score import (
    compute_journals_data,
    compute_authors_score,
    compute_final_score,
)
from data_preprocessing.move_textual_data import move_textual_data
CACHE_DIR = Path("E:/tanit_cache")
AUTHOR_SCORES_PATH = CACHE_DIR / "author_scores.parquet"
JOURNALS_DATA_PATH = CACHE_DIR / "journals_data.parquet"
LINKING_METRICS_PATH = CACHE_DIR / "linking_metrics.parquet"


class ProprocessFlow:
    def __init__(self):
        self.dataset = "opendatasets"

    def __execute_query(self, query_file_name):
        print(f"executing '{query_file_name}' query")
        conn = get_sql_con(self.dataset)
        query = get_query(query_file_name)
        conn.execute(query)
        conn.commit()
        conn.close()

    @materialize("postgres://opendatasets/pubmed_metadata")
    def move_pubmed_metadata(self):
        self.__execute_query("move_pubmed_metadata.sql")

    @materialize(
        "postgres://opendatasets/idx_map_table",
        "postgres://opendatasets/idx_map_table_backup",
        asset_deps=["postgres://opendatasets/pubmed_metadata"],
    )
    def create_idx_map_table(self):
        self.__execute_query("generate_idx_map_table.sql")

    @materialize(
        "postgres://opendatasets/opendata_combined",
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def combine_articles_data(self):
        self.__execute_query("combine_articles_data.sql")

    @materialize(
        "postgres://opendatasets/article_type_and_fields",
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_fields_types(self):
        self.__execute_query("move_fields_types.sql")

    @task(
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_pubmed_authors(self):
        self.__execute_query("move_pubmed_authors.sql")

    @task(
        asset_deps=["postgres://opendatasets/idx_map_table"],
    )
    def move_pubmed_journals(self):
        self.__execute_query("move_pubmed_journals.sql")

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
        self.__execute_query("join_article_data_tables.sql")
    @task()
    def move_table_to_eshmun (self,table_name):
        transfer_table_from_od_to_eshmun(table_name)
    @flow(name="Preprocessing Flow")
    def flow(self):
        self.move_pubmed_metadata()
        self.create_idx_map_table()
        futures = [self.combine_articles_data.submit(), self.move_fields_types.submit()]
        wait(futures)
        futures = [
            self.move_pubmed_authors.submit(),
            self.move_pubmed_journals.submit(),
        ]
        wait(futures)
        self.compute_linking_scores()
        self.compute_journals_data()
        self.compute_authors_score()
        self.compute_final_score()
        self.join_article_data_tables()
        futures=self.move_table_to_eshmun.map (['articles_data','openalex_authors'])
        futures.append (self.move_textual_data.submit())
        wait(futures)


if __name__ == "__main__":
    ProprocessFlow().flow()
