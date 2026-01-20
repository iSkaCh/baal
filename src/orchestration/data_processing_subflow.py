from pathlib import Path
from prefect import get_run_logger, task, flow
from prefect.assets import materialize
from prefect.futures import wait
from sqlalchemy import text
import logging
import subprocess

from data_processing.tables_queries import ENT_TABLE_CREATION_QUERIES
from eshmun_config.eshmun_db_configs import (
    DB_NAME,
    EMBED_SERVER_IP,
    TABLE_NAMES,
    ES_INDEX_NAME,
)
from data_utils.sql_db_tools import get_sql_con
from data_processing.entity_extraction import run_entity_extraction
from data_processing.move_entities import move_all_entities
from eshmun_models.rel_extraction import generic_rel_extract_main
from eshmun_models.vllm_rel_extraction import VLLMRelExtractor
from data_processing.create_entities_data_table import (
    get_create_ent_data_table_sql,
    get_insert_umls_concepts_sql,
    get_update_postprocessed_entity_sql,
    compute_concepts_multithreaded,
)
from data_processing.create_elastic_search_index import (
    create_index,
    truncate_index,
    create_and_fill_index,
    get_es_client,
    get_index_info
)


class ProcessFlow:
    def __init__(self):
        self.dataset = DB_NAME
        self.server_ip = EMBED_SERVER_IP
        self.logger = get_run_logger()

    def execute_query(self, query):
        if isinstance(query,str):
            query = text (query)
        with get_sql_con(self.dataset, self.server_ip) as conn:
            conn.execute(query)
            conn.commit()

    def delete_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name};"
        self.execute_query(query)
    @task
    def generate_tables(self, tables_list):
        for table in tables_list:
            table_name = TABLE_NAMES[table]
            self.delete_table(table_name)
            query = text(ENT_TABLE_CREATION_QUERIES[table])
            self.execute_query(query)
    @task
    def run_entity_extractions(self, text_columns=["title", "tldr", "abstract"]):
        for text_column in text_columns:
            self.logger.info (f'Extracting entities from {text_column}')
            run_entity_extraction(text_column)
    @task
    def move_all_entities(self):
        move_all_entities()
    @task
    def rel_extraction(self):
        self.logger.info("Starting docker container trt_qwen_small")
        subprocess.run(["docker", "start", "trt_qwen_small"], check=True)
        try:
            generic_rel_extract_main(VLLMRelExtractor, num_bmids_to_fetch=30_000_000)
        finally:
            self.logger.info("Stopping docker container trt_qwen_small")
            subprocess.run(["docker", "stop", "trt_qwen_small"], check=True)
    @task
    def create_ent_data_table(self,resume=True):
        if not resume: 
            sql_creation, sql_indexes  = get_create_ent_data_table_sql(TABLE_NAMES["entities_data_table"])
            self.logger.info("Executing create ent_data_table query")
            self.delete_table (TABLE_NAMES["entities_data_table"])
            self.execute_query(sql_creation)
            self.logger.info("Executing insert_umls query")
            insert_umls_query = get_insert_umls_concepts_sql(
                target_table=TABLE_NAMES["entities_data_table"],
                source_table=TABLE_NAMES["rectified_umls_data"],
                ent_counts_table=TABLE_NAMES["entities_with_occ"],
            )
            self.execute_query(insert_umls_query)
            self.logger.info("Executing pp_ent_update query")
            pp_ent_update_query = get_update_postprocessed_entity_sql(
                target_table=TABLE_NAMES["entities_data_table"],
                source_table="entities_data_3",
            )
            self.execute_query(pp_ent_update_query)
        self.logger.info("Executing compute_concepts_multithreaded")
        with get_sql_con(self.dataset, self.server_ip) as conn:
            compute_concepts_multithreaded(
                table_name=TABLE_NAMES["entities_data_table"],
                ent_counts_table=TABLE_NAMES["entities_with_occ"],
                conn=conn,
                batch_size=10_000,
                num_threads=2,
                min_occurrence=2,
            )
        if not resume:
            self.execute_query(sql_indexes)
    @task
    def create_es_index(self):
        es_client = get_es_client()
        create_index(es_client=es_client, index_name=ES_INDEX_NAME)
        truncate_index(es_client=es_client, index_name=ES_INDEX_NAME)
        create_and_fill_index(
            es_client=es_client,
            index_name=ES_INDEX_NAME,
            ents_data_table=TABLE_NAMES["entities_data_table"],
            ent_counts_table=TABLE_NAMES["entities_with_occ"],
            articles_entities_table= TABLE_NAMES ['articles_entities'], 
            articles_data_table = TABLE_NAMES ['articles_data'], 
            preprocessed_text_table = TABLE_NAMES ['preprocessed_text'], 
            grouped_dup_bmids_table =TABLE_NAMES ["grouped_dup_bmids"] , 
            process_missing_only=False,
            batch_size=20_000,
            max_workers=3,
        )
        index_info = get_index_info (es_client=es_client, index_name=ES_INDEX_NAME)
        self.logger.info (index_info)
    @flow
    def flow(self):
        # self.generate_tables(["grouped_dup_bmids"])
        # self.run_entity_extractions()
        # self.move_all_entities()
        # self.rel_extraction()
        self.create_ent_data_table()
        self.create_es_index()
        ents_tables_list = [
            "ent_secondary_concept_map",
            "secondary_concept_occ",
            "concepts_occ",
        ]
        self.generate_tables(ents_tables_list)
        # rel_agg_tables_list = ["agg_rels_table", "rel_simplified_prefix"]
        # self.generate_tables(rel_agg_tables_list)
        # self.delete_table(TABLE_NAMES["agg_rels_table"])
if __name__ == '__main__':
    ProcessFlow().flow()