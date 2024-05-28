import os.path

import pandas as pd
from elasticsearch import Elasticsearch, helpers



DATA_DIR = "/Users/ilan/big-data-airflow-project/data"
file_path = os.path.join(DATA_DIR, 'recomentations.parquet')
es_host = "https://movie-recommendation.kb.eu-west-3.aws.elastic-cloud.com:9243/app/home#/"


def load_data_and_index(**kwargs):
    df = pd.read_parquet(file_path)

    # Connexion à Elasticsearch
    es = Elasticsearch([es_host])

    def generate_actions(df, index_name):
        for i, row in df.iterrows():
            yield {
                "_index": index_name,
                "_source": row.to_dict(),
            }

    helpers.bulk(es, generate_actions(df, 'recommendations'))
    print("Données indexées avec succès !")

if __name__ == '__main__':
    load_data_and_index()
