import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from urllib3.exceptions import InsecureRequestWarning
import warnings

warnings.filterwarnings('ignore', category=InsecureRequestWarning)



df = pd.read_parquet('/Users/ilan/big-data-airflow-project/data/recomentations.parquet')


es = Elasticsearch(
    ['https://localhost:9200'],
    basic_auth=('elastic', '3X8=I9lGR+0HteADlaik'),
    verify_certs=False
)





index_name = 'movie_recommendations'


mapping = {
    "mappings": {
        "properties": {
            "input_title": {"type": "text"},
            "recommended_title": {"type": "text"}
        }
    }
}

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

es.indices.create(index=index_name, body=mapping)

def generate_actions(df):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": {
                "input_title": row['input_title'],
                "recommended_title": row['recommended_title']
            }
        }

# Indexation en bulk

if __name__ == '__main__':

    actions = generate_actions(df)
    bulk(es, actions)

    print("Indexation terminée avec succès !")
