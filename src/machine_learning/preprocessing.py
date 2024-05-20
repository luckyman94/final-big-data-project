import os

import pandas as pd
from pyspark.shell import spark
from sklearn.feature_extraction.text import TfidfVectorizer
nltk_data_dir = 'nltk_data'
import nltk

if not os.path.exists(nltk_data_dir):
    os.makedirs(nltk_data_dir)
nltk.data.path.append(nltk_data_dir)
nltk.download('stopwords', download_dir=nltk_data_dir)

DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"
class Preprocessing:
    def __init__(self):
        self.df = (spark.read.parquet(DATA_DIR + "/final_dataset.parquet", header=True, inferSchema=True)).to_pandas_on_spark()

    def apply_tfidf(self):
        tfidf = TfidfVectorizer(stop_words='english')
        self.df['Summary'] = self.df['Summary'].fillna('')
        self.df['Summary'] = tfidf.fit_transform(self.df['Summary'])
        return self.df


if __name__ == "__main__":
    pp = Preprocessing()
    df = pp.apply_tfidf()
    print(df)