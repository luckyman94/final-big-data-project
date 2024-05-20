import pandas as pd
from pyspark.shell import spark
import os

from src.machine_learning.transformers.count_vectorizer import CountVectorizerTransformer

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
from src.machine_learning.transformers.ohe import OneHotEncoderTransformer
from src.machine_learning.transformers.tfidf import TFIDFTransformer

DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"
class Preprocessing:
    def __init__(self):
        self.df = (spark.read.parquet(DATA_DIR + "/final_dataset.parquet", header=True, inferSchema=True)).toPandas()
        self.tfidf = TFIDFTransformer(column="Summary", max_features=1000, n_components=5)
        self.ohe = OneHotEncoderTransformer(columns=["Runtime"])
        self.cv = CountVectorizerTransformer(max_features=1000, ngram_range=(1, 1))

    def apply_preprocessing(self):
        self.df = self.tfidf.fit_transform(self.df)
        self.df = self.ohe.fit_transform(self.df)

        df_encoded = self.cv.fit_transform(self.df['Actors'])
        df_encoded = pd.DataFrame(df_encoded.toarray(), columns=self.cv.get_feature_names(), index=self.df.index)
        self.df = pd.concat([self.df.drop('Actors', axis=1), df_encoded], axis=1)

        self.tfidf_matrix = self.tfidf.get_tfidf_matrix()

        return self.df


    def get_tfidf_matrix(self):
        return self.tfidf_matrix





