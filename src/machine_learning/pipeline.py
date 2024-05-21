import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import Pipeline

from src.machine_learning.transformers.genre_renamer import GenreRenameTransformer
from src.machine_learning.transformers.missing_values import MissingValuesTransformer
from src.machine_learning.transformers.ohe import OneHotEncoderTransformer
from src.machine_learning.transformers.runtime import RuntimeTransformer
from src.machine_learning.transformers.tfidf import TFIDFTransformer


class PipelineTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.steps = [
            ('runtime', RuntimeTransformer()),
            ('missing_values', MissingValuesTransformer()),
            ('genre_renamer', GenreRenameTransformer()),
        ]

        self.pipeline = Pipeline(self.steps)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return self.pipeline.fit_transform(X)






if __name__ == "__main__":
    a = pd.read_parquet("/Users/ilan/big-data-airflow-project/data/final_dataset.parquet")
    pp = PipelineTransformer()
    b = pp.fit_transform(a)
    print(b)




