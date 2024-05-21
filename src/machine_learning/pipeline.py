import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import Pipeline

from src.machine_learning.transformers.genre_renamer import GenreRenameTransformer
from src.machine_learning.transformers.missing_values import MissingValuesTransformer
from src.machine_learning.transformers.runtime import RuntimeTransformer


class PipelineTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.steps = [
            ('missing_values', MissingValuesTransformer()),
            ('runtime', RuntimeTransformer()),
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
    print(b["Genre"].head())




