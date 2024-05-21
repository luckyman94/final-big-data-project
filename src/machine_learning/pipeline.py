from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import Pipeline

from src.machine_learning.transformers.ohe import OneHotEncoderTransformer
from src.machine_learning.transformers.tfidf import TFIDFTransformer


class PipelineTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.steps = [
            ('tfidf', TFIDFTransformer(column='Summary', max_features=5000, n_components=5)),
            ('ohe', OneHotEncoderTransformer(columns=['Runtime']))
        ]

        self.pipeline = Pipeline(self.steps)

    def fit(self, X, y=None):
        self.pipeline.fit(X)
        return self

    def transform(self, X):
        return self.pipeline.transform(X)



