from sklearn.base import BaseEstimator, TransformerMixin


class DropColumn(BaseEstimator, TransformerMixin):
    def __init__(self, cols=[]):
        self.cols = cols

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X.drop(self.cols, axis=1)


