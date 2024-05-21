import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator

DATA_DIR = "/Users/ilan/big-data-airflow-project/data"

class RuntimeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column="Runtime"):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X_copy = X.copy()

        X[self.column] = X[self.column].str.replace("min", "")
        X[self.column] = X[self.column].str.replace("h", "")
        X[self.column] = X[self.column].str.split(" ")

        def parse_runtime(runtime):
            hours = int(runtime[0])
            minutes = int(runtime[1]) if len(runtime) > 1 else 0
            total_hours = hours + minutes / 60
            if total_hours > 2:
                return '> 2 hrs'
            elif total_hours < 0.5:
                return '< 30 minutes'
            elif total_hours < 1 and total_hours >= 0.5:
                return '30 - 60 mins'
            else:
                return '1-2 hour'

        X[self.column] = X[self.column].apply(parse_runtime)
        return pd.concat([X_copy, X[self.column]], axis=1)
