from sklearn.base import TransformerMixin, BaseEstimator
import pandas as pd

class GenreRenameTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.column = "Genre"
        self.map = {
            "Science fiction": "Sci-Fi",
            "Comédie": "Comedy",
            "Drame": "Drama",
            "Comédie dramatique": "Drama",
            "Police": "Crime",
            "Epouvante-horreur": "Horror",
            "Historique": "Documentary",
        }

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")

        for column in X.columns:
            if X[column].dtype == 'object':
                X[column] = X[column].map(self.map).fillna(X[column])
        return X
