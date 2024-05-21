import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator

DATA_DIR = "/Users/ilan/big-data-airflow-project/data"

class GenreTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column="Genre"):
        self.genres = [
            "Action",
            "Adventure",
            "Animation",
            "Biopic",
            "Comedy",
            "Crime",
            "Documentary",
            "Drama",
            "Horror",
            "Famille",
            "Guerre",
            "Musical"
            "Romance",
            "Sci-Fi",
            "Thriller",
            "Western"
        ]

        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()
        X[self.column] = X[self.column].str.split(", ")
        for genre in self.genres:
            X[genre] = X[self.column].apply(lambda x: genre in x).astype("int")


        return pd.concat([X_copy, X[self.column]], axis=1)



if __name__ == '__main__':
    a = pd.read_parquet(DATA_DIR + "/allocine_movies.parquet")
    a.dropna(subset=["Duration"], inplace=True)

    print(GenreTransformer().fit_transform(a))



