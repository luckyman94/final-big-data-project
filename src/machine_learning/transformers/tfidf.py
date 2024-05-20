import pandas as pd
from pyspark.shell import spark
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"
class TFIDFPCATransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column, max_features, n_components):
        self.column = column
        self.max_features = max_features
        self.n_components = n_components
        self.vectorizer = TfidfVectorizer(max_features=max_features)
        self.svd = TruncatedSVD(n_components=n_components)

    def fit(self, X, y=None):
        vectors = self.vectorizer.fit_transform(X[self.column])
        self.svd.fit(vectors)
        return self

    def transform(self, X):
        vectors = self.vectorizer.transform(X[self.column])
        svd_result = self.svd.transform(vectors)
        tfidf_df = pd.DataFrame(svd_result, columns=[f"{self.column}_tfidf_{i}" for i in range(self.n_components)])
        X = pd.concat([X, tfidf_df], axis="columns")
        return X


if __name__ == '__main__':
    df = (spark.read.parquet(DATA_DIR + "/final_dataset.parquet", header=True, inferSchema=True)).toPandas()
    print(df["Summary"].isna().sum())
    #TFIDFPCATransformer(column="Summary", max_features=1000, n_components=5).fit_transform(df)