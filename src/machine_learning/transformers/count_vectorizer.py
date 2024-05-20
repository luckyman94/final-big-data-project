from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import CountVectorizer


class CountVectorizerTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, max_features=None, ngram_range=(1, 1)):
        self.max_features = max_features
        self.ngram_range = ngram_range
        self.vectorizer = CountVectorizer(max_features=self.max_features, ngram_range=self.ngram_range)

    def fit(self, X, y=None):
        self.vectorizer.fit(X)
        return self

    def transform(self, X):
        return self.vectorizer.transform(X)

    def fit_transform(self, X, y=None):
        return self.vectorizer.fit_transform(X)

    def get_feature_names(self):
        return self.vectorizer.get_feature_names_out()