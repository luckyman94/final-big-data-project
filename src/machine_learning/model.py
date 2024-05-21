import pandas as pd
from sklearn.metrics.pairwise import linear_kernel


from src.machine_learning.pipeline import PipelineTransformer


class RecommendationModel:
    def __init__(self, df):
        self.pipeline = PipelineTransformer()
        self.train = self.pipeline.fit_transform(df)
        self.tfidf_matrix = self.pipeline.get_tfidf_matrix()
        self.indices = pd.Series(range(len(self.train)), index=self.train['Title']).drop_duplicates()
        self.cosine_sim = linear_kernel(self.tfidf_matrix, self.tfidf_matrix)

    def get_recommendations(self, title):
        idx = self.indices.get(title, None)
        if idx is None:
            print("Title not found in the index.")
            return []

        sim_scores = list(enumerate(self.cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        movie_indices = [i[0] for i in sim_scores[1:11]]

        if any(i >= len(self.train) for i in movie_indices):
            print("Error: Some indices are out of bounds.")
            return []

        return self.train['Title'].iloc[movie_indices]


