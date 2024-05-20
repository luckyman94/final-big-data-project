import pandas as pd
from sklearn.metrics.pairwise import linear_kernel

from src.machine_learning.preprocessing import Preprocessing

class RecommendationModel:
    def __init__(self, df, tfidf_matrix):
        self.df = df
        self.tfidf_matrix = tfidf_matrix
        self.cosine_sim = linear_kernel(self.tfidf_matrix, self.tfidf_matrix)
        self.indices = pd.Series(self.df.index, index=self.df['Title']).drop_duplicates()

    def get_recommendations(self, title):
        print("Titles available in DataFrame:", self.df['Title'].unique())
        idx = self.indices.get(title, None)
        if idx is None:
            print("Title not found in the index.")
            return []
        sim_scores = list(enumerate(self.cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        movie_indices = [i[0] for i in sim_scores[1:11]]
        return self.df['Title'].iloc[movie_indices]

if __name__ == '__main__':
    preprocessor = Preprocessing()
    df_processed = preprocessor.apply_preprocessing()
    tfidf_matrix = preprocessor.get_tfidf_matrix()
    recommender = RecommendationModel(df_processed, tfidf_matrix)
    recommendations = recommender.get_recommendations("Maverick")
    print("Recommendations for 'Maverick':", recommendations)
