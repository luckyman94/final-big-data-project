import pandas as pd
from sklearn.metrics.pairwise import linear_kernel


from src.machine_learning.pipeline import PipelineTransformer
from src.utils.s3_manager import S3Manager


class RecommendationModel:
    def __init__(self, df):
        self.pipeline = PipelineTransformer()
        self.train = self.pipeline.fit_transform(df)
        self.tfidf_matrix = self.pipeline.get_tfidf_matrix()
        self.indices = pd.Series(range(len(self.train)), index=self.train['Title']).drop_duplicates()
        self.cosine_sim = linear_kernel(self.tfidf_matrix, self.tfidf_matrix)
        self.s3_manager = S3Manager()

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

    def generate_recommendations_df(self, titles, export=True):
        recommendations = []
        for title in titles:
            recommended_titles = self.get_recommendations(title)
            for rec in recommended_titles:
                recommendations.append({
                    'input_title': title,
                    'recommended_title': rec
                })

        if export:
            df = pd.DataFrame(recommendations)
            df.to_parquet('/Users/ilan/big-data-airflow-project/data/recomentations.parquet', index=False)
            self.s3_manager.upload_file('recommendations.parquet', "/Users/ilan/big-data-airflow-project/data/recomentations.parquet")


        return pd.DataFrame(recommendations)


if __name__ == '__main__':
    df = pd.read_parquet('/Users/ilan/big-data-airflow-project/data/final_dataset.parquet')
    a = RecommendationModel(df)
    pred = a.generate_recommendations_df(["Mekhong Full Moon Party", "Go Go Squid", "Ananta","Cromartie High School", "Bad Genius", "Ocean Waves"])


