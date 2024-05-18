import os

from pyspark.shell import spark

from src.utils.s3_manager import S3Manager

data_dir = "/Users/ilan/big-data-airflow-project/data"


class Preprocessing:
    def __init__(self):
        self.s3_manager = S3Manager()
        self.download_data()
        self.df_allocine = spark.read.csv(data_dir + "/allocine_movies.csv", header=True)
        self.df_netflix = spark.read.csv(data_dir + "/NetflixDataset.csv", header=True)

    def download_data(self):
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        parquet_files = self.s3_manager.list_parquet_files_in_bucket()
        for file in parquet_files:
            if not os.path.exists(data_dir + "/" + file):
                self.s3_manager.download_file(file, data_dir + "/" + file)

    def preprocess_netflix(self):
        return self.df_netflix.drop("Tags",
                                    "Languages",
                                    "Country Availability",
                                    "Director", "Writer", "View Rating", "Awards Received", "Awards Nominated",
                                    "Boxoffice", "Netflix Release Date", "Production House", "Netflix Link", "Summary",
                                    "IMDb Votes", "Image", "Awards Nominated For")
