import os

from pyspark.shell import spark
from pyspark.sql.functions import col, round as spark_round, mean
from pyspark.sql.types import FloatType

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
        self.df_netflix = self.df_netflix.drop("Tags",
                                               "Languages",
                                               "Country Availability",
                                               "Director", "Writer", "View Rating", "Awards Received",
                                               "Awards Nominated", "Boxoffice", "Netflix Release Date",
                                               "Production House", "Netflix Link", "IMDb Votes", "Image",
                                               "Awards Nominated For", "Release Date")

        # Missing values for the runtime column
        self.df_netflix = self.df_netflix.fillna({'runtime': '1-2 hour'})

        # Normalize the IMDb Score column to a 0-5 scale
        self.df_netflix = self.df_netflix.withColumn("IMDb Score", col("IMDb Score").cast(FloatType()))
        self.df_netflix = self.df_netflix.withColumn("IMDb Score", spark_round(col("IMDb Score") / 2, 1))

        # Missing values for the IMDb Score column are replaced by the mean of the column
        mean_value = self.df_netflix.select(mean(col("IMDb Score")).alias("mean")).collect()[0]["mean"]
        self.df_netflix =  self.df_netflix.na.fill({"IMDb Score": mean_value})

        # Rename Series or Movie column to Type and IMDb Score to Rating
        self.df_netflix = self.df_netflix.withColumnRenamed("Series or Movie", "Type")
        self.df_netflix = self.df_netflix.withColumnRenamed("IMDb Score", "Rating")

    def preprocess_allocine(self):
        return