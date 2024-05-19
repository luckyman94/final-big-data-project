import os

from pyspark.shell import spark
from pyspark.sql.functions import col, round as spark_round, mean, split, array_contains, udf, when, regexp_replace
from pyspark.sql.types import FloatType, StringType

import config
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

    def preprocess_netflix(self, export_parquet=False):
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
        self.df_netflix = self.df_netflix.na.fill({"IMDb Score": mean_value})

        # Rename Series or Movie column to Type and IMDb Score to Rating
        self.df_netflix = self.df_netflix.withColumnRenamed("Series or Movie", "Type")
        self.df_netflix = self.df_netflix.withColumnRenamed("IMDb Score", "Rating")

        # One Hot Encoding of the genre.
        self.df_netflix = self.df_netflix.withColumn("Genre", split(col("Genre"), ",\s*"))
        for genre in config.GENRES:
            self.df_netflix = self.df_netflix.withColumn(genre, array_contains(col("Genre"), genre).cast("integer"))

        if export_parquet:
            self.df_netflix.write.parquet(data_dir + "/NetflixDataset_preprocessed.parquet")

    def preprocess_allocine(self):
        self.df_allocine = self.df_allocine.drop("Director", "Release Date")

        # Renaming columns to match the Netflix dataset
        self.df_allocine = self.df_allocine.withColumnRenamed("Duration", "Runtime")
        self.df_allocine = self.df_allocine.withColumnRenamed("Synopsis", "Summary")

        # Convert runtime to interval (categorical variable)
        convert_runtime_udf = udf(self._convert_runtime_to_interval, StringType())
        self.df_allocine = self.df_allocine.withColumn("Runtime", convert_runtime_udf(self.df_allocine["Runtime"]))

        # Merge the spectator rating and the press rating into a single rating column
        self.df_allocine = self._merge_ratings()

        # One Hot Encoding of the genre.
        self.df_allocine = self.df_allocine.withColumn("Genre", split(col("Genre"), ", "))
        for genre in config.GENRES:
            self.df_allocine = self.df_allocine.withColumn(genre, array_contains(col("Genre"), genre).cast("integer"))

    def _convert_runtime_to_interval(self, runtime):
        hours, minutes = map(int, runtime.replace('min', '').replace('h', '').split())
        total_hours = hours + minutes / 60
        if total_hours > 2:
            return '> 2 hrs'
        elif total_hours < 0.5:
            return '< 30 minutes'
        elif total_hours < 1 and total_hours >= 0.5:
            return '30 - 60 mins'
        else:
            return '1-2 hour'

    def _merge_ratings(self):
        self.df_allocine = self.df_allocine.withColumn("Press Rating", regexp_replace(col("Press Rating"), ",", "."))
        self.df_allocine = self.df_allocine.withColumn("Press Rating",
                                                       when(col("Press Rating") == "--", None).otherwise(
                                                           col("Press Rating")))
        self.df_allocine = self.df_allocine.withColumn("Press Rating", col("Press Rating").cast(FloatType()))

        self.df_allocine = self.df_allocine.withColumn("Spectator Rating",
                                                       regexp_replace(col("Spectator Rating"), ",", "."))
        self.df_allocine = self.df_allocine.withColumn("Spectator Rating",
                                                       when(col("Spectator Rating") == "--", None).otherwise(
                                                           col("Spectator Rating")))
        self.df_allocine = self.df_allocine.withColumn("Spectator Rating", col("Spectator Rating").cast(FloatType()))

        mean_press = self.df_allocine.select(mean(col("Press Rating")).alias("mean_press")).collect()[0]["mean_press"]
        self.df_allocine = self.df_allocine.na.fill({"Press Rating": mean_press})

        mean_spectator = self.df_allocine.select(mean(col("Spectator Rating")).alias("mean_spectator")).collect()[0][
            "mean_spectator"]
        self.df_allocine = self.df_allocine.na.fill({"Spectator Rating": mean_spectator})

        self.df_allocine = self.df_allocine.withColumn("Rating",
                                                       spark_round((col("Press Rating") + col("Spectator Rating")) / 2,
                                                                   1))
        self.df_allocine = self.df_allocine.drop("Press Rating", "Spectator Rating")

        return self.df_allocine

    def _transform_value_of_a_df(self, df, column_name, old_value, new_value):
        return df.withColumn(column_name, regexp_replace(column_name, old_value, new_value))

    def _transform_genre_to_match_netflix_genre(self):
        for genre in config.ALLOCINE_GENRE_MAPPING:
            self.df_allocine = self._transform_value_of_a_df(self.df_allocine, "Genre", genre,
                                                             config.ALLOCINE_GENRE_MAPPING[genre])
