import os

from pyspark.shell import spark
from pyspark.sql.functions import col, round as spark_round, mean, split, array_contains, udf, when, regexp_replace, lit
from pyspark.sql.types import FloatType, StringType
from . import config
from src.utils.s3_manager import S3Manager




class CombiningData:
    def __init__(self):
        self.s3_manager = S3Manager()
        self.download_data()
        self.df_allocine = spark.read.parquet(config.DATA_DIR + "/allocine_movies.parquet", header=True)
        self.df_netflix = spark.read.parquet(config.DATA_DIR + "/NetflixDataset.parquet", header=True)

    def download_data(self):
        if not os.path.exists(config.DATA_DIR):
            os.makedirs(config.DATA_DIR)

        parquet_files = self.s3_manager.list_parquet_files_in_bucket()
        for file in parquet_files:
            local_path = os.path.join(config.DATA_DIR, os.path.basename(file))
            self.s3_manager.download_file(file, local_path)

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

        self.df_netflix = self.df_netflix.drop("Genre")
        self.df_netflix = self.df_netflix.dropna(subset=["Summary"])
        self.df_netflix = self.df_netflix.dropna(subset=["Actors"])

        if export_parquet:
            parquet_file_path = config.DATA_DIR + "/NetflixDataset_preprocessed.parquet"
            self.df_netflix.coalesce(1).write.parquet(parquet_file_path)

    def preprocess_allocine(self, export_parquet=False):
        self.df_allocine = self.df_allocine.drop("Director", "Release Date")

        # Renaming columns to match the Netflix dataset
        self.df_allocine = self.df_allocine.withColumnRenamed("Duration", "Runtime")
        self.df_allocine = self.df_allocine.withColumnRenamed("Synopsis", "Summary")

        # Convert runtime to interval (categorical variable)
        self.df_allocine = self.df_allocine.dropna(subset=["Runtime"])
        self.df_allocine = self.df_allocine.withColumn("Runtime", regexp_replace("Runtime", "min", ""))
        self.df_allocine = self.df_allocine.withColumn("Runtime", regexp_replace("Runtime", "h", ""))
        self.df_allocine = self.df_allocine.withColumn("Runtime", split(col("Runtime"), " "))
        def convert_runtime_to_interval(runtime):
            hours = int(runtime[0])
            minutes = int(runtime[1]) if len(runtime) > 1 else 0
            total_hours = hours + minutes / 60
            if total_hours > 2:
                return '> 2 hrs'
            elif total_hours < 0.5:
                return '< 30 minutes'
            elif total_hours < 1 and total_hours >= 0.5:
                return '30 - 60 mins'
            else:
                return '1-2 hour'

        convert_runtime_to_interval_udf = udf(convert_runtime_to_interval, StringType())


        self.df_allocine = self.df_allocine.withColumn("Runtime", convert_runtime_to_interval_udf(col("Runtime")))

        # Merge the spectator rating and the press rating into a single rating column
        self.df_allocine = self._merge_ratings()

        # One Hot Encoding of the genre.
        self.df_allocine = self.df_allocine.withColumn("Genre", split(col("Genre"), ", "))
        for genre in config.GENRES:
            self.df_allocine = self.df_allocine.withColumn(genre, array_contains(col("Genre"), genre).cast("integer"))

        self.df_allocine = self.df_allocine.drop("Genre")
        self.df_allocine = self.df_allocine.dropna(subset=["Summary"])
        self.df_allocine = self.df_allocine.dropna(subset=["Actors"])

        # Add a column type to match the Netflix dataset
        self.df_allocine = self.df_allocine.withColumn("Type", lit("Movie"))

        if export_parquet:
            parquet_file_path = config.DATA_DIR + "/allocine_movies_preprocessed.parquet"
            self.df_allocine.coalesce(1).write.parquet(parquet_file_path)

    def combine_data(self):
        parquet_path = config.DATA_DIR + "/final_dataset.parquet"
        df1 = spark.read.parquet(config.DATA_DIR + "/NetflixDataset_preprocessed.parquet", header=True, inferSchema=True)
        df2 = spark.read.parquet(config.DATA_DIR + "/allocine_movies_preprocessed.parquet", header=True, inferSchema=True)

        self.final_dataset = self._combine_two_dataframe(df1, df2)
        self.final_dataset.write.parquet(parquet_path)

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


    def _combine_two_dataframe(self, df1, df2):
        # Reorder columns to match
        df2_reordered = df2.select(df1.columns)

        # Combine the two dataframes
        combined_df = df1.union(df2_reordered)
        return combined_df




    def stop(self):
        spark.stop()