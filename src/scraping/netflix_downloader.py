import io
import os
import logging
from datetime import date
import requests

from . import config


def fetch_data_from_github(url, **kwargs):
    try:
        logging.info("Starting to fetch data...")
        local_directory = f"data/netflix/"
        os.makedirs(local_directory, exist_ok=True)
        filename = url.split("/")[-1]
        file_path = os.path.join(local_directory, filename)

        if os.path.exists(file_path):
            logging.info(f"File {file_path} already exists, skipping download.")
            return

        response = requests.get(url)
        response.raise_for_status()

        with open(file_path, 'wb') as file:
            file.write(response.content)

        logging.info(f"Data downloaded and saved locally to {file_path}.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


def download_netflix_data():
    GITHUB_DATA_URL = 'https://raw.githubusercontent.com/Ashar88/Netflix_shows_Analysis__EDA/main/netflix_dataset.csv'
    fetch_data_from_github(GITHUB_DATA_URL)


if __name__ == "__main__":
    download_netflix_data()