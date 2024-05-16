import os
import sys

import pytest
from unittest import mock
sys.path.append('/')
from src.scraping.netflix_downloader import fetch_data_from_github, download_netflix_data

GITHUB_DATA_URL = 'https://raw.githubusercontent.com/Ashar88/Netflix_shows_Analysis__EDA/main/netflix_dataset.csv'

def test_fetch_data_from_github(mocker):
    mock_requests_get = mocker.patch('requests.get')
    mock_response = mock.Mock()
    mock_response.iter_content = lambda chunk_size: [b'data']
    mock_response.raise_for_status = mock.Mock()
    mock_requests_get.return_value = mock_response

    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mock.mock_open())

    fetch_data_from_github(GITHUB_DATA_URL)

    mock_requests_get.assert_called_once_with(GITHUB_DATA_URL)
    mock_response.raise_for_status.assert_called_once()

def test_download_netflix_data(mocker):
    mock_fetch_data = mocker.patch('src.scraping.netflix_downloader.fetch_data_from_github')
    download_netflix_data()
    mock_fetch_data.assert_called_once_with(GITHUB_DATA_URL)

