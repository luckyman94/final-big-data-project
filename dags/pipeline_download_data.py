import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.s3_manager import S3Manager

sys.path.append('/Users/ilan/big-data-airflow-project')
from src.scraping.allocine_scraper import run_scrap_allocine
from src.scraping.netflix_downloader import download_netflix_data

data_dir = "/Users/ilan/big-data-airflow-project/data"

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

def upload_to_s3():
    s3_manager = S3Manager()
    s3_manager.upload_directory(data_dir,  remove_files=True, extension='.csv')


with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False,
) as dag:

    task_scrap_allocine = PythonOperator(
        task_id='scrap_allocine',
        python_callable=run_scrap_allocine,
        op_kwargs={
            'num_pages': 1
        }
    )

    task_scrap_netflix = PythonOperator(
        task_id='scrap_netflix',
        python_callable=download_netflix_data,
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3

    )

task_scrap_netflix >> task_scrap_allocine >> task_upload_to_s3

