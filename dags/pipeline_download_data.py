import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

sys.path.append('/Users/ilan/big-data-airflow-project')
from src.scraping.allocine_scraper import run_scrap_allocine
from src.scraping.netflix_downloader import download_netflix_data

data_dir = "/Users/ilan/big-data-airflow-project/data"

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

def upload_to_s3(directory, bucket_name):
    hook = S3Hook('s3_conn')

    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.csv'):
                filepath = os.path.join(root, filename)
                key = os.path.relpath(filepath, directory)
                if not hook.check_for_key(key, bucket_name=bucket_name):
                    hook.load_file(filename=filepath, key=key, bucket_name=bucket_name)
                    print(f"File {filename} loaded successfully in the bucket {bucket_name}.")
                else:
                    print(f"File {filename} already exists in the bucket {bucket_name}, skipping.")

                os.remove(filepath)


with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    task_scrap_allocine = PythonOperator(
        task_id='scrap_allocine',
        python_callable=run_scrap_allocine,
        op_kwargs={'num_pages': 2}
    )

    task_scrap_netflix = PythonOperator(
        task_id='scrap_netflix',
        python_callable=download_netflix_data,
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'directory': '/Users/ilan/big-data-airflow-project/data',
            'bucket_name': 'datalake-isep'
        }
    )

task_scrap_netflix >> task_scrap_allocine >> task_upload_to_s3