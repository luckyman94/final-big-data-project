import os
import sys
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/Users/ilan/big-data-airflow-project')
from src.combining.combining_data import CombiningData
from src.utils.s3_manager import S3Manager

DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"

def combine_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    #Download data from s3 bucket
    s3_manager = S3Manager()
    files = s3_manager.list_parquet_files_in_bucket()


    pp = CombiningData()
    print("Begin combining netflix")
    pp.preprocess_netflix(export_parquet=True)
    local_path = os.path.join(DATA_DIR, os.path.basename(files[1]))
    os.remove(local_path)
    print("End combining netflix")

    print("Begin combining allocine")
    pp.preprocess_allocine(export_parquet=True)
    local_path = os.path.join(DATA_DIR, os.path.basename(files[0]))
    os.remove(local_path)
    print("End combining allocine")

    print("Combining data")
    pp.combine_data()

    #Upload data to s3 bucket
    s3_manager.upload_directory(DATA_DIR, s3_directory="combining")

    #Stop spark session
    pp.stop()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'combining',
        default_args=default_args,
        description='Convert CSV files from S3 to Parquet and upload back to S3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    convert_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )


