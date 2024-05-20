import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.preprocessing.preprocessing import Preprocessing
from src.utils.s3_manager import S3Manager

DATA_DIR = "/Users/ilan/big-data-airflow-project/data/"

def run_preprocessing():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    #Download data from s3 bucket
    s3_manager = S3Manager()
    files = s3_manager.list_parquet_files_in_bucket()


    #Preprocess data
    pp = Preprocessing()
    print("Begin preprocessing netflix")
    pp.preprocess_netflix(export_parquet=True)
    local_path = os.path.join(DATA_DIR, os.path.basename(files[1]))
    os.remove(local_path)
    print("End preprocessing netflix")

    print("Begin preprocessing allocine")
    pp.preprocess_allocine(export_parquet=True)
    local_path = os.path.join(DATA_DIR, os.path.basename(files[0]))
    os.remove(local_path)
    print("End preprocessing allocine")

    print("Combining data")
    pp.combine_data()

    #Upload data to s3 bucket
    s3_manager.upload_directory(DATA_DIR, s3_directory="preprocessing")

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
        'preprocessing',
        default_args=default_args,
        description='Convert CSV files from S3 to Parquet and upload back to S3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    convert_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=run_preprocessing,
    )


