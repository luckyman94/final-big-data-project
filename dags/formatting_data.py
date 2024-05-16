import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

sys.path.append('/Users/ilan/big-data-airflow-project')
from src.formatting.formatter import FileFormatter

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_csv_path = "/Users/ilan/big-data-airflow-project/data/allocine.csv"
local_parquet_dir = "/Users/ilan/big-data-airflow-project/data/parquet"
local_parquet_file = "/Users/ilan/big-data-airflow-project/data/parquet/allocine.parquet"


def download_csv_from_s3(bucket_name, key, local_path):
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.get_key(key, bucket_name).download_file(local_path)


def upload_parquet_to_s3(bucket_name, key, local_path):
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.load_file(local_path, key, bucket_name, replace=True)


def format_data():
    formatter = FileFormatter()
    download_csv_from_s3("datalake-isep", "allocine/allocine_movies.csv", local_csv_path)

    df = formatter.read_csv(local_csv_path, header=True)

    if not os.path.exists(local_parquet_dir):
        os.makedirs(local_parquet_dir)

    df.coalesce(1).write.mode("overwrite").parquet(local_parquet_dir)

    for file_name in os.listdir(local_parquet_dir):
        if file_name.endswith(".parquet"):
            local_parquet_file = os.path.join(local_parquet_dir, file_name)
            break

    upload_parquet_to_s3("datalake-isep", "allocine/allocine_movies.parquet", local_parquet_file)

    formatter.stop()


with DAG(
        's3_csv_to_parquet',
        default_args=default_args,
        description='Convert CSV files from S3 to Parquet and upload back to S3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    convert_task = PythonOperator(
        task_id='convert_csv_to_parquet',
        python_callable=format_data,
    )

convert_task
