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

data_dir = "/Users/ilan/big-data-airflow-project/data"
local_csv_dir = os.path.join(data_dir, "csv")
local_parquet_dir = os.path.join(data_dir, "parquet")


def list_csv_files_in_bucket(bucket_name):
    s3 = S3Hook(aws_conn_id='s3_conn')
    keys = s3.list_keys(bucket_name=bucket_name)
    csv_files = [key for key in keys if key.endswith('.csv')]
    return csv_files


def download_csv_from_s3(bucket_name, key, local_path):
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.get_key(key, bucket_name).download_file(local_path)


def upload_parquet_to_s3(bucket_name, key, local_path):
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.load_file(local_path, key, bucket_name, replace=True)


def clean_local_directory(directory):
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            for sub_file in os.listdir(file_path):
                sub_file_path = os.path.join(file_path, sub_file)
                os.remove(sub_file_path)
            os.rmdir(file_path)


def format_data():
    formatter = FileFormatter()
    bucket_name = "datalake-isep"
    csv_files = list_csv_files_in_bucket(bucket_name)

    if not os.path.exists(local_csv_dir):
        os.makedirs(local_csv_dir)

    if not os.path.exists(local_parquet_dir):
        os.makedirs(local_parquet_dir)

    for csv_file in csv_files:
        local_csv_path = os.path.join(local_csv_dir, os.path.basename(csv_file))
        download_csv_from_s3(bucket_name, csv_file, local_csv_path)
        df = formatter.read_csv(local_csv_path, header=True)

        parquet_subdir = os.path.join(local_parquet_dir, os.path.splitext(os.path.basename(csv_file))[0])
        if not os.path.exists(parquet_subdir):
            os.makedirs(parquet_subdir)

        df.coalesce(1).write.mode("overwrite").parquet(parquet_subdir)

        for file_name in os.listdir(parquet_subdir):
            if file_name.endswith(".parquet"):
                local_parquet_file = os.path.join(parquet_subdir, file_name)
                parquet_key = os.path.join(os.path.splitext(csv_file)[0] + ".parquet")
                upload_parquet_to_s3(bucket_name, parquet_key, local_parquet_file)
                break

    formatter.stop()

    # Clean up local directories
    #clean_local_directory(data_dir)


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
