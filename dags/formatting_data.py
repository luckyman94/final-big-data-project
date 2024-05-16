import os
import shutil
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/Users/ilan/big-data-airflow-project')
from src.formatting.formatter import FileFormatter
from src.utils.s3_manager import S3Manager

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

data_dir = "/Users/ilan/big-data-airflow-project/data"


def clean_local_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

def format_data():
    s3_manager = S3Manager()
    formatter = FileFormatter()
    csv_files = s3_manager.list_csv_files_in_bucket()
    local_parquet_file = None

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for csv_file in csv_files:
        local_csv_path = os.path.join(data_dir, os.path.basename(csv_file))
        s3_manager.download_file(csv_file, local_csv_path)
        s3_manager.delete_file(csv_file)

        df = formatter.read_csv(local_csv_path, header=True)
        df = formatter.normalize_dates_to_utc(df)

        local_parquet_dir = os.path.join(data_dir, os.path.splitext(os.path.basename(csv_file))[0])
        formatter.convert_to_parquet(df, local_parquet_dir)

        for file_name in os.listdir(local_parquet_dir):
            if file_name.endswith(".parquet"):
                local_parquet_file = os.path.join(local_parquet_dir, file_name)
                break

        if local_parquet_file is not None:
            s3_manager.upload_file(os.path.splitext(csv_file)[0] + ".parquet", local_parquet_file)

    clean_local_directory(data_dir)

    formatter.stop()


with DAG(
        'formatting_data',
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
