import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Manager:
    def __init__(self, aws_conn_id='s3_conn'):
        self.s3 = S3Hook(aws_conn_id=aws_conn_id)
        self.bucket_name = 'datalake-isep'
    def list_csv_files_in_bucket(self):
        keys = self.s3.list_keys(bucket_name=self.bucket_name)
        csv_files = [key for key in keys if key.endswith('.csv')]
        return csv_files

    def list_parquet_files_in_bucket(self):
        keys = self.s3.list_keys(bucket_name=self.bucket_name)
        parquet_files = [key for key in keys if key.endswith('.parquet')]
        return parquet_files

    def download_file(self, key, local_path):
        self.s3.get_key(key, self.bucket_name).download_file(local_path)

    def upload_file(self, key, local_path):
        self.s3.load_file(local_path, key, self.bucket_name, replace=True)

    def upload_directory(self, local_directory, s3_directory):
        if not local_directory.endswith('/'):
            local_directory += '/'
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_path = os.path.join(s3_directory, relative_path)
                self.upload_file(s3_path, local_path)
                print(f"Uploaded {local_path} to {s3_path}")

    def delete_file(self, key):
        self.s3.delete_objects(bucket=self.bucket_name,keys=key)

