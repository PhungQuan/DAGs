import logging
from datetime import datetime
from typing import Dict
import pathlib
import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/data/demo1/dags/quan-test-project.json'

FOLDER_PATH = '/opt/airflow/data/northwind_gitRepo/northwind_gitRepo/tables'
GCS_BUCKET = 'us-central1-test-env-b4f32c38-bucket'


@task(task_id="extract", retries=None)
def extract_file_path():
    all_path = []
    for filename in os.listdir(FOLDER_PATH):
        path = os.path.join(FOLDER_PATH, filename)
        all_path.append(path)
    return all_path

@dag(schedule_interval=None, start_date=datetime(2023, 11, 8), catchup=False)
def taskflow():
    # Call the extract_file_path task and store the output in a variable
    extracted_paths = extract_file_path()

    # Use the extracted_paths variable as an argument for the LocalFilesystemToGCSOperator
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=extracted_paths,
        dst='data/',  # Specify the destination folder in GCS
        bucket=GCS_BUCKET,
        gcp_conn_id='google_cloud_datastore_default',  # Specify your GCP connection ID
        mime_type='application/octet-stream',  # Adjust mime type if needed
    )

# Instantiate the taskflow DAG
taskflow()
