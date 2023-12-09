import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/data/demo1/dags/quan-test-project.json'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'upload_csv_to_gcs',
    default_args=default_args,
    description='A simple DAG to upload CSV files to GCS',
    schedule=None,  # You can adjust the schedule as needed
)

# GCS_BUCKET = 'us-central1-test-env-b4f32c38-bucket/data'
GCS_BUCKET = 'us-central1-test-env-b4f32c38-bucket'
GCS_OBJECT = 'Categories.csv'
BQ_DATASET = 'dataset_for_dag_testing'
LOCAL_FOLDER_PATH = '/opt/airflow/data/northwind_gitRepo/northwind_gitRepo/tables/'
# postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')

# Define default_args

# Task to upload files to GCS


upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src=LOCAL_FOLDER_PATH,
    dst='us-central1-test-env-b4f32c38-bucket/data',  # Specify the destination folder in GCS
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_datastore_default',  # Specify your GCP connection ID
    mime_type='application/octet-stream',  # Adjust mime type if needed
    dag=dag
)

# Set task dependencies
# upload_to_gcs

# if __name__ == "__main__":
#     dag.cli()


# upload_files_task = BashOperator(
#     task_id='upload_files_to_gcs',
#     bash_command=f'gsutil -cp {LOCAL_FILE_PATH} gs://{GCS_BUCKET}',
#     # bash_command=f'gsutil -m cp -r {LOCAL_FOLDER_PATH}/* gs://{GCS_BUCKET}',
#     # bash_command= f'echo hello word {name}',
#     dag=dag
# )

# You can add more tasks or dependencies as needed

