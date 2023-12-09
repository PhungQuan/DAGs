from google.cloud import bigquery
import airflow
from airflow.models.dag import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import os
import pandas as pd
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'quan-test-project.json'
# storage_client = storage.Client()
SERVICE_ACCOUNT='quan-project@northwind-qu.iam.gserviceaccount.com'

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/demo1/dags/quan-test-project.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/data/demo1/dags/quan-test-project.json'
# client = bigquery.Client()

default_args = {
    'owner': 'phungquan',
    # 'start_date': airflow.utils.dates.days_ago(0), # chưa hiểu function này
    'start_date': datetime(2023, 12, 1), # YYYY-MM-DD
    'retries': None,
    'retry_daylsy': timedelta(minutes=2)
}

dag = DAG(
    'test-dag',
    default_args=default_args,
    description='Create a dag for testing env',
    schedule=None, # cái này dùng để set chu kì chạy 
    dagrun_timeout=timedelta(minutes=10) # chưa hiểu cái này
)

# Define GCS and BigQuery task parameters
GCS_BUCKET = 'us-central1-test-env-b4f32c38-bucket/data'
gcs_object = 'Customers.csv'
bq_dataset = 'dataset_for_dag_testing'
bq_table = 'Customers'
LOCAL_FILE_PATH = '/opt/airflow/data/northwind_gitRepo/northwind_gitRepo/tables/Customers.csv'

# upload_files_task = BashOperator(
#     task_id='upload_files_to_gcs',
#     bash_command=f'gsutil -cp {LOCAL_FILE_PATH} gs://{GCS_BUCKET}',
#     # bash_command='pwd',
#     # bash_command=f'gsutil -m cp -r {LOCAL_FOLDER_PATH}/* gs://{GCS_BUCKET}',
#     # bash_command= f'echo hello word {name}',
#     dag=dag,
#     cwd=dag.folder
# )

def read_file(filepath):
    df = pd.read_csv(filepath)
    return df.head()

# LOCAL_FILE_PATH = './DataWarehouse/tables/Customers.csv'


read_local_file = PythonOperator(
    task_id='read_csv_file',
    python_callable=read_file,
    op_args=[LOCAL_FILE_PATH], # đây là chỗ truyền argument cho function
    provide_context=True,
    dag=dag,
    
)

# Task to transfer data from GCS to BigQuery
# transfer_task = GCSToBigQueryOperator(
#     task_id = 'transfer_gcs_to_bq',
#     bucket = GCS_BUCKET,
#     source_objects = [gcs_object], # file cần chuyển
#     destination_project_dataset_table = f"{bq_dataset}.{bq_table}",
#     source_format = 'CSV',
#     create_disposition = 'CREATE_IF_NEEDED',
#     write_disposition = 'WRITE_TRUNCATE',
#     skip_leading_rows = 0, # skip dòng đầu
#     field_delimiter = ',',
#     autodetect = True,
#     impersonation_chain=SERVICE_ACCOUNT,
#     dag=dag
# )


# upload_files_task >> transfer_task