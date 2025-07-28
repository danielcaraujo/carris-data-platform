from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from google.auth import default
from googleapiclient.discovery import build

PROJECT_ID = 'data-eng-dev-437916'
REGION = 'europe-west1'
BATCH_ID = f"spark-batch-{datetime.now().strftime('%Y%m%d%H%M%S')}"
RAW_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/extract_carris.py'
STAGING_LOAD_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/load_to_bigquery.py'
STAGING_MERGE_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/merge_gtfs_and_endpoint.py'
STAGING_EXPLODE_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/explode_array_columns.py'

def create_batch_config(script_path: str) -> dict:
    return {
        "pyspark_batch": {
            "main_python_file_uri": script_path,
        },
        "runtime_config": {
            "version": "2.1"
        }
    }

with models.DAG(
    dag_id='grupo1-pipeline',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    tags=['dataproc', 'serverless', 'spark'],
) as dag:

    raw_layer_gcs_bucket = DataprocCreateBatchOperator(
        task_id='raw_layer_gcs_bucket',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"spark-raw-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        batch=create_batch_config(RAW_SCRIPT_PATH),
    )

    load_to_bigquery = DataprocCreateBatchOperator(
        task_id='staging_load_bigquery',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"spark-staging-load-bigquery{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        batch=create_batch_config(STAGING_LOAD_SCRIPT_PATH),
    )

    merge_gfts_endpoint = DataprocCreateBatchOperator(
        task_id='staging_merge_gfts_endpoint',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"spark-staging-merge-gfts-endpoint{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        batch=create_batch_config(STAGING_MERGE_SCRIPT_PATH),
    )

    explode_array_columns = DataprocCreateBatchOperator(
        task_id='staging_explode_array_columns',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"spark-staging-explode-arraycolumns{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        batch=create_batch_config(STAGING_EXPLODE_SCRIPT_PATH),
    )

    trigger_dbt_cloud_run_job = BashOperator(
        task_id='execute_cloud_run_job',
        bash_command=(
            'gcloud run jobs execute NOME_DO_JOB '
            '--region=REGIAO '
            '--project=PROJETO '
            '--wait'
        ),
    )

    raw_layer_gcs_bucket >> load_to_bigquery >> merge_gfts_endpoint >> explode_array_columns >> trigger_dbt_cloud_run_job