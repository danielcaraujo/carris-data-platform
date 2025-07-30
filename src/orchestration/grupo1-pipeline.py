from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from google.auth import default
from googleapiclient.discovery import build
from airflow.operators.bash import BashOperator
from airflow.models import Variable

PROJECT_ID = 'data-eng-dev-437916'
REGION = 'europe-west1'
BATCH_ID = f"spark-batch-{datetime.now().strftime('%Y%m%d%H%M%S')}"
RAW_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/extract_carris.py'
STAGING_LOAD_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/load_to_bigquery.py'
STAGING_MERGE_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/merge_gtfs_and_endpoint.py'
STAGING_CONVERT_ARRAY_SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/convert_array_columns.py'
project_id = Variable.get("PROJECT_ID_GRUPO1")
target = Variable.get("TARGET_GRUPO1")

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

    convert_array_columns = DataprocCreateBatchOperator(
        task_id='staging_explode_array_columns',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"spark-staging-convert-arraycolumns{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        batch=create_batch_config(STAGING_CONVERT_ARRAY_SCRIPT_PATH),
    )

    update_dbt_job = BashOperator(
        task_id="update_cloud_run_job",
        bash_command=f"""
        gcloud run jobs update dbtgrupo1 \
          --image=europe-west1-docker.pkg.dev/{project_id}/dbt-grupo-1/grupo1:latest \
          --region=europe-west1 \
          --set-env-vars=PROJECT_ID={project_id},TARGET={target}
        """
    )

    execute_dbt_job = BashOperator(
        task_id="execute_cloud_run_job",
        bash_command="""
        gcloud run jobs execute dbtgrupo1 \
          --region=europe-west1
        """
    )

    raw_layer_gcs_bucket >> load_to_bigquery >> merge_gfts_endpoint >> convert_array_columns >> update_dbt_job >> execute_dbt_job