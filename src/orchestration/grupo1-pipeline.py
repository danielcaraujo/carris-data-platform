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
SCRIPT_PATH = 'gs://applied-project/grupo-1/scripts/extract_carris.py'

CLOUD_RUN_JOB_NAME = 'nome-do-job-cloudrun' #define our cloud_run_jon_name

# def trigger_cloud_run_job(project_id, region, job_name):
#     credentials, _ = default()
#     service = build('run', 'v1', credentials=credentials)

#     parent = f"namespaces/{project_id}/jobs/{job_name}"
#     request = service.namespaces().jobs().run(
#         name=parent,
#         body={}
#     )
#     response = request.execute()
#     print(f"Cloud Run Job triggered: {response}")

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

    spark_serverless_job = DataprocCreateBatchOperator(
        task_id='executa_spark_serverless',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SCRIPT_PATH,
            },
            "runtime_config": {
                "version": "2.1"
            }
        }
    )

    # trigger_job = PythonOperator(
    #     task_id='trigger_cloud_run_job',
    #     python_callable=trigger_cloud_run_job,
    #     op_kwargs={
    #         'project_id': PROJECT_ID,
    #         'region': REGION,
    #         'job_name': CLOUD_RUN_JOB_NAME,
    #     }
    # )

    spark_serverless_job