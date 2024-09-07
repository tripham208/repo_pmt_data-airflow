import json
from datetime import timedelta

import boto3
import pendulum
import urllib3
from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from minio import Minio

default_args = {
    'owner': 'pmt',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

MINIO = json.loads(Connection.get_connection_from_secrets("pmt_airflow_minio_s3").get_extra())
DATE = "240101"
S3_BUCKET_NAME = "pmt-minio-ldz-bucket"

http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings()

# Initialize MinIO client
minio_client = Minio("s3.amazonaws.com",
                     access_key="",
                     secret_key="",
                     http_client=http_client
                     )

s3_client = boto3.client('s3',
                         aws_access_key_id='',
                         aws_secret_access_key='',
                         endpoint_url='http://localhost:9000')


# MINIO["minio_access_key_id"]
# MINIO["minio_secret_access_key"]
@dag(
    dag_id='minio_to_s3',
    default_args=default_args,
    description='Load file from Minio to S3',
    schedule_interval='@daily'
)
def execute():
    @task
    def load_minio_to_s3():
        #f = minio_client.list_buckets()
        #print(f)
        minio_client.make_bucket("dest-bucket")
        """s3_hook = S3Hook("pmt_airflow_minio_s3")
        s3_hook.load_file_obj(
            file_obj=f,
            key="a",
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )"""

    start_operator = EmptyOperator(task_id='Begin_execution')

    end_operator = EmptyOperator(task_id='End_execution')

    start_operator >> end_operator >> load_minio_to_s3()


minio_to_s3_dag = execute()
