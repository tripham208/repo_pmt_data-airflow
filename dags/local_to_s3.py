from datetime import timedelta
import pendulum
import glob

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

default_args = {
    'owner': 'pmt',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

DATE = "240101"
S3_BUCKET_NAME = "pmt-local-ldz-bucket"


@dag(
    dag_id='local_to_s3',
    default_args=default_args,
    description='Load file from local file system to S3',
    schedule='@once'
)
def execute():
    end_operator = EmptyOperator(task_id='End_execution')

    files = []

    for file in glob.glob(f"/opt/airflow/data/data_sample_{DATE}/*"):
        files.append(file)

    for idx, file in enumerate(files, start=1):
        file_name = file.split("/")[-1]

        transfer_operator = LocalFilesystemToS3Operator(
            task_id="local_to_s3_" + str(idx),
            filename=file,
            dest_key=f"date={DATE}/{file_name}",
            dest_bucket=S3_BUCKET_NAME,
            replace=True,
            aws_conn_id="pmt_airflow_aws"
        )

        transfer_operator >> end_operator


local_to_s3_dag = execute()
