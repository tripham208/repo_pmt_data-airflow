from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        records = self.redshift.get_records(self.sql)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records > 1:
            raise ValueError(f"Data quality check failed. {self.table} contained {num_records} rows")
