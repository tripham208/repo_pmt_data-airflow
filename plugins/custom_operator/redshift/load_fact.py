from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
        self.append = append

    def execute(self, context):
        if not self.append:
            self.log.info("Clearing data from destination Redshift Fact table")
            self.redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Load data to Redshift Fact table")
        self.redshift.run(self.sql)
