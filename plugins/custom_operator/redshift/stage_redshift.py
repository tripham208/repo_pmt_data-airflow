from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    s3_copy = """
            copy {table_name} from {s3_part} 
            iam_role {iam_role} 
            region 'us-west-2'
            format as json {opt}
            """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 s3_part="",
                 iam_role="",
                 json_opt="'auto'",
                 append=False,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_part = s3_part
        self.iam_role = iam_role
        self.json_opt = json_opt
        self.redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.table_name = table
        self.append = append

    def execute(self, context):
        if not self.append:
            self.log.info("Clearing data from destination Redshift table")
            self.redshift.run("DELETE FROM {}".format(self.table_name))

        self.log.info("Copying data from S3 to Redshift")
        self.redshift.run(
            self.s3_copy.format(
                table_name=self.table_name,
                s3_part=self.s3_part,
                iam_role=self.iam_role,
                opt=self.json_opt
            )
        )
