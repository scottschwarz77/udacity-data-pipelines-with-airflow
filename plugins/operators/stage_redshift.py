from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key")
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        
    def execute(self, context):
        self.log.info("Running StageToRedshiftOperator")
        metastoreBackend = MetastoreBackend()
        aws_conn=metastoreBackend.get_connection(self.aws_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            aws_conn.login,
            aws_conn.password,
            self.json_path
        )
        redshift_hook.run(formatted_sql)