from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql    = """
     COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}' 
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region=region
        self.json=json

    def execute(self, context):
        """
        Copies data from S3 to Redshift staging tables
        Parameters:
            redshift_conn_id: (str) redshift credentials
            aws_credentials_id: (str) IAM Role credentials to access S3 bucket
            table: (str) name of a table
            s3_bucket: (str) s3 bucket name
            s3_key: (str) location of s3 bucket
            region: (str) region wher s3 bucket is located
            json: (str) define format of data
        """

        self.log.info(f"Stage Process: s3 to {self.table}.")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination {self.table} table on Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from S3 to {self.table} table on Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        redshift.run(sql)
        records = redshift.get_records(f"select count(*) from {self.table};")
        self.log.info(f"Found {len(records)} in {self.table}")
        self.log.info(f"Successfully copied data to {self.table} table")
