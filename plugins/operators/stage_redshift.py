#
# Importing necessary libraries
#
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                compupdate {}
                JSON '{}'
                REGION '{}';
               """
#
# Defining __init__ funcition.
#
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 compupdate="",
                 JSON="",
                 region="",
                 #delimiter=",",
                 #ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        #self.delimiter = delimiter
        #self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.compupdate=compupdate
        self.JSON=JSON
        self.region=region

#
# Defining execute funcition.
# 
    def execute(self, context):
        self.log.info('Executing StageToRedshiftOperator ...')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        stage_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            #self.ignore_headers,
            self.compupdate,
            self.JSON,
            self.region
            #self.delimiter
        )
        redshift.run(stage_query)

# --- End of StageToRedshiftOperator code !