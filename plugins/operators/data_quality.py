#
# Importing necessary libraries
#
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'  # Setting color of task in dag.
#
# Defining __init__ funcition.
#
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
#
# Defining execute funcition.
#        
    def execute(self, context):
        self.log.info('Executing Data Quality Operator Task ...')
        pg_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_list:
            self.log.info("Running a quick Data Quality Checks on \
            {} table ...".format(table))
            rows = pg_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(rows) < 1 or len(rows[0]) < 1:
                raise ValueError("Oops! Something went wrong with {} ! \
                Please check the code.".format(table))
            elif rows[0][0]<1:
                raise ValueError("Hmm! Seems everything is not OK with \
                {} though! Please verify data sources.".format(table))
            else:
                self.log.info("Hurray! Everything seems fine with {} ! \
                Number of rows fetched := {}.".format(table,rows[0][0]))
            
# --- End of DataQualityOperator code !        
        