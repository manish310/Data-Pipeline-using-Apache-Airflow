#
# Importing necessary libraries
#
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#
# Defining LoadFactOperator class
#
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

#
# Defining __init__ funcition.
#
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_query="",
                 load_style="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_query=insert_query
        self.load_style=load_style

#
# Defining execute funcition.
#       
    def execute(self, context):
        self.log.info('Executing LoadFactOperator ...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if (self.load_style=='truncate-insert'): # option 1
            self.log.info("Clearing data from {} Fact table ...".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Inserting Data into {} Fact Table ...".format(self.table))
            redshift.run("INSERT INTO {} {}".format(self.table,self.insert_query))
        elif (self.load_style=='insert'): # option 2
            self.log.info("Inserting Data into {} Fact Table ...".format(self.table))
            redshift.run("INSERT INTO {} {}".format(self.table,self.insert_query))
        else:   # default option
            self.log.info("Inserting Data into {} Fact Table ...".format(self.table))
            redshift.run("INSERT INTO {} {}".format(self.table,self.insert_query))             
                       
# --- End of LoadFactOperator code ! 