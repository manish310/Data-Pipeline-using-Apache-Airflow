#
# Importing necessary libraries
#
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#
# Defining CRedshiftTableOperator class
#
class CRedshiftTableOperator(BaseOperator):

    ui_color = '#F98890'

#
# Defining __init__ funcition.
#
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 *args, **kwargs):

        super(CRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
#
# Defining execute funcition.
#  
    def execute(self, context):
        # Reading create table sql file.
        f = open('/home/workspace/airflow/create_tables.sql', 'r')
        sql_file = f.read()
        f.close()
        # Seperating sql query for execution.
        sql_query=sql_file.split(';')
        
        self.log.info('Executing CRedshiftTableOperator ...')
        #redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Executing Data Quality Operator Task ...')
        pg_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_list:
            # Dropping tables' old schema.
            self.log.info("Dropping table if {} already exists ...".format(table))
            pg_hook.run("DROP TABLE IF EXISTS {};".format(table))
        for query in sql_query:    
            # Creating new schema of the tables.
            if query:
                self.log.info("Creating table {} ...".format(query.split(' ')[2]))
                pg_hook.run(query)

# --- End of CRedshiftTableOperator code !  