#
# Importing necessary libraries
#
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CRedshiftTableOperator

#
# Setting defaults arguments dictionary
#
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 7, 7),
}

#
# Calling dag with suitable parameters
#
dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create Tables in Redshift with Airflow',
          schedule_interval='@once'
        )

#
# Calling Dummy Operator
#
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# Calling CreateRedshiftTableOperator 
# Purpose of this generic custom operator is to create Redshift\
# fact and dimension tables.
#
create_redshift_table = CRedshiftTableOperator(
    task_id='Create_Redshift_tables',
    dag=dag,
    table_list=['staging_events','staging_songs','songplays','users','artists','songs','time'],
    redshift_conn_id="redshift"
)

#
# Calling Dummy Operator
#
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Defining the task dependencies
#
start_operator >> create_redshift_table >> end_operator

# -- End of DAG code !