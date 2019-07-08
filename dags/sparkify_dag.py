#
# Importing necessary libraries
#
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries as sq

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#
# Setting defaults arguments dictionary
#
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 4), #  datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'catchup': False
}

#
# Calling dag with suitable parameters
#
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )
#
# Calling Dummy Operator
#
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# Calling StageToRedshiftOperator 
# Purpose of this generic custom operator is to load flat files(JSON/csv) \
# on S3 to Redshift stage table.
#
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    table="staging_events",
    compupdate="off",
    JSON="s3://udacity-dend/log_json_path.json",
    region="us-west-2"
)

#
# Calling StageToRedshiftOperator 
# Purpose of this generic custom operator is to load flat files(JSON/csv) \
# on S3 to Redshift stage table.
#
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    table="staging_songs",
    region="us-west-2",
    compupdate="off",
    JSON="auto"
)

#
# Calling LoadFactOperator 
# Purpose of this generic custom operator is to load data into Redshift Fact table \  
# using stage table data.
#
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    load_style="insert",  # Set this parm  for loading style: 'truncate-insert' or 'insert'
    insert_query=sq.songplay_table_insert
)

#
# Calling LoadDimensionOperator 
# Purpose of this generic custom operator is to load data into Redshift Dimension table \  
# using stage table data.
#
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    load_style="truncate-insert",  # Set this parm  for loading style: 'truncate-insert' or 'insert'
    insert_query=sq.user_table_insert
)

#
# Calling LoadDimensionOperator 
# Purpose of this generic custom operator is to load data into Redshift Dimension table \  
# using stage table data.
#
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    load_style="truncate-insert",  # Set this parm  for loading style: 'truncate-insert' or 'insert'
    insert_query=sq.song_table_insert
)

#
# Calling LoadDimensionOperator 
# Purpose of this generic custom operator is to load data into Redshift Dimension table \  
# using stage table data.
#
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    load_style="truncate-insert",  # Set this parm  for loading style: 'truncate-insert' or 'insert'
    insert_query=sq.artist_table_insert
)

#
# Calling LoadDimensionOperator 
# Purpose of this generic custom operator is to load data into Redshift Dimension table \  
# using stage table data.
#
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    load_style="truncate-insert",  # Set this parm  for loading style: 'truncate-insert' or 'insert'
    insert_query=sq.time_table_insert
)

#
# Calling DataQualityOperator 
# Purpose of this generic custom operator is to do a quick data quality checks. \  
#
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_list=['staging_events','staging_songs','songplays','users','artists','songs','time'],
    redshift_conn_id="redshift"
)

#
# Calling Dummy Operator
#
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Defining the task dependencies (using lists)
#
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> \
[load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,\
load_time_dimension_table] >> run_quality_checks >> end_operator

# -- End of DAG code !