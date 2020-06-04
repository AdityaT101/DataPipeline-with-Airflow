from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( StageToRedshiftOperator, StageToRedshiftOperatorOne, LoadFactOperator, LoadDimensionOperator, DataQualityOperator , PostgresOperator )
from helpers import SqlQueries
 
    
 

#defining the default arhguments for the Dag
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 3, 1),
    'Depends_on_past': False,
    'Retries': 3,
    'Retry_delay': timedelta(minutes=5),
    'Catchup': True,
    'email_on_retry': False
}


#defining the Dag
dag = DAG(
          'udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

#defining the start operator, acts like an entry point operator for the DAG.
start_operator = DummyOperator( task_id ='Begin_execution',  dag=dag )

'''
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)
'''

#operators to stage the event tables. 
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/log_data"
)
 

#operators to stage the songs table.
stage_songs_to_redshift = StageToRedshiftOperatorOne(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/song_data/A/A/"
) 


#defining operator to load Songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.songplay_table_insert,
    sql_delete=SqlQueries.songplay_table_delete,
    append_data = True
)


#defining operator to load user dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.user_table_insert,
    sql_delete=SqlQueries.user_table_delete,
    append_data = False
)


#defining operator to load song table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.song_table_insert,
    sql_delete=SqlQueries.song_table_delete,
    append_data = False
)


#defining operator to load artist table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.artist_table_insert,
    sql_delete=SqlQueries.artist_table_delete,
    append_data = False
)


#defining operator to load time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.time_table_insert,
    sql_delete=SqlQueries.time_table_delete,
    append_data = False
)


 
#defining operator for doing a quality check on data.
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    
    dq_checks=[
       {'check_sql': 'select count(*) from public."songs";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."users";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."artists";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."time";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."songplays";' , 'expected_result': 0 }
    ]
)

 


 
end_operator = DummyOperator( task_id='Stop_execution',  dag=dag )

#start_operator >> create_tables

#Data Pipeline - step 1 - running the start and the staging tasks( dependant tasks )
start_operator >> [ stage_events_to_redshift , stage_songs_to_redshift ] 
[ stage_events_to_redshift , stage_songs_to_redshift ]  >> load_songplays_table
 
 
#Data Pipeline - step 2 - running the dimension tasks
load_songplays_table >>[ load_user_dimension_table , load_song_dimension_table ,load_artist_dimension_table ,load_time_dimension_table ]
 

#Data Pipeline - step 3 - running the Data Quality check task. 
[ load_user_dimension_table , load_song_dimension_table ,load_artist_dimension_table ,load_time_dimension_table ] >> run_quality_checks


run_quality_checks >> end_operator

 



