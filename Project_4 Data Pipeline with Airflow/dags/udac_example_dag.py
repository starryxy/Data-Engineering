from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12), 
    # DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, tasks are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False, 
    # Do not email on retry
    'email_on_retry': False, 
    'email_on_failure': False,
    'email': ['starryxy311@gmail.com']
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow', 
          # hourly
          schedule_interval='0 * * * *'
          # once
          # schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data", 
    # s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json", 
    # test smaller dataset
    # s3_key="log_data/2018/11", 
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag, 
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data", 
    # test smaller dataset
    # s3_key="song_data/A/A", 
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="songs", 
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="artists", 
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="time", 
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

