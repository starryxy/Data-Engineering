from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadTableOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'yukixiao',
    'start_date': datetime(2020, 5, 12),
    # DAG does not have dependencies on past runs
    'depends_on_past': False,
    # SLA: each task in the DAG needs to complete within 1 hr
    'sla':timedelta(hours=1),
    # On failure, tasks are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Do not email on retry
    'email_on_retry': False,
    'email_on_failure': True,
    # 'email_on_failure': False,
    'email': ['starryxy311@gmail.com']
}

dag = DAG('youtube_trending_videos_etl_dag',
          default_args=default_args,
          description='Full ETL pipeline combining trending YouTube videos and channels data',
          # daily
          schedule_interval='@daily'
          # once
          # schedule_interval='@once',
          # Turn off catchup
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_videos_to_redshift = StageToRedshiftOperator(
    task_id='Stage_videos',
    dag=dag,
    table="staging_videos",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="aws-logs-511878075746-us-west-2",
    s3_key="youtube/Youtubevideos.csv"
)

stage_channels_to_redshift = StageToRedshiftOperator(
    task_id='Stage_channels',
    dag=dag,
    table="staging_channels",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="aws-logs-511878075746-us-west-2",
    s3_key="youtube/Youtubechannels.csv"
)

load_trendingvideos_table = LoadTableOperator(
    task_id='Load_trendingvideos_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="trendingvideos",
    sql=SqlQueries.insert_trendingvideos_table
)

load_videos_dimension_table = LoadTableOperator(
    task_id='Load_videos_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="videos",
    sql=SqlQueries.insert_videos_table
)

load_channels_dimension_table = LoadTableOperator(
    task_id='Load_channels_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="channels",
    sql=SqlQueries.insert_channels_table
)

load_category_dimension_table = LoadTableOperator(
    task_id='Load_category_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="category",
    sql=SqlQueries.insert_category_table
)

load_country_dimension_table = LoadTableOperator(
    task_id='Load_country_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="country",
    sql=SqlQueries.insert_country_table
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[
            "staging_videos",
            "staging_channels",
             "trendingvideos",
             "videos",
             "channels",
             "category",
             "country"
             ]
)

compute_most_trending_days_video = LoadTableOperator(
    task_id='Compute_most_trending_days_video',
    dag=dag,
    redshift_conn_id="redshift",
    table="most_trending_days_video",
    sql=SqlQueries.most_trending_days_video
)

compute_most_trending_channel = LoadTableOperator(
    task_id='Compute_most_trending_channel',
    dag=dag,
    redshift_conn_id="redshift",
    table="most_trending_channel",
    sql=SqlQueries.most_trending_channel
)

validate_data = DataQualityOperator(
    task_id='Validate_computation_results',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["most_trending_days_video", "most_trending_channel"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define dependencies
start_operator >> create_tables_task >> [stage_videos_to_redshift, stage_channels_to_redshift]

[stage_videos_to_redshift, stage_channels_to_redshift] >> load_trendingvideos_table
load_trendingvideos_table >> [load_videos_dimension_table, load_channels_dimension_table, load_category_dimension_table, load_country_dimension_table]

[load_videos_dimension_table, load_channels_dimension_table, load_category_dimension_table, load_country_dimension_table] >> run_quality_checks

run_quality_checks >> [compute_most_trending_days_video, compute_most_trending_channel] >> validate_data
validate_data >> end_operator
