from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import Sqlquery
from airflow.secrets.metastore import MetastoreBackend

default_args = {
    'owner': 'scott_schwarz',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 5,
    'catchup': True,
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    
    metastoreBackend = MetastoreBackend()
    aws_conn=metastoreBackend.get_connection("aws_connection")
    
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift_conn",
        aws_conn_id="aws_conn",
        target_table='staging_songs',
        s3_bucket='scottschwarz77-bucket5200',
        s3_key="song-data/A/A/H/",
        json_path = "FORMAT JSON 'auto'"
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift_conn",
        aws_conn_id="aws_conn",
        target_table='staging_events',
        s3_bucket='scottschwarz77-bucket5200',
        json_path="FORMAT JSON 's3://scottschwarz77-bucket5200/log_json_path.json'",
        s3_key="log-data/{{execution_date.year}}/{{execution_date.month}}/{{execution_date.year}}-{{execution_date.month}}-{{execution_date.day}}-events.json"
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        target_table="songplays",
        sql_query= Sqlquery.songplay_table_insert,
        redshift_conn_id="redshift_conn"
    )

    load_user_dimension_table = LoadDimensionOperator(
        redshift_conn_id="redshift_conn",
        task_id="Load_user_dim_table",
        target_table="users",
        #append_key="userid",
        sql_query= Sqlquery.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        redshift_conn_id="redshift_conn",
        task_id='Load_song_dim_table',
        target_table="songs",
        #append_key="songid",
        sql_query= Sqlquery.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        redshift_conn_id="redshift_conn",
        task_id="Load_artist_dim_table",
        target_table="artists",
        #append_key="artistid",
        sql_query=Sqlquery.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        redshift_conn_id="redshift_conn",
        task_id="Load_time_dim_table",
        target_table="time",
        #append_key="start_time",
        sql_query=Sqlquery.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        redshift_conn_id="redshift_conn",
        task_id='Run_data_quality_checks',
        tests=
        [
            ("select count(*) from songs", lambda n: n > 0),
            ("select count(*) from users", lambda n: n > 0),
            ("select count(*) from artists", lambda n: n > 0),
            ("select count(*) from time", lambda n: n > 0)
        ]
    )
    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

final_project_dag = final_project()