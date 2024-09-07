from datetime import timedelta
import pendulum

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from plugins.custom_operator.redshift.stage_redshift import StageToRedshiftOperator
from plugins.custom_operator.redshift.load_fact import LoadFactOperator
from plugins.custom_operator.redshift.load_dimension import LoadDimensionOperator
from plugins.custom_operator.redshift.data_quality import DataQualityOperator
from plugins.custom_operator.redshift.sql_statements import SqlQueries as S

default_args = {
    'owner': 'pmt',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def s3_to_redshift():
    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        redshift_conn_id="redshift",
        table=S.STAGING_EVENTS,
        s3_part="'s3://udacity-dend/log_data'",
        iam_role=Variable.get("pmt_redshift_iam_role"),
        json_opt="'s3://udacity-dend/log_json_path.json'",
        task_id="Stage_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        table=S.STAGING_SONGS,
        s3_part="'s3://udacity-dend/song_data'",
        iam_role=Variable.get("pmt_redshift_iam_role"),
        json_opt="'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=S.songplay_table_insert,
        table=S.SONG_PLAY
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql=S.user_table_insert,
        table=S.USERS
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql=S.song_table_insert,
        table=S.SONGS
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql=S.artist_table_insert,
        table=S.ARTISTS
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql=S.time_table_insert,
        table=S.TIME
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table=S.USERS,
        sql=S.validate_user
    )

    end_operator = EmptyOperator(task_id='End_execution')

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks >> end_operator


s3_to_redshift_dag = s3_to_redshift()
