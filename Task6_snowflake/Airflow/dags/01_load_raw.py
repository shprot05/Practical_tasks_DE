from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='01_load_raw_data',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['load_dag']
) as dag:
    upload_to_stage = SnowflakeOperator(
        task_id  = 'upload_to_stage',
        snowflake_conn_id = 'snowflake_default',
        sql="""
                PUT file:///opt/airflow/dags/data/flights.csv @AIRLINE_DB.RAW_DATA.MY_INTERNAL_STAGE 
                AUTO_COMPRESS=TRUE 
                OVERWRITE=TRUE;
            """
    )

    copy_to_table = SnowflakeOperator(
        task_id='copy_to_raw_table',
        snowflake_conn_id='snowflake_default',
        sql="""
                COPY INTO AIRLINE_DB.RAW_DATA.FLIGHTS_RAW
                FROM @AIRLINE_DB.RAW_DATA.MY_INTERNAL_STAGE
                FILE_FORMAT = (FORMAT_NAME = AIRLINE_DB.RAW_DATA.MY_CSV_FORMAT)
                PATTERN = '.*flights.csv.gz'
                ON_ERROR = 'CONTINUE'
                FORCE = TRUE;
            """
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_layer',
        trigger_dag_id='02_load_silver',
        wait_for_completion=False
    )

    upload_to_stage >> copy_to_table >> trigger_silver
