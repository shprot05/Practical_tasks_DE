from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '03_load_gold',
    default_args=default_args,
    description='Aggregates Silver data into Gold reporting tables',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'gold', 'reporting'],
) as dag:

    run_gold_procedure = SnowflakeOperator(
        task_id='run_gold_procedure',
        snowflake_conn_id='snowflake_default',
        sql='CALL AIRLINE_DB.GOLD_DATA.PROCESS_DAILY_STATS();',
        autocommit=True,
    )

    run_gold_procedure