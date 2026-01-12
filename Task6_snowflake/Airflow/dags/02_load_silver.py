from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='02_load_silver',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver']
) as dag:
    run_silver_proc = SnowflakeOperator(
        task_id='run_silver_procedure',
        snowflake_conn_id='snowflake_default',
        sql="CALL AIRLINE_DB.SILVER_DATA.PROCESS_FLIGHTS();",
        autocommit=True,
        dag=dag
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_layer',
        trigger_dag_id='03_load_gold',
        wait_for_completion=False
    )

    run_silver_proc >> trigger_gold