from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from datetime import datetime
import os
import pandas as pd
import re
import logging


SOURCE_FILE_PATH = '/opt/airflow/dags/data/tiktok_google_play_reviews.csv'
FINAL_FILE_PATH = '/opt/airflow/dags/data_frames/final_df.csv'
MY_DATASET = Dataset(f"file://{FINAL_FILE_PATH}")


def branch_func(**kwargs):
    if os.path.exists(SOURCE_FILE_PATH) and os.path.getsize(SOURCE_FILE_PATH) > 0:
        logging.info("Файл найден и он не пустой. Идем в обработку.")
        return 'data_processing_group.clean_data'
    else:
        logging.info("Файл пустой или отсутствует.")
        return 'empty_file_task'

def clean_data_func():
    os.makedirs('/opt/airflow/dags/data_frames', exist_ok=True)
    df = pd.read_csv(SOURCE_FILE_PATH)
    clean_df = df.fillna("-")
    output_path = '/opt/airflow/dags/data_frames/clean_df.csv'
    clean_df.to_csv(output_path, index=False)
    return output_path

def sort_data_func(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='data_processing_group.clean_data')
    df = pd.read_csv(file_path)
    sort_df = df.sort_values(by=['at'])
    output_path = '/opt/airflow/dags/data_frames/sort_df.csv'
    sort_df.to_csv(output_path, index=False)
    return output_path

def delete_symbols_func(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='data_processing_group.sort_data')
    df = pd.read_csv(file_path)

    def clean_text(text):
        if not isinstance(text, str):
            return ""
        pattern = r'[^a-zA-Zа-яА-ЯёЁ0-9\s\.,!?;:()"\'-]'
        return re.sub(pattern, '', text)

    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text)

    df.to_csv(FINAL_FILE_PATH, index=False)
    return FINAL_FILE_PATH

with DAG(
        dag_id="first_dag_processing",
        start_date=datetime(2023, 12, 16),
        schedule_interval=None,
        catchup=False
) as dag:

    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath=SOURCE_FILE_PATH,
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=60,
        mode="poke"
    )

    check_file_branch = BranchPythonOperator(
        task_id="check_file_branch",
        python_callable=branch_func
    )

    empty_file_task = BashOperator(
        task_id="empty_file_task",
        bash_command='echo "Файл пустой!"'
    )

    with TaskGroup("data_processing_group", tooltip="Этап обработки данных") as processing_group:
        clean_data = PythonOperator(
            task_id="clean_data",
            python_callable=clean_data_func
        )

        sort_data = PythonOperator(
            task_id="sort_data",
            python_callable=sort_data_func
        )

        delete_symbols = PythonOperator(
            task_id="delete_symbols",
            python_callable=delete_symbols_func,
            outlets=[MY_DATASET]
        )

        clean_data >> sort_data >> delete_symbols

    file_sensor >> check_file_branch
    check_file_branch >> empty_file_task
    check_file_branch >> processing_group