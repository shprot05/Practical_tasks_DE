from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.datasets import Dataset
from datetime import datetime
import os
import pandas as pd
import logging

FINAL_FILE_PATH = '/opt/airflow/dags/data_frames/final_df.csv'
MY_DATASET = Dataset(f"file://{FINAL_FILE_PATH}")


def load_to_mongo_func():
    if not os.path.exists(FINAL_FILE_PATH):
        raise FileNotFoundError("Файл dataset не найден!")

    df = pd.read_csv(FINAL_FILE_PATH)

    df['content'] = df['content'].fillna("").astype(str)

    data = df.to_dict(orient='records')
    hook = MongoHook(conn_id='mongo_default')

    hook.insert_many(
        mongo_collection='reviews',
        docs=data,
        mongo_db='analytics_db'
    )
    logging.info("Данные загружены.")


def analyze_mongo_data_func():
    hook = MongoHook(conn_id='mongo_default')
    client = hook.get_conn()
    db = client['analytics_db']
    collection = db['reviews']

    pipeline_top_comments = [
        {"$group": {"_id": "$content", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_comments = list(collection.aggregate(pipeline_top_comments))
    logging.info(f"Top 5 Comments: {top_comments}")

    query_short = {
        "$expr": {
            "$lt": [{"$strLenCP": {"$toString": "$content"}}, 5]
        }
    }
    short_content_docs = list(collection.find(query_short))
    logging.info(f"Short comments count: {len(short_content_docs)}")

    pipeline_avg_score = [
        {
            "$addFields": {
                "date_only": {"$substr": [{"$toString": "$created_date"}, 0, 10]}
            }
        },
        {
            "$group": {
                "_id": "$date_only",
                "avg_score": {"$avg": "$score"}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    avg_scores = list(collection.aggregate(pipeline_avg_score))
    logging.info(f"Avg scores by date: {avg_scores}")


with DAG(
        dag_id='second_dag_mongo_loader',
        start_date=datetime(2023, 1, 1),
        schedule=[MY_DATASET],
        catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo_func
    )

    analyze_task = PythonOperator(
        task_id="analyze_mongo",
        python_callable=analyze_mongo_data_func
    )

    load_task >> analyze_task