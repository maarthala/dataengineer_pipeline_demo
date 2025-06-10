from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging


def say_hello():
    print("Hello, Airflow!")

with DAG(
    dag_id="test_hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Only run when triggered manually
    catchup=False,
    tags=["test"],
) as dag:
    def extract():
        logging.info("🔍 Extracting data...")

    def transform():
        logging.info("🔄 Transforming data...")

    def load():
        logging.info("📤 Loading data...")

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Set task dependencies
    task_extract >> task_transform >> task_load
