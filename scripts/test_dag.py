from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("✅ Airflow is working! The current datetime is:", datetime.now())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_airflow_dag',
    default_args=default_args,
    description='A simple DAG to test Airflow setup',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    test_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    test_task
