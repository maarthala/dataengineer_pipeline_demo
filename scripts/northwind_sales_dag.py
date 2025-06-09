from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='northwind_sales_pipeline',
    default_args=default_args,
    description='A simple DAG to test Airflow setup',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    
    start = DummyOperator(task_id="start")

    extract = SparkSubmitOperator(
        task_id="extract_sales_data",
        application="/data/airflow/etl/extract.py",
        name="ExtractNorthWindSalesData"
    )

    transform = SparkSubmitOperator(
        task_id="transform_sales_data",
        application="/data/airflow/etl/transform.py",
        name="TransformNorthWindSalesData"
    )

    load = SparkSubmitOperator(
        task_id="load_sales_data",
        application="/data/airflow/etl/load.py",
        name="LoadNorthWindSalesData"
    )


    end = DummyOperator(task_id="end")

    start >> extract >> transform >> load >> end
