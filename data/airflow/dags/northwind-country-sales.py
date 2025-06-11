from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='northwind_country_sales_pipeline',
    default_args=default_args,
    description='Generate NorthWind sales data by country',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    
    start = EmptyOperator(task_id="start")

    extract = SparkSubmitOperator(
        task_id="extract_sales_data",
        application="/data/etl/extract.py",
        name="ExtractNorthWindSalesData",
        jars="/data/extras/postgresql-42.7.3.jar",
        verbose=True
    )

    transform = SparkSubmitOperator(
        task_id="transform_sales_data",
        application="/data/etl/transform.py",
        name="CalculateNorthWindSalesData",
        jars="/data/extras/postgresql-42.7.3.jar",
        verbose=True
    )

    load = SparkSubmitOperator(
        task_id="load_sales_data",
        application="/data/etl/load.py",
        name="LoadNorthWindSalesData",
        jars="/data/extras/postgresql-42.7.3.jar",
        verbose=True
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> transform >> load  >> end


