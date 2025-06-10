from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
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
        # conf=["--master", "spark://spark-master:7077"]
        verbose=True
    )

    # transform = SparkSubmitOperator(
    #     task_id="transform_sales_data",
    #     application="/data/etl/transform.py",
    #     name="TransformNorthWindSalesData",
    #     # application_args=["--master", "spark://spark-master:7077"], 
    #     conn_id='spark_default',
    #     verbose=True
    # )

    # load = SparkSubmitOperator(
    #     task_id="load_sales_data",
    #     application="/data/etl/load.py",
    #     name="LoadNorthWindSalesData",
    #     # application_args=["--master", "spark://spark-master:7077"],
    #     conn_id='spark_default',
    #     verbose=True
    # )


    end = EmptyOperator(task_id="end")

    start >> extract  >> end

    # start >> extract >> transform >> load >> end
