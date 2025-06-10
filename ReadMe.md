docker compose up airflow-init


# Spark
http://localhost:9000/ 

# Airflow
http://localhost:9001/


version: apache/airflow:2.9.0-python3.12
admin/admin - airflow

docker-compose up airflow-init
docker-compose up


https://airflow.apache.org/docs/docker-stack/build.html

spark-submit --master spark://spark-master:7077 --name ExtractNorthWindSalesData --verbose --deploy-mode client /data/etl/extract.py