docker compose up airflow-init

# Setup
echo -e "AIRFLOW_UID=$(id -u)" > .env


docker-compose up



## Spark
version: spark:4.0.0-python3
http://localhost:8080/ 



## Airflow
version: apache/airflow:3.0.1-python3.10
http://localhost:8081/
credentials: airflow/airflow


## Spark Connector
docker exec -it sandbox-airflow-apiserver-1 airflow connections add 'spark_default' --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port 7077



# To Remove Dockers
docker compose down --volumes --rmi all
docker compose down --volumes --remove-orphans



# Others

https://airflow.apache.org/docs/docker-stack/build.html

spark-submit --master spark://spark-master:7077 --name ExtractNorthWindSalesData --verbose --deploy-mode client /data/etl/extract.py
