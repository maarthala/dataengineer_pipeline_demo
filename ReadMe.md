# Introduction

The primary objective of this repo is to demonstrate a data engineering pipeline. In this demo, we use the Northwind database's order details.

## Use case
The business requires a dashboard that shows total orders by country. The dashboard should be updated every hour.

## Solution
A Grafana dashboard is provided to the business, displaying Total Orders by Country using a bar gauge graph.
The dashboard pulls data from a PostgreSQL table named orders_per_country. This involves:

- Extracting data from the Orders table

- Aggregating it by country

- Loading the results into the orders_per_country table

The pipeline is orchestrated using Apache Airflow, and computation is performed on a Spark Cluster.



# Setup
From the terminal, run the following commands:
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up
```

## Spark
version: `spark:4.0.0-python3`

URL: http://localhost:8080/ 


## Airflow
- version: `apache/airflow:3.0.1-python3.10`
- URL: http://localhost:8081/
- credentials: `airflow/airflow`


## Spark Connector
Create a Spark connection in Airflow with the following command:

```
docker exec -it sandbox-airflow-apiserver-1 airflow connections add 'spark_default' --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port 7077
```

## Run Pipeline

Login to Airflow and trigger the DAG `northwind_country_sales_pipeline` .

Airflow uses the Spark cluster to execute the ETL tasks.


## Grafana Dashboard

Access the Grafana dashboard using the link below:

http://localhost:3000/d/feolylok314hsd/country-sales?orgId=1&from=now-5m&to=now&timezone=browser&refresh=10s

Credentials: admin / admin


## Add more order
To add more orders to the database, run:

```
 python scripts/orders.py
```

# Clean Up

To clean up the Docker setup:

```
docker compose down --volumes  all
```




