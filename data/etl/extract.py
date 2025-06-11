from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def extract(spark, jdbc_url, props):
    tables = ['orders', 'order_details', 'products', 'categories']
    for table in tables:
        df = spark.read.jdbc(jdbc_url, table, properties=props)
        df.write.mode("overwrite").parquet(f"/data/tmp/{table}")

def main():
    spark = SparkSession.builder \
        .appName("NorthWindSales Extract Job") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/northwind"

    props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    extract(spark, jdbc_url, props)

    spark.stop()

if __name__ == "__main__":
    main()

