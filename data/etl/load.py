from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def load(spark, jdbc_url, props):

    orders_per_country = spark.read.parquet("/data/tmp/orders_per_country")

    orders_per_country.write.jdbc(
        url=jdbc_url,
        table="orders_per_country",
        mode="overwrite",
        properties=props
    )

def main():
    spark = SparkSession.builder \
        .appName("NorthWindSales Extract Job") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/northwind"

    props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    load(spark, jdbc_url, props)

    spark.stop()

if __name__ == "__main__":
    main()

