from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def transform(spark):
    orders = spark.read.parquet("/data/tmp/orders")
    orders_per_country = orders.groupBy("ShipCountry") \
                                  .agg(count("*").alias("OrderCount"))
    orders_per_country.printSchema()
    orders_per_country.write.mode("overwrite").parquet("/data/tmp/orders_per_country")

def main():
    spark = SparkSession.builder \
        .appName("NorthWindSales Extract Job") \
        .config("spark.jars", "/data/extras/postgresql-42.7.3.jar") \
        .getOrCreate()

    transform(spark)

    spark.stop()

if __name__ == "__main__":
    main()

