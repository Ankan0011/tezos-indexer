from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder.appName("app_name") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()


