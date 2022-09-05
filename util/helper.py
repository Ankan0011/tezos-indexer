
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from datetime import datetime

def initalizeSpark(AppName):
        print("Importing the Spark Initiallization")
        builder = SparkSession.builder.appName(AppName) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.jars", "//mnt/indexer-build/jar/postgresql-42.5.0.jar") \
                .config("spark.executor.memory", "12g") \
                .config("spark.executor.cores", "3") \
                .config('spark.cores.max', '6') \
                .config('spark.driver.memory','4g') \
                .config('spark.worker.cleanup.enabled', 'true') \
                .config('spark.worker.cleanup.interval', '60') \
                .config('spark.shuffle.service.db.enabled', 'true') \
                .config('spark.worker.cleanup.appDataTtl', '60') \
                .config('spark.sql.debug.maxToStringFields', 100)

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

def loadFile(spark, path, header):
        print("Loading the csv files")
        df = spark.read.option("header", header).option("inferSchema", "true") \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true") \
                .csv(path+"/*.csv")
        return df

#To Parse the Date timestamp to Date Object
def dateParser(Timestamp):    
    mydate = datetime.strptime(Timestamp, "%Y-%m-%d %H:%M:%S")
    return mydate
