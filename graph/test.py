from util.helper import initalizeGraphSpark,loadFile
from pyspark.sql.functions import *
from graphframes import *
import pandas as pd

# Paths
test_txs_path="/mnt/indexer-build/migrated_data/stage/user_txs"
accounts_path="/mnt/indexer-build/migrated_data/raw/rest_Accounts"
destination_path="/mnt/indexer-build/migrated_data/stage/SSC"

# Variables
spark = initalizeGraphSpark("GraphTest")
v1_cols=["Id","Balance"]
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["src","dst","relationship"]


def columnMerger(YEAR,MONTH):
    if (YEAR != "") & (MONTH != ""):
        return str(YEAR)+"_"+str(MONTH)
    else:
        return "0"

def add(row):
    merged_row = columnMerger(row[0],row[1])
    print("Merged Row :"+merged_row)
    # Create a GraphFrame
    e2 = e1.filter(e1.relationship == merged_row)
    g = GraphFrame(v1,e2).dropIsolatedVertices()

    result = g.stronglyConnectedComponents(maxIter=10)
    final = result.select("id", "component").groupBy("component").agg(countDistinct(result["id"]).alias("count"))
    next = final.agg(max("count").alias("max_count")).collect()
    return next[0].__getitem__("max_count")

udf_columnMerger = udf(lambda x,y:columnMerger(x,y), StringType() )

df_test_txs = loadFile(spark, test_txs_path, True )
df_accounts = loadFile(spark, accounts_path, True )

# Vertex DataFrame
v1 = df_accounts.select(v1_cols).withColumnRenamed("Id","id")
e1 = df_test_txs.select(e1_cols).withColumnRenamed("SenderId","src") \
    .withColumnRenamed("TargetId","dst") \
    .withColumn("relationship",udf_columnMerger(col("Year_no"),col("Week_no"))).select(e1_final)

df = df_test_txs.select("Year_no","Week_no").groupBy("Year_no","Week_no").count().select("Year_no","Week_no")
df_pandas = df.toPandas()
df_pandas['max_SSC'] = df_pandas.apply(add, axis=1)

spark_df = spark.createDataFrame(df_pandas)
# spark_df.show()
spark_df.write.option("header", True).mode('overwrite').csv(destination_path)