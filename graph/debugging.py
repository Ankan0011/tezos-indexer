from pyspark.sql.functions import *
from util.helper import initalizeGraphSpark,loadFile
from graphframes import *
import os

# Paths
test_txs_path="/mnt/indexer-build/migrated_data/stage/SSC"
# test_txs_path="/mnt/indexer-build/migrated_data/raw/test_txs-1"
accounts_path="/mnt/indexer-build/migrated_data/raw/rest_Accounts"
destination_path="/mnt/indexer-build/migrated_data/curated/SSC"

# Variables
spark = initalizeGraphSpark("Debug")
v1_cols=["Id","Balance"]
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["src","dst","relationship"]

df_accounts = loadFile(spark, accounts_path, True )
v1 = df_accounts.select(v1_cols).withColumnRenamed("Id","id")

listDesDir =[x[0].split("/")[-1] for x in os.walk(destination_path)]

listSrcDir  = [x[0] for x in os.walk(test_txs_path)]

for x in listSrcDir:
    if x.find("relationship=202152")  != -1:
        dirname = x.split("/")[-1]
        #if not (dirname in listDesDir):
            #print("Not Found it :"+dirname)
        df_new = loadFile(spark, x, True )
        e1 = df_new.groupBy("src","dst").count().withColumn("relationship",lit('txs')).select(e1_final)
        try:
            g = GraphFrame(v1,e1).dropIsolatedVertices()
            result = g.stronglyConnectedComponents(maxIter=10)
            final = result.select("id", "component").groupBy("component").agg(countDistinct(result["id"]).alias("count"))
            next = final.agg(max("count").alias("max_count")).withColumn("time_week",lit(dirname.split("=")[-1])).withColumn("Total_Vertices",lit(g.vertices.count()))
            next.show()
            #next.write.option("header", True).mode('overwrite').csv(destination_path+"/"+dirname)
        except NameError:
            print("Exception")
            print(NameError)

spark.stop()