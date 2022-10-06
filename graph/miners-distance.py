from itertools import count
from pyspark.sql.functions import lit, col
from pyspark.sql import Row
from util.helper import initalizeGraphSpark,loadFile
from graphframes import *
import os
import pandas as pd
import networkx as nx
import numpy as np
from collections import Counter

# Paths
test_txs_path="/mnt/indexer-build/migrated_data/stage/all_txs"
rest_accounts="/mnt/indexer-build/migrated_data/raw/rest_Accounts"
delegator_path="/mnt/indexer-build/migrated_data/raw/delegate_Accounts"
destination_path="/mnt/indexer-build/migrated_data/curated/gini"

# Variables
spark = initalizeGraphSpark("Miners-Distance")
e1_cols=["SenderId","TargetId","Year_no","Week_no","Amount"]
e1_final=["src","dst","relationship"]

listDesDir =[x[0].split("/")[-1] for x in os.walk(destination_path)]

listSrcDir  = [x[0] for x in os.walk(test_txs_path)]


df_new = loadFile(spark, delegator_path, True ).select("Id").withColumnRenamed("Id", "id").distinct()
df_rest = loadFile(spark, rest_accounts, True ).select("Id").withColumnRenamed("Id", "id").distinct()

# print("Count delegator :"+str(df_new.count()))
all_acc = df_new.unionAll(df_rest)
# print("Count all account :"+str(all_acc.count()))
# df_account = df_new.groupBy("Id").count().select("Id")

for x in listSrcDir:
    if x.find("date=201941")  != -1:
        dirname = x.split("/")[-1]
        if not (dirname in listDesDir):
            print("Not Found it :"+dirname)
            df_file = loadFile(spark, x, True ).select(e1_cols)
            e1 = df_file.filter(df_file['Amount'] != 0).filter(col("SenderId").isNotNull()) \
                .filter(col("TargetId").isNotNull())

            delegetor = df_new.join(e1, e1.SenderId == df_new.id, "inner").select("id").distinct()
            # data_array = (delegetor.select("id").collect())
            data_array = [ str(row.id) for row in delegetor.select("id").collect()]

            e2 = e1.groupBy("SenderId","TargetId").count().withColumn("relationship",lit('txs')).withColumnRenamed("SenderId", "src") \
                .withColumnRenamed("TargetId", "dst").select(e1_final)

            g = GraphFrame(all_acc, e2).dropIsolatedVertices()
            results = g.shortestPaths(landmarks=data_array)
            results.select("id","distances").show(5)
           
            # Person=Row( "year_week","gini_coff")
            # data = [ Person(str(dirname.split("=")[-1]), str(gini(df_pandas.to_numpy()))) ]
            # next = spark.createDataFrame(data)
            # next.show()
            # next.write.option("header", True).mode('overwrite').csv(destination_path+"/"+dirname)