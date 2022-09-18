from util.helper import initalizeGraphSpark,loadFile
from pyspark.sql.functions import *
from graphframes import *

test_txs_path="/mnt/indexer-build/migrated_data/raw/test_txs-1"
accounts_path="/mnt/indexer-build/migrated_data/raw/rest_Accounts"
spark = initalizeGraphSpark("GraphTest")
v1_cols=["Id","Balance"]
e1_cols=["SenderId","TargetId"]

df_test_txs = loadFile(spark, test_txs_path, True )
df_accounts = loadFile(spark, accounts_path, True )


df_test_txs.groupBy("")


# Vertex DataFrame
v1 = df_accounts.select(v1_cols).withColumnRenamed("Id","id")
e1 = df_test_txs.select(e1_cols).withColumnRenamed("SenderId","src") \
    .withColumnRenamed("TargetId","dst") \
    .withColumn("relationship",lit("transfer"))

# Create a GraphFrame
g = GraphFrame(v1,e1)

# Query: Get in-degree of each vertex.
result = g.stronglyConnectedComponents(maxIter=10)
result.show()
# result.select("SenderId", "component").orderBy("component").show()

# # Vertex DataFrame
# v = spark.createDataFrame([
#   ("a", "Alice", 34),
#   ("b", "Bob", 36),
#   ("c", "Charlie", 30),
#   ("d", "David", 29),
#   ("e", "Esther", 32),
#   ("f", "Fanny", 36),
#   ("g", "Gabby", 60)
# ], ["id", "name", "age"])

# # Edge DataFrame
# e = spark.createDataFrame([
#   ("a", "b", "friend"),
#   ("b", "c", "follow"),
#   ("c", "b", "follow"),
#   ("f", "c", "follow"),
#   ("e", "f", "follow"),
#   ("e", "d", "friend"),
#   ("d", "a", "friend"),
#   ("a", "e", "friend")
# ], ["src", "dst", "relationship"])

# v1 = v.filter(v.age <= 34)