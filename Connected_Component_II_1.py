import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.getOrCreate()
sparkSession.sparkContext.setCheckpointDir(".")
vertices = sparkSession.createDataFrame([
    ("1","Alex",28,"M","MIPT"),
    ("2","Emeli",28,"F","MIPT"),
    ("3","Natasha",27,"F","SPBSU"),
    ("4","Pavel",30,"M","MIPT"),
    ("5","Oleg",35,"M","MIPT"),
    ("6","Ivan",30,"M","MSU"),
    ("7","Ilya",29,"M","MSU")
    ],["id","name","age","gender","university"])
edges = sparkSession.createDataFrame([
    ("1","2","friend"),
    ("1","4","friend"),
    ("3","5","friend"),
    ("3","6","friend"),
    ("3","7","friend")],
    ["src", "dst" ,"relationship"])

from graphframes import *
g = GraphFrame(vertices, edges)

result = g.connectedComponents()
result.show()
result.select("id", "component").orderBy("component").show()

# ranks = g.pageRank(resetProbability=0.20, maxIter=5)
