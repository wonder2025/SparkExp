f = open("E:\SparkExp\call_score_delete.txt")             # 返回一个文件对象
line = f.readline()             # 调用文件的 readline()方法
raw_data=line[1:len(line)-2]
map_list=raw_data.split(",")
f1 = open("E:\SparkExp\call_score_st.txt")             # 返回一个文件对象
line2 = f1.readline()
match=line2[0:len(line)-2]
map=match.split("|")
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.getOrCreate()
sparkSession.sparkContext.setCheckpointDir(".")
vertices = sparkSession.createDataFrame(map_list,["id"])
edges = sparkSession.createDataFrame(map,["src", "dst" ])

from graphframes import *
g = GraphFrame(vertices, edges)

result = g.connectedComponents()
result.show()
result.select("id", "component").orderBy("component").show()
