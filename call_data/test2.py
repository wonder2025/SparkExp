"""
话单数3260928
主叫用户数359469
被叫用户数417128
"""


from pyspark import SparkContext
from pyspark.sql import SQLContext
import os
from pyspark.sql import SparkSession
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
sc = SparkContext()
sqlctx = SQLContext(sc)
dataframe_mysql = sqlctx.read.load("D:/深圳培训/spark/call_data/call_data_test.csv", format="csv",header="true")
dataframe_mysql.show()
# rdd = dataframe_mysql.rdd\
#     .map(lambda row:[row])
# rdd.take(1)
sparkSession = SparkSession.builder.getOrCreate()
rdd = dataframe_mysql.rdd\
    .flatMap(lambda s: [s.CALLING_NBR,s.CALLED_NBR]).distinct()\
    .map(lambda row:[row])
vertices = sparkSession.createDataFrame(rdd,["id"])
rdd2 = dataframe_mysql.rdd\
    .map(lambda row:[row.CALLING_NBR,row.CALLED_NBR])
edges = sparkSession.createDataFrame(rdd2,["src","dst"])
from graphframes import *
g = GraphFrame(vertices,edges)
#filter("A.id <B.id").filter("B.id <C.id") 去除重复三角形
trnagles = g.find("(A)-[]->(B)")
print(trnagles.count())
j = 0
score = []
rawScore = []
rawNBR = []
calling = trnagles.select("A.id").toPandas()
len = calling.count()
threshold = 10
j=0
score=[]
rawScore=[]
#取出所有的calling并去重
calling=trnagles.select("A.id").distinct().toPandas()
calling
len=calling.count()
threshold=10
while j<int(len):
    #取出calling对应的每个值
    value=calling.get_value(j,'id')
    con="A.id = "+value
    #过滤得到该CALLING_NBR对应的话单
    filterData=trnagles.filter(con)
    #计算这个CALLING_NBR的出度
    x=filterData.count()
    #得到CALLING_NBR呼出的CALED_NBR，即该CALLING_NBR的朋友们，并且去重
    subV=filterData.select("B.id").distinct()
    #得到一个新的graph
    subG = GraphFrame(subV,edges)
    #朋友们的联系数目
    countPath = subG.find("(A)-[]->(B)").count()
    if(countPath==0):
        score.append([value,"inf"])
        continue
    else:
        val=x/countPath*1.0
        rawScore.append([value,val])
        if val<threshold:
            score.append(value)
    j=j+1