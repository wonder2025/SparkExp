import os

from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
sparkSession = SparkSession.builder.getOrCreate()
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
    ("1","2","friend"),("2","1","friend"),
    ("1","3","friend"),("3","1","friend"),
    ("1","4","friend"),("4","1","friend"),
    ("2","3","friend"),("3","2","friend"),
    ("2","5","friend"),("5","2","friend"),
    ("3","4","friend"),("4","3","friend"),
    ("3","5","friend"),("5","3","friend"),
    ("3","6","friend"),("6","3","friend"),
    ("3","7","friend"),("7","3","friend"),
    ],["src","dst","relationship"])



from graphframes import *
g = GraphFrame(vertices,edges)
#filter("A.id <B.id").filter("B.id <C.id") 去除重复三角形
trnagles = g.find("(A)-[]->(B); (B)-[]->(C);(A)-[]->(C)").filter("A.id <B.id").filter("B.id <C.id")

trnagles.show()
"""
+-------------------+--------------------+--------------------+
|                  A|                   B|                   C|
+-------------------+--------------------+--------------------+
| [1,Alex,28,M,MIPT]|[3,Natasha,27,F,S...| [4,Pavel,30,M,MIPT]|
| [1,Alex,28,M,MIPT]| [2,Emeli,28,F,MIPT]|[3,Natasha,27,F,S...|
|[2,Emeli,28,F,MIPT]|[3,Natasha,27,F,S...|  [5,Oleg,35,M,MIPT]|
+-------------------+--------------------+--------------------+
"""
#统计一个顶点出现在几个三角形中
from pyspark.sql.functions import explode, array
vertexTriangles=trnagles.selectExpr("A.id as A", "B.id as B", "C.id as C") \
    .select(array("A","B","C").alias("id"))\
    .select(explode("id").alias("id"))\
    .groupBy("id").count()
print(type(vertexTriangles))
print(type(vertices))
"""

+---+-----+
| id|count|
+---+-----+
|  3|    3|
|  5|    1|
|  1|    2|
|  4|    1|
|  2|    2|
+---+-----+
"""
vertices.join(vertexTriangles, "id", "left_outer").show()
"""
+---+-------+---+------+----------+-----+
| id|   name|age|gender|university|count|
+---+-------+---+------+----------+-----+
|  7|   Ilya| 29|     M|       MSU| null|
|  3|Natasha| 27|     F|     SPBSU|    3|
|  5|   Oleg| 35|     M|      MIPT|    1|
|  6|   Ivan| 30|     M|       MSU| null|
|  1|   Alex| 28|     M|      MIPT|    2|
|  4|  Pavel| 30|     M|      MIPT|    1|
|  2|  Emeli| 28|     F|      MIPT|    2|
+---+-------+---+------+----------+-----+
"""