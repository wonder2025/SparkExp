from pyspark.sql import SparkSession
import os
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
motifs = g.find("(A)-[]->(B); (B)-[]->(C)").filter("A.id != C.id")
motifs.show()
AC = motifs.selectExpr("A.id as A", "C.id as C")
AC.show()
AC.groupBy("A", "C").count().orderBy("A").show()