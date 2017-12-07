
from pyspark.sql import SparkSession
# import os
# os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
from pyspark.sql.functions import explode, array

sparkSession = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
edgeList = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 5), (3, 4), (3, 5), (3, 6), (3, 7)]
graphData = sparkSession.sparkContext.parallelize(edgeList).map(lambda src_dst:Row(src_dst[0], src_dst[1]))
graphSchemaAB = StructType([StructField('A', IntegerType(), nullable=False),StructField('B', StringType(), nullable=False)])
ab = sparkSession.createDataFrame(graphData, graphSchemaAB)
ab.show()
graphSchemaBCl = StructType([StructField( 'B', IntegerType(), nullable=False),StructField('C1', StringType(), nullable=False)])
bc1 = sparkSession.createDataFrame(graphData, graphSchemaBCl)
graphSchemaBC2 = StructType([StructField( 'A', IntegerType(), nullable=False),StructField('C2', StringType(), nullable=False)])
ac2 = sparkSession.createDataFrame(graphData, graphSchemaBC2)
#不是rdd里面的join，是spark sql 里面的jion。
# 根据B进行连接（the column(s) must exist on both sides）
abc1=ab.join(bc1,"B")
abc1.show()
abc1c2=abc1.join(ac2,"A")
abc1c2.show()
#得出形成三角形的几条边,图中有三个三角形
"""
+---+---+---+---+
|  A|  B| C1| C2|
+---+---+---+---+
|  1|  3|  4|  4|
|  1|  2|  3|  3|
|  2|  3|  5|  5|
+---+---+---+---+
"""
tran=abc1c2.filter("C1=C2")
tran.show()
"""
+---+---+---+---+
|  A|  B| C1| C2|
+---+---+---+---+
|  1|  3|  4|  4|
|  1|  2|  3|  3|
|  2|  3|  5|  5|
+---+---+---+---+
-->
+---------------+
|trangleVertices|
+---------------+
|      [1, 3, 4]|
|      [1, 2, 3]|
|      [2, 3, 5]|
+---------------+
-->explode
+---------------+
|trangleVertices|
+---------------+
|              1|
|              3|
|              4|
|              1|
|              2|
|              3|
|              2|
|              3|
|              5|
+---------------+
-->groupBy count
+---------------+-----+
|trangleVertices|count|
+---------------+-----+
|              3|    3|
|              5|    1|
|              1|    2|
|              4|    1|
|              2|    2|
+---------------+-----+
计算一个点出现在三角形中的次数
"""
aa=tran.select(array("A","B","C1").alias("trangleVertices"))\
    .select(explode("trangleVertices").alias("trangleVertices"))\
    .groupBy("trangleVertices").count().show()


