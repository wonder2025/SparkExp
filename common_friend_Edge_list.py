"""
计算共同朋友的数量Edge list
"""
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.enableHiveSupport().master("local").getOrCreate()
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
edgeList = [(1, 2), (1, 3), (1, 4), (2, 3), (2, 5), (3, 4), (3, 5), (3, 6), (3, 7),
            (2,1), (3,1), (4,1), (3,2), (5,2), (4,3), (5,3), (6,3), (7,3)]
graphData = sparkSession.sparkContext.parallelize(edgeList).map(lambda src_dst:Row(src_dst[0], src_dst[1]))
graphSchemaAB = StructType([StructField('A', IntegerType(), nullable=False),StructField('B', StringType(), nullable=False)])
abDF = sparkSession.createDataFrame(graphData, graphSchemaAB)
graphSchemaBCl = StructType([StructField( 'B', IntegerType(), nullable=False),StructField('C', StringType(), nullable=False)])
bcDF = sparkSession.createDataFrame(graphData, graphSchemaBCl)

abDF.show()
bcDF.show()
joinDF = abDF.join(bcDF, abDF.B == bcDF.B)
joinDF.show()
joinDF.drop("B").groupBy("A", "C").count().filter("A =1").show()