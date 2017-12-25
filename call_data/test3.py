from pyspark import SparkContext
from collections import namedtuple
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
from pyspark.sql import SparkSession
file = r"D:\深圳培训\spark\call_data\call_data_test.csv"
sparkSession=SparkSession.builder.master('yarn').config('spark.yarn.historyServer.address', 'http://Medusa001:18089 ')\
.config('spark.eventLog.dir', 'hdfs://hdfs-production/user/spark/spark2ApplicationHistory')\
.config('spark.yarn.executor.memoryOverhead', '2g').config('spark.eventLog.enabled', 'true').config("spark.some.config.option", "some-value")\
.config('spark.yarn.am .memory', '1g').config('spark.executor.instances', '3')\
.appName('scdx03TestSpark')\
.enableHiveSupport().getOrCreate()
Record = namedtuple("Record", ["SERV_ID", "CALLING_NBR", "CALLED_NBR", "START_DATE",
                               "MSC", "LAC", "CELL_ID"])
def parse_record(s):
    fields = s.split(",")
    return Record(fields[0], *map(float, fields[1:6]), int(fields[6]))
parsed_data = sc.textFile(file).map(parse_record).cache()
data=sparkSession.sparkContext.textFile(file, minPartitions=None,  use_unicode=False)