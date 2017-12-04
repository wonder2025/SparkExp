"""SimpleApp"""

from pyspark import SparkContext

logFile = r"D:\spark-2.2.0-bin-hadoop2.6\spark-2.2.0-bin-hadoop2.6\README.md "
sc = SparkContext("local","Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i"%(numAs, numBs))