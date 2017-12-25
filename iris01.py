from displayfunction import display
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes

from pyspark.ml.feature import StringIndexer, VectorAssembler

from pyspark.sql import SQLContext, Row
from wheel.signatures.djbec import double

sc = SparkContext("local","scdx08 App")

file = "D://深圳培训//spark//   iris.data"
lines = sc.textFile(file)

line1 = lines.map(lambda x: x.split(","))
print(line1.take(5))

line2 = line1.map(lambda p: Row(SepalLength=float(p[0]), SepalWidth=float(p[1]),PetalLength=float(p[2]), PetalWidth=float(p[3]), Species=p[4]))
print(line2.take(5))
sqlContext = SQLContext(sc)
iris=sqlContext.createDataFrame(line2)


iris.registerTempTable("iris")
irisdf = sqlContext.sql("SELECT SepalLength, SepalWidth, PetalLength, PetalWidth, Species FROM iris")
display(irisdf)
irisdf.printSchema()
# print(irisdf.dtypes)

# Convert target into numerical categories

labelIndexer = StringIndexer(inputCol="Species", outputCol="label")
display(irisdf.select("SepalLength", "Species"))
print(irisdf.take(2))
irisdf.printSchema()
print(irisdf.dtypes)

# Split dataset randomly into Training and Test sets. Set seed for reproducibility
# (trainingData, testData) = irisdf.randomSplit([0.7, 0.3], seed = 100)
#
# trainingData.cache()
# testData.cache()
#
# print (trainingData.count())
# print (testData.count())

# Split the data into train and test
splits = irisdf.randomSplit([0.6, 0.4], 1234)
train = splits[0]
test = splits[1]
print(train.count())
