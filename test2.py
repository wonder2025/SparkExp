# from pixiedust.packageManager import PackageManager

# pkg = PackageManager()
# pkg.installPackage("graphframes:graphframes:0")
# pkg.printAllPackages()
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
sc = SparkContext()
sqlContext=SQLContext(sc)


#import the display module
from pixiedust.display import *
#import the Graphs example
from graphframes.examples import Graphs
#create the friends example graph
g=Graphs(sqlContext).friends()
#use the pixiedust display
display(g)