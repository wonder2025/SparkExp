from pyspark import SparkContext
sc = SparkContext("local","Simple App")
#设置debug模式
# sc.setLogLevel("debug")
a=sc.parallelize([1,2,3,4,5,6,7,8,9,10,11],3)
# print(a.getNumPartitions())
# print(a.collect())
b=a.map(lambda x:(print(x),2*x)[1])
# b=a.map(lambda x:2*x)
print(b)
print(b.collect())