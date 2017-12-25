"""
[[2,3,4],                    <1, 3>       <(1, 2), 1>
[1,3,5],                     <1, 5>       <(1, 3), 2>
[1, 2, 4, 5, 6 ,7],  ------> <1, 2>---->  <(1, 4), 1>
[1, 3],                      <1, 4>       <(1, 5), 2>
[2, 3],                      <1, 5>       <(1, 6), 1>
[3],                         <1, 6>       <(1, 7), 1>
[3]]                         <1, 7>
                             <1, 3>  1，3的共同朋友出现了两次分别是1和7
邻接链表法计算共同朋友的个数
"""
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
#图的邻接链表表示法
graph = {1: [2,3,4],
         2: [3,1,5],
         3: [1,2,4,5,6,7],
         4: [1,3],
         5: [2,3],
         6: [3],
         7: [3]}
#比较大小,小的在前，大的在后
def comp(first,sencond,g):
    if first<sencond:
        return (first,sencond,g)
    else:
        return (sencond,first,g )

#生成键值对
def generator(v,vertexAll):
    for g in graph:
        if g==v:
            continue
        else:
            #数组中如果包含v才继续执行
            if v in graph[g]:
                #右边数组中的数的遍历
                for data in graph[g]:
                    if data!=v:
                        #传入3个值，加上本次遍历的行号g形成元组，防止重复计算，如果之后有同样的朋友关系则不再加入vertexAll中
                        data=comp(v, data,g)
                        if data not in vertexAll:
                            vertexAll.append(data)
    return vertexAll

vertexAll = []
def generatorAll ():
    for i in graph:
        generator(i,vertexAll)
    return vertexAll


# print(generatorAll())
#形成pair rdd
sparkSession = SparkSession.builder.enableHiveSupport().master("local").getOrCreate()
graphData = sparkSession.sparkContext.parallelize(generatorAll()).map(lambda src_dst:Row(src_dst[0], src_dst[1], src_dst[2]))
graphSchemaABC = StructType([StructField('A', IntegerType(), nullable=False),
                            StructField('B', IntegerType(), nullable=False),
                            StructField('C', IntegerType(), nullable=False)])
abc = sparkSession.createDataFrame(graphData, graphSchemaABC)
abc.drop("C").groupBy("A", "B").count().orderBy("A").show()

"""
+---+---+-----+
|  A|  B|count|
+---+---+-----+
|  1|  4|    1|
|  1|  7|    1|
|  1|  5|    2|
|  1|  3|    2|
|  1|  6|    1|
|  1|  2|    1|
|  2|  7|    1|
|  2|  4|    2|
|  2|  3|    2|
|  2|  5|    1|
|  2|  6|    1|
|  3|  4|    1|
|  3|  5|    1|
|  4|  5|    1|
|  4|  7|    1|
|  4|  6|    1|
|  5|  7|    1|
|  5|  6|    1|
|  6|  7|    1|
+---+---+-----+
"""

