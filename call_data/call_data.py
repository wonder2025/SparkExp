"""
话单数3260928
主叫用户数359469
被叫用户数417128
"""
import plotly.offline as offline
from plotly.graph_objs import Scatter, Layout


from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlctx = SQLContext(sc)

dataframe_mysql = sqlctx.read.load("D:/深圳培训/spark/call_data/call_data.csv", format="csv",header="true")
# dataframe_mysql = sqlctx.read.format("jdbc").options(
#         url="jdbc:mysql://localhost:3306/call_data",
#         driver="com.mysql.jdbc.Driver",
#         dbtable="call_data",
#         user="root",
#         password="123456").load()
#统计用户拨打电话量
res1=dataframe_mysql.groupby("CALLING_NBR").count().orderBy("CALLING_NBR")
res1.show()
# con=res1.filter("cout>100")
# sqlctx.sql("SELECT * FROM dataframe_mysql")
offline.plot({
    "data": [Scatter(x=res1.toPandas()['CALLING_NBR'], y=res1.toPandas()['count'])],
    "layout": Layout(title="统计用户拨打电话量")},
     filename="calling.html")

res2=dataframe_mysql.groupby("CALLED_NBR").count().orderBy("CALLED_NBR")
#统计用户接听电话量
offline.plot({
    "data": [Scatter(x=res2.toPandas()['CALLED_NBR'], y=res2.toPandas()['count'])],
    "layout": Layout(title="统计用户接听电话量")},
     filename="called.html")
res1.createOrReplaceTempView("res1")
maxdegree=sqlctx.sql("SELECT max(count) FROM res1").show()
i=0
xd=[]
yd=[]
while i<3442:
        condition="count>"+str(i)
        res=res1.filter(condition).count()
        yd.append(res)
        xd.append(i)
        i=i+10

# import matplotlib.pyplot as plt
# plt.plot(xd, yd, 'ro')
# plt.show()
offline.plot({
    "data": [Scatter(x=xd, y=yd)],
    "layout": Layout(title="")},
     filename="cal.html")

x=[]
calling=trnagles.select("A.id").toPandas()

len=calling.count()
i=0
score=[]
while i<10000:
    con="A.id = "+calling.get_value(i,'id')
    x=trnagles.filter(con).count()
    subV=trnagles.filter(con).select("B.id").distinct()
    subV.show()

    subG = GraphFrame(subV,edges)
    countPath = subG.find("(A)-[]->(B)").count()
    if(countPath==0):
        score.append([con,inf])
    else:
        score.append([con,x/countPath*1.0])
    i=i+1