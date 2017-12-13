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


dataframe_mysql = sqlctx.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/call_data",
        driver="com.mysql.jdbc.Driver",
        dbtable="call_data",
        user="root",
        password="123456").load()
#统计用户拨打电话量
res1=dataframe_mysql.groupby("CALLING_NBR").count().orderBy("CALLING_NBR")
res1.show()
# offline.plot({
#     "data": [Scatter(x=res1.toPandas()['CALLING_NBR'], y=res1.toPandas()['count'])],
#     "layout": Layout(title="统计用户拨打电话量")},
#      filename="calling.html")
#
# res2=dataframe_mysql.groupby("CALLED_NBR").count().orderBy("CALLED_NBR")
# #统计用户接听电话量
# offline.plot({
#     "data": [Scatter(x=res2.toPandas()['CALLED_NBR'], y=res2.toPandas()['count'])],
#     "layout": Layout(title="统计用户接听电话量")},
#      filename="called.html")