from pyspark import SparkContext
from collections import namedtuple
import os
os.environ["PYTHONHASHSEED"]=("0")
file = r"D:\深圳培训\spark\实验\20171204\NDAQ.csv"
sc = SparkContext("local","Simple App")
Record = namedtuple("Record", ["date", "open", "high", "low", "close", "adj_close", "volume"])
def parse_record(s):
    fields = s.split(",")
    return Record(fields[0], *map(float, fields[1:6]), int(fields[6]))
parsed_data = sc.textFile(file).map(parse_record).cache()
print(parsed_data.take(2))
#形成今天日期和收盘价的key、value
date_and_close_price = parsed_data.map(lambda r: (r.date, r.close))
print(date_and_close_price.take(4))
from datetime import datetime, timedelta
#计算明天的日期
def get_next_date(s):
    fmt = "%Y/%m/%d"
    return (datetime.strptime(s, fmt) + timedelta(days=1)).strftime(fmt)
# print(get_next_date("2017/12/31"))
from datetime import datetime, timedelta
#获取明天日期与今天的收盘价的key，value
date_and_prev_close_price = parsed_data.map(lambda r: (get_next_date(r.date), r.close))
print(date_and_prev_close_price.take(3))
#根据日期进行连接('2010/11/18', 21.83)+('2010/11/18', 21.25)(其实是2010/11/17的收盘价)=('2010/11/18', (21.83, 21.25))
joined = date_and_close_price.join(date_and_prev_close_price)
print(joined.take(3))
#寻找key是2010/11/18所对应的值
print(joined.lookup("2010/11/18"))
#计算涨跌幅度
returns = joined.mapValues(lambda p: (p[0] / p[1] - 1.0) * 100.0)
print(returns.take(3))
print(returns.lookup('2010/11/18'))
#左连接
joined_left = date_and_close_price.leftOuterJoin(date_and_prev_close_price)
#[66.68]
print(date_and_close_price.lookup('2017/1/3'))
#[]
print(date_and_close_price.lookup('2017/1/2'))
#[(66.68, None)] 2017/1/2放假无数据
print(joined_left.lookup('2017/1/3'))
#[] date_and_prev_close_price里面没有2017/1/3，也就说原数据没有2017/1/2
print(date_and_prev_close_price.lookup("2017/1/3"))

joined_right = date_and_close_price.rightOuterJoin(date_and_prev_close_price)
#右连接(2017/1/3,66.68)+[]=[]
print(joined.lookup("2017/1/3"))
#全连接
joined_full = date_and_close_price.fullOuterJoin(date_and_prev_close_price)
#[(66.68, None)] 不管左右是否能找到匹配的键都会保留
print(joined_full.lookup("2017/1/3"))
