from pyspark import SparkContext
from collections import namedtuple
from datetime import datetime, timedelta
file = r"D:\深圳培训\spark\实验\20171204\NDAQ.csv"
sc = SparkContext("local","Simple App")
Record = namedtuple("Record", ["date", "open", "high", "low", "close", "adj_close", "volume"])
def parse_record(s):
    fields = s.split(",")
    return Record(fields[0], *map(float, fields[1:6]), int(fields[6]))
parsed_data = sc.textFile(file).map(parse_record).cache()
#形成今天日期和收盘价的key、value
date_and_close_price = parsed_data.map(lambda r: (r.date, r.close))

#计算明天的日期
def get_next_date(s):
    fmt = "%Y/%m/%d"
    return (datetime.strptime(s, fmt) + timedelta(days=1)).strftime(fmt)
# print(get_next_date("2017/12/31"))
#获取明天日期与今天的收盘价的key，value
date_and_prev_close_price = parsed_data.map(lambda r: (get_next_date(r.date), r.close))
#根据日期进行连接('2010/11/18', 21.83)+('2010/11/18', 21.25)(其实是2010/11/17的收盘价)=('2010/11/18', (21.83, 21.25))
joined = date_and_close_price.join(date_and_prev_close_price)

#计算涨跌幅度
returns = joined.mapValues(lambda p: (p[0] / p[1] - 1.0) * 100.0)
#[('2010/11/18', 2.729411764705869), ('2010/11/19', 0.5955061841502518)]
print(returns.take(2))
#http://192.168.19.1:4040
print(sc.uiWebUrl)
# [('2015/10/22', 6.948968763987273)] top 求value最大的，即涨幅最大的
print(returns.top(1, lambda x: x[1]))