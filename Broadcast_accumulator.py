from pyspark import SparkContext
from collections import namedtuple
file = r"D:\深圳培训\spark\实验\20171204\NDAQ.csv"
sc = SparkContext("local","Simple App")
Record = namedtuple("Record", ["date", "open", "high", "low", "close", "adj_close", "volume"])
def parse_record(s):
    fields = s.split(",")
    return Record(fields[0], *map(float, fields[1:6]), int(fields[6]))
parsed_data = sc.textFile(file).map(parse_record).cache()
print(parsed_data)
import time
import random
def super_regressor(v):
    time.sleep(random.random()/1000.0)
    return 0.5 * ((v - 1910949928.057554) / 284610509.115) ** 2.0
time_spent = sc.accumulator(0.0)
#print(time_spent)
def timed_super_regressor(v):
    before = time.time()
    result = super_regressor(v)
    after = time.time()
    time_spent.add(after - before)
    return result
#estimates = parsed_data.map(lambda r: timed_super_regressor(r.volume)).collect()
#print(estimates)
#print(time_spent.value)
#estimates = parsed_data.map(lambda r: timed_super_regressor(r.volume)).collect()
#print(estimates)
#print(time_spent.value)
from pyspark import AccumulatorParam
class MaxAccumulatorParam(AccumulatorParam):
    def zero(self, initial_value):
        return initial_value
    def addInPlace(self, accumulator, delta):
        return max(accumulator, delta)
time_persist = sc.accumulator(0.0, MaxAccumulatorParam())
def persist_to_external_storage(iterable):
    for record in iterable:
        before = time.time()
        time.sleep(random.random() / 1000.0)  # --party-- persist hard
        after = time.time()
        time_persist.add(after - before)
parsed_data.foreachPartition(persist_to_external_storage)
print(time_persist.value)

parsed_data = sc.textFile(file).map(parse_record).cache()
params = sc.broadcast({"mu": 1910949928.057554, "sigma": 284610509.115})
def super_regressor1(v):
    time.sleep(random.random()/ 1000.0)
    return 0.5 * ((v - params.value["mu"]) / params.value["sigma"]) ** 2.0
print(parsed_data.map(lambda x: super_regressor1(x.volume)).top(1))