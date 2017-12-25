f = open("E:\SparkExp\call_score.txt")             # 返回一个文件对象
line = f.readline()             # 调用文件的 readline()方法
nbr=line.split("||")[0]
nbrs=nbr.split(",")
print(len(nbrs))
score=line.split("||")[1]
scores=score.split(",")
scores2=[float(item) for item in scores[0:len(scores)-2]]
nbrs2=[item.split('"')[1] for item in nbrs[0:len(nbrs)-2]]
print("max:"+str(max(scores2)))#max:2654.0
print("min:"+str(min(scores2)))#min:0.1443850267379679
print("mean:"+str(sum(scores2)/len(scores2)))#mean:7.921515125153732
print(type(nbrs2))

# import matplotlib.pyplot as plt
# plt.plot(nbrs2, scores2, 'r.')
# plt.show()

# map=line.split("||")[2]

# print(map)
import plotly.offline as offline
import plotly.graph_objs as go

data = [go.Bar(
            x=nbrs2,
            y=scores2
    )]

offline.plot(data, filename='call_score.html')

from collections import Counter
print(Counter(scores2))