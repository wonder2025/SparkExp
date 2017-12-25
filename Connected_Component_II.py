
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from graphframes import *
def run(graph, broadcastThreshold):
    g = prepare(graph)
    vv = g.vertices
    ee = g.edges.persist(StorageLevel.MEMORY_AND_DISK)
def prepare(graph):
    vertices = graph.vertices.indexedVertices .select(col("LONG_ID").as("ID"), col("ATTR"))
    edges = graph.indexedEdges.select(col("LONG_SRC").as("SRC"), col("LONG_DST").as("DST"))

    orderedEdges = {
        edges.flter("src!= dst").select(minValue(col("src"), col("dst")).as("src"),
        maxValue(col("src"), col("dst")).as("dst")).distinct()
    }

    GraphFrame(vertices, orderedEdges)

converged = False
iteration = 1
prevSum = None
while (!converged):
    # large-star step
    # compute min neighbors including self
    minNbrs1 = minNbrs(ee)
    .withColumn("min_nbr", minValue(col("src"), col("min_nbr")).as("min_nbr"))
    .persist(StorageLevel.MEMORY_AND_DISK)

    # connect all strictly larger neighbors to the min neighbor (including self)
    ee = skewedJoin(ee, minNbrs1, broadcastThreshold)
    .select(col("dst").as("src"), col("min_nbr").as("dst")) # src > dst
    .distinct()
    .persist(StorageLevel.MEMORY_AND_DISK)

def minNbrs(edges):
    symmetrized_edges = ee.unionAll(ee.select(col("dst").as("src"),
    col("src").as("dst")))

    symmetrized_edges.groupBy("src").agg(min(col("dst")).as("min_nbr"),
    count("*").as("cnt"))
def skewedJoin(edges, minNbrs, broadcastThreshold):
    hubs = {
    minNbrs.flter(col("cnt") > broadcastThreshold)
    .select("src")
    .as[Long]
    .collect()
    .toSet
    }
    GraphFrame.skewedJoin(edges, minNbrs, "src", hubs)

