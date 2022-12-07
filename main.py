
from pprint import pprint
#from dotenv import dotenv_values

import matplotlib.pyplot as plt

import numpy as np
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, desc, asc, collect_list, col, size
from pyspark.sql.types import IntegerType
#from pyspark.ml.clustering import KMeans
#from pyspark.ml.evaluation import ClusteringEvaluator
#from pyspark.ml.feature import VectorAssembler
from datetime import timedelta

from pyspark_kmodes import *
import pyspark.sql.functions as pyf

import path

def nonemax(a, b):
    if (b is None):
        return a
    else:
        return max(a, b)


# TODO: Performance considerations considering the cores count
session = SparkSession.builder    \
    .master("local[*]")           \
    .getOrCreate()

# Reduce log level
session.sparkContext.setLogLevel("ERROR")
session.sparkContext.addPyFile("pyspark_kmodes.py")

# ===================================================================
# Load entire dataset

df = session.read.format("org.apache.spark.sql.cassandra")  \
    .options(table="swipes", keyspace="tugrafa")            \
    .load()

print("Data schema:")
df.printSchema()

# Find all known POIs (vertices)
pois = df.rdd.map(lambda r: r[2]).distinct()

# Find all possible edges with the initial count of zero
edges = pois.cartesian(pois)
edges = edges.map(lambda r: ((r[0], r[1]), 0))
#print(edges.take(10))

# ===================================================================
# Process dataset

# print(f"Size of the dataset: {df.count()}")
# 
# print("Number of swipes foreach POI")
# df.groupBy("poi").count().show()
# 
# print("Number of swipes foreach card")
# df.groupBy("card_id").count().show()

"""
edges = session.sparkContext.parallelize([
    ("Arena", "Porto Tolle", 4205),
    ("Arena", "Duomo", 291),
    ("Arena", "Torre Lamberti", 954),
    ("Duomo", "Torre Ridolfi", 524),
    ("Porto Tolle", "Duomo", 2345)
])

cur_node  = "Arena"
cur_edges = edges
while True:
    cur_node, cur_edges = path.find_best_exit(cur_node, cur_edges)
    print(cur_edges.collect())
"""

if False:

    # ===================================================================
    # Calcolare l'efficienza di uso della Card, ovvero il numero di PoI al giorno visitati;
    # mostrare l'istogramma del risultato (asse x: numero di PoI; asse y: quantità di Card usate per quel numero di PoI)

    # Returns a (card_id, date, poi_visited) dataframe
    visited = df                                        \
        .groupBy("card_id", to_date("datetime")         \
            .alias("date"))                             \
        .count()                                        \
        .withColumnRenamed("count", "poi_visited")      \
        .sort(desc("card_id"), asc("date"))             \

    visited.show()

    # Returns a (date, poi_visited)
    efficency = visited                                 \
        .groupBy("poi_visited")                         \
        .count()                                        \
        .withColumnRenamed("count", "cards_count")      \
        .sort(asc("poi_visited"))

    efficency.show()

    # Show histogram of data
    pdf = efficency.toPandas()
    pdf.plot(x="poi_visited", y="cards_count", xlabel="poi visited", ylabel="cards count", kind="bar")
    plt.show()

if True:
    # Aggregate by card_id and generate list of visited POIs
    df2 = df.sort(asc("datetime")) \
        .groupBy('card_id') \
        .agg(collect_list("poi"))

    df2.show(truncate=False)

if False:
    # ===================================================================
    # Generate One-Hot Encoding of the visited pois, 
    # this increases the spatial dimension of the data a lot
    
    pois_list = pois.collect()

    onehot = df2
    for poi_name in pois_list:
        print(f"Adding boolean-integer value for poi: {poi_name}")
        onehot = onehot.withColumn(poi_name, pyf.array_contains("collect_list(poi)", poi_name))
        onehot = onehot.withColumn(poi_name, col(poi_name).cast(IntegerType()))

    print("DataFrame with On-Hot encoding of the visited pois:")
    onehot = onehot.drop("collect_list(poi)").select(pois_list)
    onehot.show()

    kmode_eva = EnsembleKModes(3, 10)
    kmode_fit = kmode_eva.fit(onehot.rdd)

    # Visualizza i principali subset di pois visitati
    for (idx, cluster) in enumerate(kmode_fit.clusters):
        print(f"Cluster {idx} = [ ", end="")
        for (poi, present) in zip(pois_list, cluster):
            if bool(present) == 1:
                print(poi, end=" ")
        print("]")

    result = [0] * len(pois_list)
    for cluster in kmode_fit.clusters:
        result = [x + y for x, y in zip(result, cluster)]

    # ==========================================================
    # Controlla che i clusters siano separati
    
    separated = True
    for value in result:
        if value > 1:
            separated = False
            break

    print(f"Clusters are separated: {separated}")

    # ==========================================================
    # Controlla che i clusters comprendano tutti i poi

    complete = True
    for value in result:
        if value == 0:
            complete = False
            break

    print(f"Clusters are complete: {complete}")

if True:
    def pois_to_path2(row):
        pois = row['collect_list(poi)']
        return list(zip(pois, pois[1:]))

    # Generate visited pois path
    paths_rdd = df2.rdd.flatMap(lambda row: pois_to_path2(row))
    paths_df  = paths_rdd.toDF(["from", "to"])

    paths_df.printSchema()
    paths_df.show(truncate=False)

if True:

    # ["id", "name", "age"]
    edges_count = paths_df.groupBy("from", "to").count() \
        .withColumnRenamed("from", "src") \
        .withColumnRenamed("to", "dst")
    
    # Map items in the form ((src, dst), cnt)
    edges_count = edges_count.rdd.map(lambda r: ((r[0], r[1]), r[2]))
    # Join with the complete graph's edges and keep the max
    edges = edges.leftOuterJoin(edges_count) \
        .map(lambda r: (r[0], nonemax(r[1][0], r[1][1])))

    #pprint(edges.collect())
    #exit(1)

    edges_df = edges.map(lambda r: (r[0][0], r[0][1], r[1])).toDF(["src", "dst", "cnt"])
    edges_df.write.csv("./data/output/edges")

    # From the paths extract the first POI visited and count how many times it was the first
    first_pois = df2.where(size("collect_list(poi)") > 0).select("card_id", col("collect_list(poi)")[0]) \
                .withColumnRenamed("collect_list(poi)[0]", "first_poi") \
                .groupBy("first_poi").count() \
                .sort(desc("count"))

    print("N. of times a POI was the first in the paths:")

    first_pois.show()
    first_pois.write.csv("./data/output/first")

    first = first_pois.take(1)[0].first_poi
    print(f"Most probable first POI: {first}")

    # Get number of POIs
    pois_count = len(pois.collect())
    print(f"Number of POIs: {pois_count}")

    # edges count should be pois * pois
    assert(len(edges.collect()) == pois_count * pois_count)

    visited = [ first ]

    current_src = first

    # Remove all edges goint to the first poi
    edges = edges.filter(lambda r: r[0][1] != first)

    # Find the next best node from the current
    for i in range(0, pois_count - 1):
        next_src, edges = path.find_best_exit(current_src, edges)
        visited.append(next_src)
        current_src = next_src

    #for i in range(0, pois_count-1):
    #    
    #    # Find all edges starting from current_poi and select the next probable one
    #    print(f"Finding the next best POI from {current_poi}...")
    #    new_poi = edges.filter((~edges.dst.isin(visited)) & (edges.src == current_poi)) \
    #        .sort(desc("count"))
    #    
    #    print(new_poi.count())
    #    if new_poi.count() == 0:    
    #        break
    #
    #    next_poi = new_poi.collect()[0].dst
    #    print(f"Next best POI is {next_poi}")
    #
    #    # Remove all edges that are now superflous
    #    print("Removing edges...")
    #    edges = edges.filter(~(edges.src == current_poi) & ~(edges.dst == current_poi))
    #    #edges.show()
    #
    #    # Append to best path
    #    visited.append(next_poi)
    #    current_poi = next_poi
        

    print(f"Most probable path is {visited}")


if True:
    #visited = ["Santa Anastasia", "Casa Giulietta", "Torre Lamberti",  "Castelvecchio"]
    #paths = [["049D67523F3880", ["Santa Anastasia", "Casa Giulietta", "Castelvecchio"]]]
    visited.insert(0, "start")
    visited.insert(len(visited), "end")

    print(visited)

    data = []
    # 
    for row in df2.rdd.collect():
        print("---------------------------------------------------------------------")
        perfect = True
        deviation = False
        card_id, path = row
        path.insert(0, "start")
        path.insert(len(path), "end")
        
        for i in range(1, len(path)-1) :
            poi = path[i]
            optimal_node = visited[i]
            if not deviation:
                if poi != optimal_node:
                    time = df.filter((df.card_id == card_id ) & (df.poi == poi)).select("datetime")
                    arrived_poi = (time.first()["datetime"])
                    prima = arrived_poi - timedelta(hours=0, minutes=30, seconds=0)
                    dopo = arrived_poi + timedelta(hours=0, minutes=30, seconds=0)
                    
                    people_perfect = df.filter((df.datetime >= prima) & (df.datetime <= dopo) & (df.poi == optimal_node)).count()
                    people_poi = df.filter((df.datetime >= prima) & (df.datetime <= dopo) & (df.poi == poi)).count()
                    
                    print(f"{card_id}: Nel poi scelto({poi}) sono presenti {people_poi} persone mentre nel perfect ({optimal_node}) sono presenti {people_perfect}")
                    data.append((path[i-1], path[i], arrived_poi.strftime("%H:%M:%S"), people_perfect-people_poi))
                    perfect = False
                    deviation = True
                else:
                    print("Node Same")

        if perfect:
            print(f"{card_id}: Path uguale a quello ottimo")
            print(path)
        

    print("Tutte le difference (people_perfect-people_poi):")
    
    print(data)
    columns = ['from', 'dest', 'time', 'Difference']
   
    deviations = session.createDataFrame(data, columns)
    deviations.show()
    deviations.write.csv("./data/output/deviations")