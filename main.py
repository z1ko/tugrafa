
from pprint import pprint
#from dotenv import dotenv_values

#import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, desc, asc, collect_list, col, size
import pyspark.sql.functions as pyf

from graphframes import *


# TODO: Performance considerations considering the cores count
session = SparkSession.builder    \
    .master("local[*]")           \
    .getOrCreate()

# Reduce log level
session.sparkContext.setLogLevel("ERROR")

# ===================================================================
# Load entire dataset

df = session.read.format("org.apache.spark.sql.cassandra")  \
    .options(table="swipes", keyspace="tugrafa")            \
    .load()

print("Data schema:")
df.printSchema()

# ===================================================================
# Process dataset

# print(f"Size of the dataset: {df.count()}")
# 
# print("Number of swipes foreach POI")
# df.groupBy("poi").count().show()
# 
# print("Number of swipes foreach card")
# df.groupBy("card_id").count().show()

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

    df2.show()

    def pois_to_path2(row):
        pois = row['collect_list(poi)']
        return list(zip(pois, pois[1:]))

    # Generate visited pois path
    #paths_rdd = df2.rdd.flatMap(lambda row: pois_to_path2(row))
    #paths_df  = paths_rdd.toDF(["from", "to"])

    #paths_df.printSchema()
    #paths_df.show(truncate=False)

    # ["id", "name", "age"]

    #edges = paths_df.groupBy("from", "to").count() \
    #                .withColumnRenamed("from", "src") \
    #                .withColumnRenamed("to", "dst")
    #edges.show()
#
    #vertices = df.select("poi").distinct() \
    #            .withColumnRenamed("poi", "id")
    #vertices.show()

    # From the paths extract the first POI visited and count how many times it was the first
    first_pois = df2.where(size("collect_list(poi)") > 0).select("card_id", col("collect_list(poi)")[0]) \
                .withColumnRenamed("collect_list(poi)[0]", "first_poi") \
                .groupBy("first_poi").count() \
                .sort(desc("count"))

    first_pois.show()
    first = first_pois.agg(pyf.max("count")).select("first_poi")
    print(f"Most probable first POI: {first}")
    
