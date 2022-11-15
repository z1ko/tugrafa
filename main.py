
from pprint import pprint
#from dotenv import dotenv_values

import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, desc, asc

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