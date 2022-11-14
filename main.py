
from pprint import pprint
#from dotenv import dotenv_values

from pyspark.sql import SparkSession

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

# ===================================================================
# Process dataset

print(f"Size of the dataset: {df.count()}")

print("Number of swipes foreach POI")
df.groupBy("poi").count().show()

print("Number of swipes foreach card")
df.groupBy("card_id").count().show()