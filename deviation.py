
import datetime

import pyspark.sql.functions as pyf
from pyspark.sql.types import *

# Best path found using other queries
BEST_PATH = [
    'Arena', 
    'Casa Giulietta', 
    'Torre Lamberti', 
    'Palazzo della Ragione', 
    'Santa Anastasia', 
    'Duomo', 
    'Teatro Romano', 
    'Castelvecchio', 
    'San Zeno', 
    'San Fermo', 
    'Tomba Giulietta', 
    'Museo Storia', 
    'Giardino Giusti', 
    'Museo Lapidario', 
    'Museo Radio', 
    'Centro Fotografia', 
    'AMO', 
    'Sighseeing', 
    'Verona Tour',
    'END'
]

# Job executed on each row in parallel, 
# returns the index of the best path where there is the deviation
@pyf.udf(returnType=IntegerType())
def get_deviation_index(path):

    # Find the first divergent poi from the best path
    # NOTE: Shorter paths are not a deviation
    path = sorted(path, key=lambda x: x[1])
    for index, (poi, datetime) in enumerate(path):
        if index == 0:
            last_datetime = datetime

        # Elements must be in correct temporal order
        assert(last_datetime <= datetime)
        last_datetime = datetime

        if poi != BEST_PATH[index]:
            return index

    return -1


@pyf.udf(returnType=StringType())
def get_deviation_original(index):
    return BEST_PATH[index]


@pyf.udf(returnType=StringType())
def get_deviation_poi(index, path):
    assert(index >= 0) # There must be a deviation
    return path[index][0]


@pyf.udf(returnType=TimestampType())
def get_deviation_timeslot(index, path):
    assert(index >= 0) # There must be a deviation
    timestamp = path[index][1]
    return timestamp.replace(minute=0, second=0, microsecond=0)


def calculate(df):

    print("Cards paths:")

    paths = df.sort(pyf.asc("datetime")).groupBy('card_id') \
        .agg(pyf.collect_list(pyf.struct("poi", "datetime")).alias("path"))
    
    #paths.show(truncate=False)
    print("Cards with a deviation:")
    
    deviations = paths \
        .withColumn("deviation_index", get_deviation_index("path")) \
        .filter(pyf.col("deviation_index") >= 0)
    
    #deviations.select("card_id", "deviation_index").show()
    print("Cards deviations:")

    deviations = deviations \
        .withColumn("deviation_timeslot", get_deviation_timeslot("deviation_index", "path")) \
        .withColumn("deviation_original", get_deviation_original("deviation_index")) \
        .withColumn("deviation_poi", get_deviation_poi("deviation_index", "path")) \
        .drop("path", "deviation_index")
    
    #deviations.show()

    # Count the amount of times a veronacard has changed path from the best path at a specific index and timeslot
    timeslot_groups = deviations.groupBy("deviation_timeslot", "deviation_original", "deviation_poi").count()
    #timeslot_groups.show(200)

    # Count the number of cards in a poi in a timeslot
    df2 = df.rdd.map(lambda r: (r[0], r[1].replace(minute=0, second=0, microsecond=0), r[2]))
    df2 = df2.toDF(["card_id", "timeslot", "poi"])
    df2 = df2.groupBy("timeslot", "poi").count().withColumnRenamed("count", "original_count")
    #df2.show()

    # Create final difference dataframe
    joined = timeslot_groups.join(df2, \
        (timeslot_groups.deviation_timeslot == df2.timeslot) & (timeslot_groups.deviation_original == df2.poi), \
        "inner").drop("poi", "timeslot").withColumnRenamed("count", "deviation_count")
    
    # Insert delta
    joined = joined.withColumn("delta", joined.original_count - joined.deviation_count)
    joined.show()

    joined = joined.withColumn("hour",  pyf.date_format(joined.deviation_timeslot, 'HH:mm:ss')) \
        .groupBy("deviation_original", "deviation_timeslot") \
        .agg(pyf.sum("deviation_count")).show()



