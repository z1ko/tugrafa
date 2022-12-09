
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
import matplotlib.pyplot as plt

def calculate(df: DataFrame, show_histogram: bool = False):
    """
    Calcolare l'efficienza d'uso della card, ovvero il numero di POI al giorno visitati,
    mostrare l'istogramma del risultato (x: numero di POI, y: quantita di cards per quel numero di POI)
    """

    visited = df.groupBy("card_id", pyf.to_date("datetime").alias("date")).count() \
            .withColumnRenamed("count", "visited_pois_count") \
            .sort(pyf.desc("card_id"), pyf.asc("date"))

    # Returns the number of pois visited by day
    efficency = visited.groupBy("visited_pois_count").count() \
            .withColumnRenamed("count", "cards_count") \
            .sort(pyf.asc("visited_pois_count"))

    efficency.show()

    # Show histogram of data
    if show_histogram:
        pdf = efficency.toPandas()
        pdf.plot(x="poi_visited", y="cards_count", xlabel="poi visited", ylabel="cards count", kind="bar")
        plt.show()

