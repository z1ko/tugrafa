
from pyspark.sql import SparkSession

import multiprocessing as mp
import time

import bestpath
import clusters
import efficency
import deviation

def perf_measure(func, df):
    beg = time.process_time()
    func(df)
    return time.process_time() - beg

measures = []

# Create a session foreach increment of cores
for i, cores in enumerate(range(1, mp.cpu_count() + 1)):
    measures.append(dict())

    print(f"Measuring performance of {cores} cores...")
    session = SparkSession.builder.master(f"local[{cores}]").getOrCreate()

    # Load cassandra data
    df = session.read.format("org.apache.spark.sql.cassandra") \
            .options(table = "swipes", keyspace = "tugrafa") \
            .load()

    t1 = perf_measure(efficency.calculate, df)
    print(f"Efficency query time: {t1}")
    measures[i]["efficency"] = t1

    t2 = perf_measure(clusters.calculate, df)
    print(f"Clusters query time: {t2}")
    measures[i]["clusters"] = t2

    t3 = perf_measure(bestpath.calculate, df)
    print(f"Best path query time: {t3}")
    measures[i]["bestpath"] = t3

    t4 = perf_measure(deviation.calculate, df)
    print(f"Deviations query time: {t4}")
    measures[i]["deviations"] = t4

    print(f"MEASURES:\n{measures}")
    session.close()

