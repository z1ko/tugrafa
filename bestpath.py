from pyspark import DataFrame

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
import path

# Generate all directed edges from a path
@pyf.udf(returnType=pyt.ArrayType(pyt.StringType()))
def path_to_edges(path):
    return list(zip(path, path[1:]))


def nonemax(a, b):
    if (b is None): return a
    else: return max(a, b)


def calculate(df: DataFrame):
    """
    Find the most problable path
    """
    # Find all distinct POIs
    pois = df.rdd.map(lambda r: r[2]).distinct()
    pois_list = pois.collect()

    # Aggregate all visited pois by a card
    visited = df.sort(pyf.asc("datetime")).groupBy("card_id") \
            .agg(pyf.collect_list("poi").alias("pois"))

    # Extract the best starting POI based on the number of times it was the first in a path
    best_first_poi = visited.where(pyf.size("pois") > 0) \
            .select("card_id", pyf.col("pois")[0]) \
            .withColumnRenamed("pois[0]", "first_poi") \
            .groupBy("first_poi").count() \
            .sort(pyf.desc("count")) \
            .take(1)[0].first_poi

    print(f"Best first POI: {best_first_poi}")
    
    # Find all possible edges with the initial count of zero ((from, to), 0)
    edges_count = pois.cartesian(pois).map(lambda r: ((r[0], r[1]), 0))

    # Generate all edges from the visited dataframe ((from, to), cnt)
    visited_edges_count = visited.rdd.flatMap(path_to_edges("pois")) \
            .map(lambda r: ((r[0], r[1]), 1)) \
            .reduceByKey(add)

    # Generate complete graph of edges with count
    edges_count = edges_count.leftOuterJoin(visited_edges_count) \
            .map(lambda r: (r[0], nonemax(r[1][0], r[1][1]))) \
            .map(lambda r: (r[0][0], r[0][1], r[1])) \
            .toDF(["from", "to", "count"])

    pois_count = len(pois_list)
    assert(len(edges_count.collect()) == pois_count * pois_count)

    # ==========================================================================
    # Find the best path based on the number of times an edge was used

    best_path = [ best_first_poi ]
    current_poi = best_first_poi

    # Remove all edges going to the first POI
    edges_count = edges_count.filter(lambda r: r[0][1] != best_first_poi)
    
    for i in range(0, pois_count - 1):
        next_poi, edges_count = path.find_best_exit(current_poi, edges_count)
        best_path.append(next_poi)
        current_poi = next_poi

    print(f"Best path: {best_path}")


