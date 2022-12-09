
from pyspark.sql import DataFrame
from pyspark_kmodes import EnsembleKModes

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

def calculate(df: DataFrame):
    """
    Find if the cards are related to subsets of POIs
    """

    # Find all known POIs (vertices)
    pois = df.rdd.map(lambda r: r[2]).distinct()
    pois_list = pois.collect()

    # Find all visited POIs of the cards
    visited = df.sort(pyf.asc("datetime")).groupBy("card_id") \
            .agg(pyf.collect_list("poi").alias("pois"))

    # Generate One-Hot Encoding of visited pois,
    # This increases the spatial dimension of the data a lot
    for poi_name in pois_list:
        visited = visited.withColumn(poi_name, \
                pyf.array_contains("pois", poi_name).cast(pyt.IntegerType()))
    
    visited = visited.drop("pois")
    visited.select(pois_list).show()

    # Uses the KModes method to generate 3 subsets of pois with the lowest hamming distance
    kmodes = EnsembleKModes(n_clusters=3, max_dist_iter=10, local_kmodes_iter=10)
    evaluation = kmodes.fit(visited.rdd)

    # Visualizza i cluster di pois
    print("Principal POIs subsets found:")
    for idx, cluster in enumerate(evaluation.clusters):
        print(f"Subset {idx}: [ ", end='')
        for poi, active in zip(pois_list, cluster):
            if bool(active):
                print(poi, end=' ')
        print("]")

