
from pyspark import RDD

# edges is in the form [((src, dst), cnt)]
def find_best_exit(cur: str, edges: RDD):

    # Return the best edge
    fn_best_edge = lambda r, s: r if r[1] >= s[1] else s
    # Return true if the edge starts at the cur node
    fn_valid_src = lambda r: r[0][0] == cur

    # Remove all edges going to the current node
    edges = edges.filter(lambda r: r[0][1] != cur)

    # Find only the outgoing edges from cur
    out_edges = edges.filter(fn_valid_src)
        
    # Find the destination with the max path count
    (next_src, next_dst), next_cnt = out_edges.reduce(fn_best_edge)
    print(f"Best outgoing edge: {next_src} -> {next_dst}, with a score of {next_cnt}")

    # Returns a copy of the edges and next node
    return next_dst, edges