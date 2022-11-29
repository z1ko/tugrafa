
import matplotlib.pyplot as plt
import pandas as pd
import pyvis as pv
import math

edges = pd.read_csv("./data/output/edges.csv/part-00000-5e798aa6-7ff3-47cf-ad40-63e5b9dfbc02-c000.csv", header=None)
first = pd.read_csv("./data/output/first.csv/part-00000-592e54df-01a1-475f-bb40-626e5c2f1ae4-c000.csv", header=None)

nt = pv.network.Network('1000px', '1000px', directed=True)

#G.add_nodes_from([poi for (poi, fcnt) in first.itertuples(index=False)])

nodes_idx = 0
nodes_map = {}

for (poi, cnt) in first.itertuples(index=False):
    
    current_idx = nodes_idx
    nodes_map[poi] = current_idx
    nodes_idx += 1

    print(f"Node({poi})")
    nt.add_node(current_idx, label=poi, size=math.log2(cnt))

# edges = [(src, dst, { "path_count": cnt }) for (src, dst, cnt) in edges.itertuples(index=False) if cnt > 1000]
for (src, dst, cnt) in edges.itertuples(index=False):

    if src not in nodes_map:
        nodes_map[src] = nodes_idx
        nt.add_node(nodes_idx, label=src, size=1)
        nodes_idx += 1

    if dst not in nodes_map:
        nodes_map[dst] = nodes_idx
        nt.add_node(nodes_idx, label=dst, size=1)
        nodes_idx += 1

    to_hide = False
    if cnt < 5000:
        to_hide = True

    nt.add_edge(nodes_map[src], nodes_map[dst], value=cnt, hidden=to_hide, arrowStrikethrough=False)


#nt.toggle_physics(False)
nt.show_buttons(filter_=['physics'])
nt.show("output.html")

#pos = nx.circular_layout(G)
#pct = nx.get_edge_attributes(G, 'path_count')
#nx.draw_networkx(G, pos, with_labels=True, font_weight="bold")
#nx.draw_networkx_edge_labels(G, pos, edge_labels = { e: pct[e] for e in G.edges()})