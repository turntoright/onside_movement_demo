"""Key property identification (spec §5): degree, strength and weighted PageRank."""

import networkx as nx
import pandas as pd


def model(dbt, session):
    dbt.config(materialized="table")
    movements = dbt.ref("fct_movements").df()
    properties = dbt.ref("stg_properties").df()[["property_id", "property_type", "region"]]

    edges = (
        movements.groupby(["origin_property_id", "dest_property_id"])
        .size()
        .reset_index(name="movements")
    )
    graph = nx.DiGraph()
    graph.add_nodes_from(properties["property_id"])
    for row in edges.itertuples(index=False):
        graph.add_edge(row.origin_property_id, row.dest_property_id, weight=float(row.movements))

    pagerank = nx.pagerank(graph, weight="weight")
    rows = [
        {
            "property_id": node,
            "in_degree": graph.in_degree(node),
            "out_degree": graph.out_degree(node),
            "weighted_in": graph.in_degree(node, weight="weight"),
            "weighted_out": graph.out_degree(node, weight="weight"),
            "pagerank": pagerank[node],
        }
        for node in graph.nodes
    ]
    df = properties.merge(pd.DataFrame(rows), on="property_id", how="left")
    df["pagerank_rank"] = df["pagerank"].rank(ascending=False, method="first").astype(int)
    return df.sort_values("pagerank_rank").reset_index(drop=True)
