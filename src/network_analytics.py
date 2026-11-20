import os
import re

import graphistry
import networkx as nx
import pandas as pd
from dotenv import load_dotenv

from src.config import SEED
from src.utils.data_loading import PATHS

# Reading for community detection
# https://arxiv.org/html/2309.11798v4; https://www.nature.com/articles/s41598-019-41695-z;
# TODO - refactor with Leiden algo rather than Louvain - https://github.com/esclear/louvain-leiden/blob/main/community_detection/leiden.py

# TODO look into using cuDF instead if speed becomes a barrier

# Edge types: (source column prefix, target column prefix, edge type, directed)
EDGE_RELATIONS = [
    ("DJ", "Artist", "played", True),
    ("DJ", "RemixOrEdit", "played", True),
    ("RemixOrEdit", "Artist", "remixed", True),
    ("Artist", "Artist", "collab", False),
    ("DJ", "DJ", "b2b", False),
]
UNDIRECTED_EDGE_TYPES = {edge_type for *_, edge_type, directed in EDGE_RELATIONS if not directed}
TRACK_DETAIL_COLS = ["DJ", "Artist", "TrackName", "Genre", "RemixOrEdit", "Date"]


def register_graphistry() -> None:
    load_dotenv(PATHS.env_file)
    graphistry.register(
        api=3,
        server="hub.graphistry.com",
        username=os.getenv("GRAPHISTRY_USERNAME"),
        password=os.getenv("GRAPHISTRY_PASSWORD"),
    )


def _names_long(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Long table of (RowID, Name) for the numbered columns prefix0, prefix1, ..."""
    cols = [col for col in df.columns if re.fullmatch(rf"{prefix}\d+", col)]
    if not cols:
        return pd.DataFrame(columns=["RowID", "Name"])
    long = df[cols].rename_axis("RowID").reset_index().melt(id_vars="RowID", value_name="Name")
    return long.dropna(subset=["Name"])[["RowID", "Name"]]


def build_play_edges(df: pd.DataFrame) -> pd.DataFrame:
    """One typed edge per track row and relation (a multigraph edge list).

    Columns: Source, Target, EdgeType, RowID, plus the track details. Self-loops (e.g. a DJ
    playing their own track) are dropped. Undirected types (collab, b2b) are stored once,
    with Source < Target.
    """
    df = df.reset_index(drop=True)
    detail_cols = [col for col in TRACK_DETAIL_COLS if col in df.columns]
    names = {prefix: _names_long(df, prefix) for prefix in ("DJ", "Artist", "RemixOrEdit")}

    edge_frames = []
    for source_prefix, target_prefix, edge_type, directed in EDGE_RELATIONS:
        edges = names[source_prefix].merge(
            names[target_prefix], on="RowID", suffixes=("Source", "Target")
        )
        edges = edges.rename(columns={"NameSource": "Source", "NameTarget": "Target"})
        edges = edges.loc[edges["Source"] != edges["Target"]]
        if not directed:
            edges = edges.loc[edges["Source"] < edges["Target"]]
        edges = edges.assign(EdgeType=edge_type)
        edge_frames.append(edges[["Source", "Target", "EdgeType", "RowID"]])

    play_edges = pd.concat(edge_frames, ignore_index=True)
    play_edges = play_edges.merge(df[detail_cols], left_on="RowID", right_index=True, how="left")
    return play_edges


def aggregate_edges(play_edges: pd.DataFrame) -> pd.DataFrame:
    """Collapse parallel edges: one row per (Source, Target, EdgeType) with Weight = play count."""
    return (
        play_edges.groupby(["Source", "Target", "EdgeType"], as_index=False)
        .size()
        .rename(columns={"size": "Weight"})
    )


def to_weighted_digraph(edges: pd.DataFrame) -> nx.DiGraph:
    """Weighted DiGraph for community detection: weights summed across edge types, and
    undirected types (collab, b2b) added in both directions."""
    reverse = edges.loc[edges["EdgeType"].isin(UNDIRECTED_EDGE_TYPES)].rename(
        columns={"Source": "Target", "Target": "Source"}
    )
    both = pd.concat([edges, reverse])
    summed = both.groupby(["Source", "Target"], as_index=False)["Weight"].sum()
    return nx.from_pandas_edgelist(
        summed, "Source", "Target", edge_attr="Weight", create_using=nx.DiGraph
    )


def to_multidigraph(edges: pd.DataFrame) -> nx.MultiDiGraph:
    """Typed multigraph: one edge per (Source, Target, EdgeType), keyed by EdgeType."""
    return nx.from_pandas_edgelist(
        edges,
        "Source",
        "Target",
        edge_attr=["EdgeType", "Weight"],
        edge_key="EdgeType",
        create_using=nx.MultiDiGraph,
    )


def create_edglist(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Typed, weighted edges (one row per Source, Target, EdgeType) and the node list."""
    edges = aggregate_edges(build_play_edges(df))
    nodes = pd.DataFrame({"Node": pd.unique(pd.concat([edges["Source"], edges["Target"]]))})
    return edges, nodes


def assign_communities(
    edges: pd.DataFrame, nodes: pd.DataFrame, seed: int = SEED
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Seeded weighted Louvain. Communities are numbered by size (0 = largest).

    Nodes get LouvainCommunity and CommunitySize. Edges get CommunitySource, CommunityTarget,
    IsInterCommunity, and LouvainCommunity/CommunitySize taken from the source node.
    """
    communities = nx.community.louvain_communities(
        to_weighted_digraph(edges), weight="Weight", seed=seed
    )
    communities = sorted(communities, key=lambda c: (-len(c), min(c)))
    community_of = {node: i for i, members in enumerate(communities) for node in members}
    size_of = {i: len(members) for i, members in enumerate(communities)}

    nodes = nodes.assign(LouvainCommunity=nodes["Node"].map(community_of))
    nodes = nodes.assign(CommunitySize=nodes["LouvainCommunity"].map(size_of))

    edges = edges.assign(
        CommunitySource=edges["Source"].map(community_of),
        CommunityTarget=edges["Target"].map(community_of),
    )
    edges = edges.assign(
        IsInterCommunity=edges["CommunitySource"] != edges["CommunityTarget"],
        LouvainCommunity=edges["CommunitySource"],
        CommunitySize=edges["CommunitySource"].map(size_of),
    )
    return edges, nodes


if __name__ == "__main__":
    # df = pd.read_csv(PATHS.processed.cleaned_boiler_room_data_csv, encoding="utf-8")
    df = pd.read_parquet(PATHS.processed.cleaned_boiler_room_data_parquet)
    df_of_edges, df_of_nodes = create_edglist(df=df)
    df_of_edges, df_of_nodes = assign_communities(df_of_edges, df_of_nodes)

    # Only keep communities with > 15 members, to try get graph to load
    df_of_nodes = df_of_nodes.loc[df_of_nodes["CommunitySize"] > 15, :]
    kept = set(df_of_nodes["Node"])
    df_of_edges = df_of_edges.loc[
        df_of_edges["Source"].isin(kept) & df_of_edges["Target"].isin(kept), :
    ]
    print(df_of_edges.shape, df_of_nodes.shape)
    print(df_of_edges.head())
    print(df_of_edges["EdgeType"].value_counts())

    # TODO - Natural language processing of genres for each community

    register_graphistry()
    colour_pallete = ['black', '#fdae6b', '#fee6ce']
    # Graph analytics with graphistry
    g = graphistry.edges(df_of_edges, "Source", "Target")\
                  .nodes(df_of_nodes, "Node")\
                  .encode_point_color("LouvainCommunity",  palette=colour_pallete, as_continuous=True)\
                  .encode_edge_color("LouvainCommunity", palette=colour_pallete, as_continuous=True)\
                  .encode_point_icon("LouvainCommunity")\
                  .scene_settings(menu=False, info=False, show_arrows=False, point_size=0.7, edge_curvature=0.0, edge_opacity=0.5, point_opacity=0.9)\
                  .addStyle(bg={"color": 'light grey'})

    url = g.plot()
    print(url)

