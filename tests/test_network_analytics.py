import numpy as np
import pandas as pd
import pytest

import src.network_analytics as na


def _row(**names):
    return {"TrackName": "T", "Genre": np.nan, "Date": "2020", "DJ": "x", "Artist": "y", **names}


def _edge_set(edges):
    return set(edges[["Source", "Target", "EdgeType"]].itertuples(index=False, name=None))


# --- Issue 8: typed, directed, weighted edges --------------------------------------------------


def test_build_play_edges_types_and_directions():
    df = pd.DataFrame([_row(DJ0="A", DJ1="B", Artist0="D", Artist1="C", RemixOrEdit0="E")])
    edges = na.build_play_edges(df)
    assert _edge_set(edges) == {
        ("A", "D", "played"),
        ("A", "C", "played"),
        ("B", "D", "played"),
        ("B", "C", "played"),
        ("A", "E", "played"),
        ("B", "E", "played"),
        ("E", "D", "remixed"),
        ("E", "C", "remixed"),
        ("C", "D", "collab"),  # undirected: stored once, sorted
        ("A", "B", "b2b"),
    }
    assert len(edges) == 10
    assert (edges["TrackName"] == "T").all()  # track details carried onto play edges


def test_build_play_edges_drops_self_loops_and_missing_names():
    df = pd.DataFrame(
        [_row(DJ0="A", Artist0="A", RemixOrEdit0=np.nan), _row(DJ0="A", Artist0=np.nan)]
    )
    assert na.build_play_edges(df).empty


def test_aggregate_edges_weights_parallel_plays():
    df = pd.DataFrame([_row(DJ0="A", Artist0="C"), _row(DJ0="A", Artist0="C")], index=[0, 0])
    edges, nodes = na.create_edglist(df)
    assert edges[["Source", "Target", "EdgeType", "Weight"]].to_numpy().tolist() == [
        ["A", "C", "played", 2]
    ]
    assert set(nodes["Node"]) == {"A", "C"}


def test_to_weighted_digraph_symmetrises_undirected_and_sums_types():
    edges = pd.DataFrame(
        [("A", "B", "played", 2), ("A", "B", "b2b", 3)],
        columns=["Source", "Target", "EdgeType", "Weight"],
    )
    g = na.to_weighted_digraph(edges)
    assert g["A"]["B"]["Weight"] == 5
    assert g["B"]["A"]["Weight"] == 3


def test_to_multidigraph_keeps_edge_types():
    edges = pd.DataFrame(
        [("A", "B", "played", 2), ("A", "B", "remixed", 1)],
        columns=["Source", "Target", "EdgeType", "Weight"],
    )
    g = na.to_multidigraph(edges)
    assert g.number_of_edges("A", "B") == 2
    assert g["A"]["B"]["remixed"]["Weight"] == 1


# --- Issues 6 & 7: seeded communities, explicit inter-community edges --------------------------


@pytest.fixture
def two_cliques():
    """Two 4-cliques joined by a single light edge (D -> E)."""
    rows = []
    for clique in (["A", "B", "C", "D"], ["E", "F", "G", "H"]):
        rows += [(s, t, "collab", 5) for i, s in enumerate(clique) for t in clique[i + 1 :]]
    rows.append(("D", "E", "played", 1))
    edges = pd.DataFrame(rows, columns=["Source", "Target", "EdgeType", "Weight"])
    nodes = pd.DataFrame({"Node": list("ABCDEFGH")})
    return edges, nodes


def test_assign_communities_is_reproducible(two_cliques):
    first = na.assign_communities(*two_cliques)
    second = na.assign_communities(*two_cliques)
    pd.testing.assert_frame_equal(first[0], second[0])
    pd.testing.assert_frame_equal(first[1], second[1])


def test_assign_communities_flags_inter_community_edges(two_cliques):
    edges, nodes = na.assign_communities(*two_cliques)
    community = dict(zip(nodes["Node"], nodes["LouvainCommunity"], strict=True))
    assert community["A"] == community["D"] != community["E"] == community["H"]
    assert (nodes["CommunitySize"] == 4).all()

    bridge = edges.loc[(edges["Source"] == "D") & (edges["Target"] == "E")].iloc[0]
    assert bridge["IsInterCommunity"]
    assert bridge["LouvainCommunity"] == bridge["CommunitySource"] == community["D"]
    assert edges["IsInterCommunity"].sum() == 1
