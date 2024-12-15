import os
import pandas as pd
import graphistry
import networkx as nx
from dotenv import load_dotenv

# TODO look into using cuDF instead if speed becomes a barrier
load_dotenv(".env")
graphistry.register(api=3,
                    server='hub.graphistry.com',
                    username=os.getenv("GRAPHISTRY_USERNAME"),
                    password=os.getenv("GRAPHISTRY_PASSWORD"))

if __name__ == "__main__":
    # df = pd.read_csv(r"Data\cleaned_boiler_room_data.csv", encoding="utf-8")
    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")
    artist_cols = df.filter(regex=r"(DJ\d+)|(RemixOrEdit\d+)|(Artist\d+)").columns

    # Create a list of edges which are artist to artist.
    list_of_edges = []  # Format - start - finish - DJ - artist - track name - genre
    for index, col in enumerate(artist_cols):
        if not index == len(artist_cols):
            for second_col in artist_cols[index+1:]:
                # this isnt great code to read, but basically:
                # store a df that is the Node 1 (col), Node 2 (second col), and all the track details
                # drop any edges where either side is null.
                sub_df = df[[col, second_col, "DJ", "Artist", "TrackName", "Genre", "RemixOrEdit", "Date"]].dropna(
                        subset=[col, second_col], how="any"
                        ).copy()

                sub_df = sub_df.rename(
                        columns={col: "Node1", second_col: "Node2"}  # rename cols
                    )
                if not sub_df.empty:
                    list_of_edges.append(sub_df)

    # empty series warning indicated there were empty series in the list of edges - now remove above
    df_of_edges = pd.concat(list_of_edges).reset_index()
    df_of_nodes = pd.DataFrame(pd.unique(pd.concat([df_of_edges["Node1"], df_of_edges["Node2"]], axis=0)), columns=["Node"])

    # calculate communities using louvain algorithm and networx package
    g_nx = nx.from_pandas_edgelist(df=df_of_edges, source="Node1", target="Node2") 
    communities = nx.community.louvain_communities(g_nx)

    for index, community in enumerate(communities):
        # note most elegant but fastest way to assign communities
        for node in community:
            df_of_nodes.loc[df_of_nodes["Node"] == node, "Louvain_Community"] = index

    print(df_of_edges)
    print(df_of_nodes)

    # Graph analytics with graphistry
    g = graphistry.edges(df_of_edges, "Node1", "Node2").nodes(df_of_nodes, "Node")
    g.encode_point_color("Louvain_Community",  palette=['silver', 'maroon', '#FF99FF'], as_continuous=True).plot()


