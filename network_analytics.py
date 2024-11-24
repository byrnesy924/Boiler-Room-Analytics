import pandas as pd
import graphistry

# TODO look into using cuDF instead if speed becomes a barrier

if __name__=="__main__":
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
                list_of_edges.append(
                    df[[col, second_col, "DJ", "Artist", "TrackName", "Genre"]].dropna(
                        subset=[col, second_col], how="any"
                        ).copy().rename(
                        {col: "Node1", second_col: "Node2"}  # rename cols
                    )
                )

    df_of_edges = pd.concat(list_of_edges)
    print(df_of_edges)

    # Graph analytics with graphistry
    g = graphistry.edges(df, "Node1", "Node2")
    g.plot()

    # hg2 = graphistry.hypergraph(
    #     df,
    #     entity_types=['attackerIP', 'victimIP', 'victimPort', 'vulnName'],
    #     direct=True,
    #     opts={
    #         # Optional: Without, creates edges that are all-to-all for each row
    #         'EDGES': {
    #             'attackerIP': ['victimIP', 'victimPort', 'vulnName'],
    #             'victimPort': ['victimIP'],
    #             'vulnName': ['victimIP']
    #         },

    #         # Optional: merge nodes when their IDs appear in multiple columns
    #         # ... so replace nodes attackerIP::1.1.1.1 and victimIP::1.1.1.1
    #         # ... with just one node ip::1.1.1.1
    #         'CATEGORIES': {
    #             'ip': ['attackerIP', 'victimIP']
    #         }
    #     })

    # hg2_g = hg2['graph']
    # hg2_g.plot()

