import os
import pandas as pd
import graphistry
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
    print(df_of_edges)

    # Graph analytics with graphistry
    g = graphistry.hypergraph(
        df,

        # Optional: Subset of columns to turn into nodes; defaults to all
        entity_types=['DJ', 'Artist', "RemixOrEdit"],

        # Optional: merge nodes when their IDs appear in multiple columns
        # ... so replace nodes attackerIP::1.1.1.1 and victimIP::1.1.1.1
        # ... with just one node ip::1.1.1.1
        opts={
            'CATEGORIES': {
                'Artist': ['Artist', "RemixOrEdit"]
            }
        })
    g_g = g['graph']
    g_g.plot()

    # TODO switch to Approach 1 in this: https://pygraphistry.readthedocs.io/en/latest/demos/for_analysis.html
    # rather than using a edge based approach

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

