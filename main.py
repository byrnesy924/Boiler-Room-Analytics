import os
import pandas as pd
import logging
import graphistry
import networkx as nx

from dotenv import load_dotenv

from scrape_live_1001tracklist_data import get_urls_for_boiler_rooms, multiprocessing_wrapper
from clean_br_data import clean_data
from genre_download_script import spotify_functional_flow, discogs_functional_flow
from network_analytics import create_edglist


logger = logging.getLogger(__name__)
logging.basicConfig(filename='main.log', level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', filemode="w")

# Reading for community detection
# https://arxiv.org/html/2309.11798v4; https://www.nature.com/articles/s41598-019-41695-z;

load_dotenv(".env")
graphistry.register(api=3,
                    server='hub.graphistry.com',
                    username=os.getenv("GRAPHISTRY_USERNAME"),
                    password=os.getenv("GRAPHISTRY_PASSWORD"))

if __name__ == "__main__":
    # Hack - 1001 tracklists gives 404 on get requests, imitate a browser with the following headers
    headers = {
        "authority": "www.1001tracklists.com",
        "method": "GET",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Connection": "keep-alive",
        "Cookie": "__qca=P0-820745957-1730510035946; guid=182cd3a595868; cookie=67f919b8-748c-4374-af2a-01fd8441f9ce; cookie_cst=zix7LPQsHA%3D%3D; _lr_env_src_ats=false; _cc_id=a939ad4a21d176c1613a23d09c2dd280; _au_1d=AU1D-0100-001730510032-PNJ3K11R-9Q0Z; ccuid=a0ce8c1d-1a64-4ce3-b6f8-4e340ac33f25; __browsiUID=3947c912-37f2-4c47-8b68-d4aa47c6dabc; __qca=P0-820745957-1730510035946; _ga=GA1.1.557609463.1730510128; panoramaId_expiry=1732246617834; panoramaId=244ad9e18af7acbe9d63ca051aba4945a7023f9f254678cec2c592fc994c43d7; panoramaIdType=panoIndiv; uuid=30CD4DF9-5B15-4E0D-923C-1F8621C78EBD; rl_anonymous_id=RS_ENC_v3_IjAzMDlkZDNiLTkxYjItNDQ5MS1iOWVhLTMzNTkxNjk3OWM0ZSI%3D; rl_page_init_referrer=RS_ENC_v3_IiRkaXJlY3Qi; rl_user_id=RS_ENC_v3_ImQzOGFmOTI0NzZjODM3NmMzMGYwYzE0NGMxMjk2NzhmZGU5ZTczNjQi; rl_session=RS_ENC_v3_eyJpZCI6MTczMTY0OTI3MDU5NSwiZXhwaXJlc0F0IjoxNzMxNjUxMDcwNjA2LCJ0aW1lb3V0IjoxODAwMDAwLCJhdXRvVHJhY2siOnRydWUsInNlc3Npb25TdGFydCI6dHJ1ZX0%3D; rl_trait=RS_ENC_v3_eyJydWRkZXJJZCI6IjJVQVBxY0tsYUFqTHQwa0poTTJ1YzNoWUpmNCIsImNhbXBhaWduIjoiMzI1NTkyMDAiLCJhZHYiOiIxMjUzNjMxNCIsInAiOiI0MDY0MTI5MjMiLCJjIjoiMjIzMDk1NTU1IiwiYWQiOiI1OTYzNDk0MDMiLCJlbnYiOiJqIiwicm5kIjoiMTAxMDM4NjUyOSIsInV1aWQiOiIxNTUyMGVmNi1mNjUwLTRjMzQtODdkMi1mZTY3ODEzMjE2MmEiLCJhY3Rpb24iOiJpbXByZXNzaW9uIiwib3JpZ2lucyI6Imh0dHBzOi8vd3d3LjEwMDF0cmFja2xpc3RzLmNvbSxodHRwczovL3d3dy4xMDAxdHJhY2tsaXN0cy5jb20ifQ%3D%3D; _lr_retry_request=true; connectId=%7B%22vmuid%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22connectid%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22connectId%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22ttl%22%3A86400000%2C%22lastSynced%22%3A1732013392484%2C%22lastUsed%22%3A1732013410284%7D; _ga_FVWZ0RM4DH=GS1.1.1732013352.7.1.1732013416.60.0.0; cto_bundle=tXSRZl9pOFI4aFE4N2ZrZG42UkVCVzZ5V3JuRFFNcDRaZEUlMkJ4MDFIZG52UWdzJTJGZ1B0dEQ3Vnl4ZUNvbktPRmVqQm1rdURZeGUxUkhZTHQwbVZRQnlJdHpJYUtBWVpsNW92S1ZYclpFUFFOVWdaOFRZVG05bjFKWXBmV3prM1lDeDElMkJqWGRWTjhoY0Jhenp1VDFOJTJCazNYJTJGUjNscjFBJTJCTUM1NWdmaXdqN1pwbiUyRlpkYyUzRA; __gads=ID=2d8730b0bb5c4846:T=1730510033:RT=1732016048:S=ALNI_MbmXAwUeYEfUNmb0EsfqQoSSLDLuw; __gpi=UID=00000f5d9c477e5a:T=1730510033:RT=1732016048:S=ALNI_Ma2FV8skRTR4UE1YsUgfJvV0E6tFA; __eoi=ID=398cba7e0c60d090:T=1730510033:RT=1732016048:S=AA-AfjY2swsSuc8pyzj5-wPCSGNa",
        "dnt": "1",
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1"
    }

    sets = get_urls_for_boiler_rooms(headers=headers)
    logger.info(f"Number of sets found:{len(sets)}")
    logger.info(f"Sets to do:{sets}")
    if len(sets) == 0:
        raise Exception("Sets is empty - request and load failed")

    # df = create_db_of_tracklists(br_urls=sets, headers=headers)
    dfs = [multiprocessing_wrapper(set=br_set, headers=headers) for br_set in sets]
    df = pd.concat(dfs)

    raw_df = df.copy()
    df = clean_data(raw_df, save_visualisation=True)

    # number of missing genres - whether it is worth getting these from the beatport API
    print(df.isnull().sum())

    # Genre Data
    df = discogs_functional_flow(df=df)
    df = spotify_functional_flow(df=df)

    # Graph Analytics
    artist_cols = df.filter(regex=r"(DJ\d+)|(RemixOrEdit\d+)|(Artist\d+)").columns
    df_of_edges, df_of_nodes = create_edglist(df=df, artist_cols=artist_cols)

    # calculate communities using louvain algorithm and networx package
    g_nx = nx.from_pandas_edgelist(df=df_of_edges, source="Node1", target="Node2") 
    communities = nx.community.louvain_communities(g_nx)

    for index, community in enumerate(communities):
        # note most elegant but fastest way to assign communities
        for node in community:
            df_of_nodes.loc[df_of_nodes["Node"] == node, "Louvain_Community"] = index
            df_of_nodes.loc[df_of_nodes["Node"] == node, "Community_size"] = len(community)

            df_of_edges.loc[(df_of_edges["Node1"] == node) | (df_of_edges["Node2"] == node), "Louvain_Community"] = index

    # TODO - Natural language processing of genres for each community 

    colour_pallete = ['black', '#fdae6b', '#fee6ce']
    # Graph analytics with graphistry
    g = graphistry.edges(df_of_edges, "Node1", "Node2")\
                  .nodes(df_of_nodes, "Node")\
                  .encode_point_color("Louvain_Community",  palette=colour_pallete, as_continuous=True)\
                  .encode_edge_color('Louvain_Community', palette=colour_pallete, as_continuous=True)\
                  .encode_point_icon("Louvain_Community")\
                  .scene_settings(menu=False, info=False, show_arrows=False, point_size=0.7, edge_curvature=0.0, edge_opacity=0.5, point_opacity=0.9)\
                  .addStyle(bg={"color": 'white'})

    url = g.plot()
    print(url)
