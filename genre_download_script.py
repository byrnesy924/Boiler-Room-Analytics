# Script for getting genres from the Spotify or Discogs APIs
import os
import pandas as pd
# import discogs_client
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv



if __name__ == "__main__":
    load_dotenv(".env")

    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")

    # Spotify OAth flow
    scope = "user-library-read"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                   client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                   client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")))


