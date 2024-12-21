# Script for getting genres from the Spotify or Discogs APIs
import os
import pandas as pd
import re
# import discogs_client
import spotipy
from rapidfuzz import fuzz
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv


def compare_spotify_return_result(track: str, artist: str, spotify_track: str, spotify_artist):
    """Logic for deciding if a track from search is a match"""
    # Hedge bets on artist - if its the right artist, then the genre is probably correct or pretty close
    if spotify_artist == artist:
        return True

    # scrub any versions from track names - hedge again that genres of remixes will be comparible
    remove_version = re.compile("\s\(.*\)")
    track = remove_version.sub("", track)
    spotify_track = remove_version.sub("", track)

    # If average string distance ratio is above 60%, then take the match
    if (fuzz.ratio(track, spotify_track) + fuzz.ratio(artist, spotify_artist))/2 > 0.6:
        return True

    return False


def search_song_ID(sp: spotipy.Spotify, artist: str, track: str) -> str | None:
    """Search Spotify API for the track ID. note artist needs to be sorted, concated with ,"""
    # Search using this!! https://developer.spotify.com/documentation/web-api/reference/search
    url_regex = re.compile(r"%s")
    encoded_track_name = url_regex.sub("%20", track)
    encoded_artist_name = url_regex.sub("%20", artist)
    url = rf"track:{encoded_track_name}%20artist:{encoded_artist_name}"
    test_search = sp.search(q=url, type="track", limit=3)

    results = test_search["tracks"]["items"]

    for result in results:
        # concat artists from spotify together in same format as search song ID
        artists = [artist["name"] for artist in result["artists"]]
        artists = sorted(artists)
        if len(artists) > 1:
            spotify_artist = ",".join(artists) 
        else:
            spotify_artist = artists[0]

        spotify_track_name = result["name"]

        # If match based on match logic
        if compare_spotify_return_result(track=track,
                                         artist=artist,
                                         spotify_track=spotify_track_name,
                                         spotify_artist=spotify_artist):
            # Update - cant get genre of TRACK, need to get genre of ARTIST only which sucks
            artist_IDs = [artist["id"] for artist in result["artists"]]
            return artist_IDs
    # if no matches just return None
    return None


def format_dataframe_artists_to_match_spotify(df_of_artist: pd.DataFrame) -> pd.Series:
    df_of_artist["ListOfArtists"] = df_of_artist.apply(lambda x: x.to_list(), axis=1)
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(lambda x: [y for y in x if y is not None])
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(lambda x: sorted(x))
    return df_of_artist["ListOfArtists"].str.join(",")


if __name__ == "__main__":
    load_dotenv(".env")

    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")

    # Spotify OAth flow
    scope = "user-library-read"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                   client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                   client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                                                   redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI")))

    # Format multiple artists into alphabetical, concatenated with a comma (",")
    df["ArtistForSearch"] = format_dataframe_artists_to_match_spotify(df.filter(regex="Artist\d"))

    # get the ID of the song
    # dont do any ID artists or missing values, which appear as empty string after data formatting
    df["ID"] = df.apply(lambda x: search_song_ID(sp, x["ArtistForSearch"], x["TrackName"]) if (x["TrackName"] != "" and x["Artist"] != "") else "", axis=1)

    print(df["ID"])

    # get the genre of the ID
