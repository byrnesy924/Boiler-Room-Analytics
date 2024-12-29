# Script for getting genres from the Spotify or Discogs APIs
import os
import pandas as pd
import re
import logging
# import discogs_client
import spotipy
from rapidfuzz import fuzz
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout

logger = logging.getLogger(__name__)
logging.basicConfig(filename='Spotify_genre_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode="w")


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
    try:
        test_search = sp.search(q=url, type="track", limit=3)
    except ReadTimeout as timeout:
        # Ignore timeouts - assume that there is no data. Given the size of the data set this is no worry
        print(f"Timeout error.Data: {artist} - {track}")
        logger.error(f"Timeout error for {artist} - {track}. Error:\n {timeout}")
        return None
    except SpotifyException as e:
        # Spotify errors are worth catching
        # First catch is a 400 for a bad search URL - try remove track and just use artist to search
        if e.http_status == 400 and e.code == -1:
            try:
                # The embedded try except try is unideal but rather than abstracting the search on just artist
                # to a different function I have bodged the search just on artist here. The process is the same
                # if another spotify error, catch, log, return None
                url = rf"artist:{encoded_artist_name}"
                test_search = sp.search(q=url, type="track", limit=3)
            except SpotifyException as e_e:
                logger.error(f"Double spotify search error for artist {artist} and track {track}. Errors:\n{e}\n{e_e}")
                return None
        else:
            print(f"Check this error! unkown: {e}")
            logger.error(f"Caught an error: {e}")
            # Don't want to stop program - but I do want to manually check these.
            # Currently, the analysis is point in time, i.e. the data doesn't change. It is worth reviewing all errors to improve approach
            # In the future, to refresh, I don't think I will want to catch other errors - there is enough data for interesting insights

    # Process the results of the seach
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
            logger.info(f"Retrieved artist ID {artist_IDs} for {artist}")
            return artist_IDs
    # if no matches just return None
    logger.info(f"Found no artist for {artist}")
    return None


def format_dataframe_artists_to_match_spotify(df_of_artist: pd.DataFrame) -> pd.Series:
    df_of_artist["ListOfArtists"] = df_of_artist.apply(lambda x: x.to_list(), axis=1)
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(lambda x: [y for y in x if y is not None])
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(lambda x: sorted(x))
    return df_of_artist["ListOfArtists"].str.join(",")


def get_artist_genres_from_ID(sp: spotipy.Spotify, artists: list[str]) -> list[str]:
    if artists == "":
        logger.info("Artist was blank")
        return None
    logger.info(f"Found {[sp.artist(id).__dict__ for id in artists]} for artists: {artists}")
    return [sp.artist(id)["genres"] for id in artists]


def spotify_functional_flow(df: pd.DataFrame):
    """Wrapper for the functionality that gets artist genres using the spotify package"""
    load_dotenv(".env")

    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")

    # Spotify OAth flow
    scope = "user-library-read"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                   client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                   client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                                                   redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
                                                   open_browser=False,
                                                   ), requests_timeout=10)

    # Format multiple artists into alphabetical, concatenated with a comma (",")
    df["ArtistForSearch"] = format_dataframe_artists_to_match_spotify(df.filter(regex=r"Artist\d"))

    # get the ID of the song
    # dont do any ID artists or missing values, which appear as empty string after data formatting
    df["ArtistIDs"] = df.apply(
        lambda x: search_song_ID(sp, x["ArtistForSearch"], x["TrackName"]) if (x["TrackName"] != "" and x["Artist"] != "") else "", axis=1
        )

    df["ArtistGenre"] = df.apply(lambda x: get_artist_genres_from_ID(sp, x["ArtistIDs"]), axis=1)

    return df


if __name__ == "__main__":
    load_dotenv(".env")

    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")

    # Spotify OAth flow
    scope = "user-library-read"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                   client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                   client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                                                   redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
                                                   open_browser=False,
                                                   ), requests_timeout=10)

    # Format multiple artists into alphabetical, concatenated with a comma (",")
    df["ArtistForSearch"] = format_dataframe_artists_to_match_spotify(df.filter(regex=r"Artist\d"))

    # get the ID of the song
    # dont do any ID artists or missing values, which appear as empty string after data formatting
    df["ArtistIDs"] = df.apply(
        lambda x: search_song_ID(sp, x["ArtistForSearch"], x["TrackName"]) if (x["TrackName"] != "" and x["Artist"] != "") else "", axis=1
        )

    df["ArtistGenre"] = df.apply(lambda x: get_artist_genres_from_ID(sp, x["ArtistIDs"]), axis=1)
    print(df["ArtistGenre"])

    # get the genre of the ID
