# Script for getting genres from the Spotify or Discogs APIs
# use the "flow" functions in other scripts the way a utils.py normally works
import os
import pandas as pd
import re
import logging
import discogs_client
import spotipy
from rapidfuzz import fuzz
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
from discogs_client.models import Release
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout

logger = logging.getLogger(__name__)
logging.basicConfig(filename='Spotify_genre_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode="w")


def compare_spotify_return_result(track: str, artist: str, spotify_track: str, spotify_artist) -> bool:
    """Logic for deciding if a track from search is a match"""
    # Hedge bets on artist - if its the right artist, then the genre is probably correct or pretty close
    if spotify_artist == artist:
        return True

    # scrub any versions from track names - hedge again that genres of remixes will be comparible
    remove_version = re.compile(r"\s\(.*\)")
    track = remove_version.sub("", track)
    spotify_track = remove_version.sub("", spotify_track)

    # If average string distance ratio is above 60%, then take the match
    if (fuzz.ratio(track, spotify_track) + fuzz.ratio(artist, spotify_artist))/2 > 0.6:
        return True

    return False


def spotify_search_song_ID(sp: spotipy.Spotify, artist: str, track: str) -> str | None:
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


def spotify_get_artist_genres_from_ID(sp: spotipy.Spotify, artists: list[str]) -> list[str]:
    if artists == "":
        logger.info("Artist was blank")
        return None
    logger.info(f"Found {[sp.artist(id).__dict__ for id in artists]} for artists: {artists}")
    return [sp.artist(id)["genres"] for id in artists]


def spotify_functional_flow(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper for the functionality that gets artist genres using the spotify package"""
    load_dotenv(".env")

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
        lambda x: spotify_search_song_ID(sp, x["ArtistForSearch"], x["TrackName"]) if (x["TrackName"] != "" and x["Artist"] != "") else "",
        axis=1
        )

    df["ArtistGenre"] = df.apply(lambda x: spotify_get_artist_genres_from_ID(sp, x["ArtistIDs"]), axis=1)

    return df


def discogs_evaluate_search_result(artist: str, track: str, release_result: Release) -> bool:
    """"""
    """Logic for deciding if a track from search is a match"""
    # Hedge bets on artist - if its the right artist, then the genre is probably correct or pretty close
    if release_result.artists_sort == artist:
        # TODO check format of double artist
        return True

    # No scrub of remixes - Discogs has more exact results for obscure music; if its not there, its not there
    # If average string distance ratio is above 60%, then take the match
    if (fuzz.ratio(track, release_result.title) + fuzz.ratio(artist, release_result.artists_sort))/2 > 0.6:
        logger.info(f"Fuzzy match found: {artist} - {track} and {release_result.artists_sort} - {release_result.title}")
        return True

    return False


def discogs_search_track_artist(artist: str, track: str, d: discogs_client.Client) -> list[str]:
    """Wrapper function for searching and sifting through the results of discogs REST API"""

    results = d.search(track, artist=artist, type="release")
    # Bet - only do first page, I would rather faster with worse results at this stage given the volume of
    # data and the nature of the NLP following
    if len(results.page(1)) == 0:
        logger.warning(f"Discogs API did not return a valid search for {artist} - {track}")
        return None

    # Just do first page
    for result in results.page(1):
        # Approx 50 results per page
        if discogs_evaluate_search_result(artist, track, result):
            # If, by the logic in evaluate result, they are a match, return the genre
            logger.info(f"Discogs APi found a match for {artist} - {track}: {result.artists_sort} - {result.title}")
            return result.genres
    logger.warning(f"Didn't find a match for {artist} - {track}")
    return None


def discogs_functional_flow(df: pd.DataFrame) -> pd.DataFrame:
    """"""
    load_dotenv(".env")

    d = discogs_client.Client('Boiler_Room_Analytics/0.1', user_token=os.getenv("DISCOGS_USER_TOKEN"))

    # IF require formatting of artist, then do so here
    # Lorem Ipsum

    # Search
    df["DiscogsTrackID"] = df.loc[:, ["Artist", "TrackName"]].apply(
        lambda x: discogs_search_track_artist(x["Artist"], x["TrackName"], d=d) if (x["TrackName"] != "" and x["Artist"] != "") else "",
        axis=1
    )

    return df


if __name__ == "__main__":

    df = pd.read_parquet(r"Data\cleaned_boiler_room_data.parquet")
    df = discogs_functional_flow(df=df)

    df = spotify_functional_flow(df=df)

    df.to_parquet(r"Data\cleaned_boiler_room_data_with_genre.parquet")
