# Script for getting genres from the Spotify or Discogs APIs
# use the "flow" functions in other scripts the way a utils.py normally works
import json
import logging
import os
import re
import time
from collections.abc import Iterable, MutableMapping
from pathlib import Path

import discogs_client
import numpy as np
import pandas as pd
import spotipy
from discogs_client.exceptions import HTTPError as DiscogsHTTPError
from discogs_client.models import Release
from dotenv import load_dotenv
from rapidfuzz import fuzz
from requests.exceptions import ReadTimeout, RequestException
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyOAuth

from src.utils.data_loading import PATHS

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="Spotify_genre_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)

MATCH_THRESHOLD = 60  # mean fuzz.ratio (0-100) of track and artist needed to accept a result
SPOTIFY_ARTIST_BATCH_SIZE = 50  # max IDs per sp.artists() call
DISCOGS_MAX_RETRIES = 3
DISCOGS_BACKOFF_SECONDS = 60  # Discogs allows ~60 authenticated requests per minute


class SearchError(Exception):
    """A transient search failure (timeout, rate limit, server error). Never cached."""


class JsonCache(dict):
    """A dict that checkpoints itself to a JSON file every `save_every` writes."""

    def __init__(self, path: Path | None, save_every: int = 25):
        super().__init__()
        self.path = path
        self.save_every = save_every
        self._unsaved = 0
        if path is not None and path.exists():
            self.update(json.loads(path.read_text(encoding="utf-8")))

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._unsaved += 1
        if self._unsaved >= self.save_every:
            self.save()

    def save(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)
        self._unsaved = 0


def pair_key(artist: str, track: str) -> str:
    """Unambiguous string key for an (artist, track) pair."""
    return json.dumps([artist, track], ensure_ascii=False)


def _as_id_list(value) -> list[str] | None:
    """Normalise an ArtistIDs cell (list, numpy array from parquet, None, "", NaN) to a list."""
    if isinstance(value, list | tuple | np.ndarray) and len(value) > 0:
        return list(value)
    return None


# --- Spotify ------------------------------------------------------------------------------------


def compare_spotify_return_result(
    track: str, artist: str, spotify_track: str, spotify_artist
) -> bool:
    """Logic for deciding if a track from search is a match"""
    # Hedge bets on artist - if its the right artist, then the genre is probably correct or close
    if spotify_artist == artist:
        return True

    # scrub any versions from track names - hedge again that genres of remixes will be comparable
    remove_version = re.compile(r"\s\(.*\)")
    track = remove_version.sub("", track)
    spotify_track = remove_version.sub("", spotify_track)

    mean_ratio = (fuzz.ratio(track, spotify_track) + fuzz.ratio(artist, spotify_artist)) / 2
    return mean_ratio > MATCH_THRESHOLD


def _spotify_search_artist_ids(sp: spotipy.Spotify, artist: str, track: str) -> list[str] | None:
    """Search Spotify for the track and return the matching result's artist IDs, or None if no
    result matches. Raises SearchError on failures worth retrying on a later run."""
    # https://developer.spotify.com/documentation/web-api/reference/search
    # spotipy URL-encodes the query itself.
    try:
        search = sp.search(q=f"track:{track} artist:{artist}", type="track", limit=3)
    except ReadTimeout as timeout:
        logger.error(f"Timeout error for {artist} - {track}. Error:\n {timeout}")
        raise SearchError from timeout
    except SpotifyException as e:
        if e.http_status == 400 and e.code == -1:
            # Bad search query - retry searching on the artist only
            try:
                search = sp.search(q=f"artist:{artist}", type="track", limit=3)
            except (SpotifyException, ReadTimeout) as e_e:
                logger.error(f"Double spotify search error for {artist} - {track}:\n{e}\n{e_e}")
                raise SearchError from e_e
        else:
            logger.error(f"Spotify error for {artist} - {track}: {e}")
            raise SearchError from e

    for result in search["tracks"]["items"]:
        # concat artists from spotify together in same format as ArtistForSearch
        spotify_artist = ",".join(sorted(a["name"] for a in result["artists"]))
        if compare_spotify_return_result(
            track=track, artist=artist, spotify_track=result["name"], spotify_artist=spotify_artist
        ):
            # Spotify only exposes genres for ARTISTS, not tracks
            artist_ids = [a["id"] for a in result["artists"]]
            logger.info(f"Retrieved artist ID {artist_ids} for {artist}")
            return artist_ids
    logger.info(f"Found no artist for {artist}")
    return None


def spotify_search_song_ID(sp: spotipy.Spotify, artist: str, track: str) -> list[str] | None:
    """Search Spotify for the track's artist IDs. Artist must be sorted and joined with ','.
    Returns None on no match or on any search error."""
    try:
        return _spotify_search_artist_ids(sp, artist, track)
    except SearchError:
        return None


def format_dataframe_artists_to_match_spotify(df_of_artist: pd.DataFrame) -> pd.Series:
    df_of_artist["ListOfArtists"] = df_of_artist.apply(lambda x: x.to_list(), axis=1)
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(
        lambda x: [y for y in x if pd.notna(y)]
    )
    df_of_artist["ListOfArtists"] = df_of_artist["ListOfArtists"].apply(lambda x: sorted(x))
    return df_of_artist["ListOfArtists"].str.join(",")


def spotify_fetch_artist_genres(
    sp: spotipy.Spotify, artist_ids: Iterable[str], genre_cache: MutableMapping[str, list[str]]
) -> MutableMapping[str, list[str]]:
    """Fetch genres for every artist ID not already in `genre_cache`, 50 IDs per request."""
    missing = [i for i in dict.fromkeys(artist_ids) if i not in genre_cache]
    for start in range(0, len(missing), SPOTIFY_ARTIST_BATCH_SIZE):
        batch = missing[start : start + SPOTIFY_ARTIST_BATCH_SIZE]
        try:
            response = sp.artists(batch)
        except (SpotifyException, ReadTimeout) as e:
            logger.error(f"Failed to fetch artists {batch}: {e}")
            continue
        for artist in response["artists"]:
            if artist:  # Spotify returns null for unknown IDs
                genre_cache[artist["id"]] = artist.get("genres", [])
    return genre_cache


def spotify_get_artist_genres_from_ID(
    sp: spotipy.Spotify,
    artists: list[str] | None,
    genre_cache: MutableMapping[str, list[str]] | None = None,
) -> list[list[str] | None] | None:
    """Genres for each artist ID (positional; None where the lookup failed).
    Returns None when there are no IDs."""
    ids = _as_id_list(artists)
    if ids is None:
        return None
    genre_cache = {} if genre_cache is None else genre_cache
    spotify_fetch_artist_genres(sp, ids, genre_cache)
    return [genre_cache.get(i) for i in ids]


def _spotify_client() -> spotipy.Spotify:
    load_dotenv(PATHS.env_file)
    auth = SpotifyOAuth(
        scope="user-library-read",
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        open_browser=False,
    )
    return spotipy.Spotify(auth_manager=auth, requests_timeout=10)


def spotify_functional_flow(
    df: pd.DataFrame,
    sp: spotipy.Spotify | None = None,
    search_cache_path: Path | None = None,
    genre_cache_path: Path | None = None,
) -> pd.DataFrame:
    """Get artist genres from Spotify. Searches once per unique (artist, track) pair and
    checkpoints results to JSON so an interrupted run resumes where it stopped."""
    sp = _spotify_client() if sp is None else sp
    search_cache = JsonCache(search_cache_path or PATHS.processed.spotify_search_cache)
    genre_cache = JsonCache(genre_cache_path or PATHS.processed.spotify_artist_genres_cache)

    # Format multiple artists into alphabetical, concatenated with a comma (",")
    df["ArtistForSearch"] = format_dataframe_artists_to_match_spotify(df.filter(regex=r"Artist\d"))
    searchable = df["TrackName"].notna() & df["Artist"].notna() & (df["ArtistForSearch"] != "")

    pairs = df.loc[searchable, ["ArtistForSearch", "TrackName"]].drop_duplicates()
    for artist, track in pairs.itertuples(index=False):
        key = pair_key(artist, track)
        if key in search_cache:
            continue
        try:
            search_cache[key] = _spotify_search_artist_ids(sp, artist, track)
        except SearchError:
            pass  # not cached, so retried on the next run
    search_cache.save()

    df["ArtistIDs"] = [
        search_cache.get(pair_key(a, t)) if s else None
        for a, t, s in zip(df["ArtistForSearch"], df["TrackName"], searchable, strict=True)
    ]

    all_ids = [i for cell in df["ArtistIDs"] for i in (_as_id_list(cell) or [])]
    spotify_fetch_artist_genres(sp, all_ids, genre_cache)
    genre_cache.save()
    df["ArtistGenre"] = df["ArtistIDs"].map(
        lambda ids: spotify_get_artist_genres_from_ID(sp, ids, genre_cache)
    )
    return df


# --- Discogs ------------------------------------------------------------------------------------


def discogs_evaluate_search_result(artist: str, track: str, release_result: Release) -> bool:
    """Logic for deciding if a release from search is a match"""
    # Hedge bets on artist - if its the right artist, then the genre is probably correct or close
    if release_result.artists_sort == artist:
        # TODO check format of double artist
        return True

    # No scrub of remixes - Discogs has more exact results for obscure music
    mean_ratio = (
        fuzz.ratio(track, release_result.title) + fuzz.ratio(artist, release_result.artists_sort)
    ) / 2
    if mean_ratio > MATCH_THRESHOLD:
        logger.info(
            f"Fuzzy match found: {artist} - {track} and "
            f"{release_result.artists_sort} - {release_result.title}"
        )
        return True
    return False


def _discogs_search_genres(
    artist: str,
    track: str,
    d: discogs_client.Client,
    max_retries: int = DISCOGS_MAX_RETRIES,
    backoff_seconds: float = DISCOGS_BACKOFF_SECONDS,
) -> list[str] | None:
    """Genres of the first matching release, or None if nothing matches.
    Retries on HTTP 429 with linear backoff; raises SearchError on other failures."""
    for attempt in range(max_retries + 1):
        try:
            # Only the first page (~50 results): faster, and good enough for the NLP that follows
            page = d.search(track, artist=artist, type="release").page(1)
            if len(page) == 0:
                logger.warning(f"Discogs API did not return a valid search for {artist} - {track}")
                return None
            for result in page:
                if discogs_evaluate_search_result(artist, track, result):
                    logger.info(
                        f"Discogs found a match for {artist} - {track}: "
                        f"{result.artists_sort} - {result.title}"
                    )
                    return list(result.genres)
            logger.warning(f"Didn't find a match for {artist} - {track}")
            return None
        except DiscogsHTTPError as e:
            if e.status_code == 429 and attempt < max_retries:
                wait = backoff_seconds * (attempt + 1)
                logger.warning(f"Discogs rate limit hit; sleeping {wait}s")
                time.sleep(wait)
                continue
            logger.error(f"Discogs error for {artist} - {track}: {e}")
            raise SearchError from e
        except RequestException as e:
            logger.error(f"Discogs request failed for {artist} - {track}: {e}")
            raise SearchError from e
    raise SearchError("unreachable")  # pragma: no cover


def discogs_search_track_artist(artist: str, track: str, d: discogs_client.Client) -> list[str]:
    """Genres of the first matching Discogs release; None on no match or any error."""
    try:
        return _discogs_search_genres(artist, track, d)
    except SearchError:
        return None


def _discogs_client() -> discogs_client.Client:
    load_dotenv(PATHS.env_file)
    return discogs_client.Client(
        "Boiler_Room_Analytics/0.1", user_token=os.getenv("DISCOGS_USER_TOKEN")
    )


def discogs_functional_flow(
    df: pd.DataFrame,
    d: discogs_client.Client | None = None,
    cache_path: Path | None = None,
) -> pd.DataFrame:
    """Get release genres from Discogs. Searches once per unique (artist, track) pair and
    checkpoints results to JSON so an interrupted run resumes where it stopped."""
    d = _discogs_client() if d is None else d
    cache = JsonCache(cache_path or PATHS.processed.discogs_search_cache)

    searchable = df["TrackName"].notna() & df["Artist"].notna()
    pairs = df.loc[searchable, ["Artist", "TrackName"]].drop_duplicates()
    for artist, track in pairs.itertuples(index=False):
        key = pair_key(artist, track)
        if key in cache:
            continue
        try:
            cache[key] = _discogs_search_genres(artist, track, d)
        except SearchError:
            pass  # not cached, so retried on the next run
    cache.save()

    df["DiscogsGenres"] = [
        cache.get(pair_key(a, t)) if s else None
        for a, t, s in zip(df["Artist"], df["TrackName"], searchable, strict=True)
    ]
    return df


if __name__ == "__main__":
    df = pd.read_parquet(PATHS.processed.cleaned_boiler_room_data_parquet)
    df = discogs_functional_flow(df=df)
    df = spotify_functional_flow(df=df)
    df.to_parquet(PATHS.processed.cleaned_boiler_room_data_with_genre)
