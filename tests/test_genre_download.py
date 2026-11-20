import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from discogs_client.exceptions import HTTPError as DiscogsHTTPError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from spotipy.exceptions import SpotifyException

import src.genre_download_script as gds

# --- Fakes --------------------------------------------------------------------------------------


def _track(name, artists):
    return {"name": name, "artists": [{"name": a, "id": f"id-{a}"} for a in artists]}


class FakeSpotify:
    def __init__(self, search_results=None, search_errors=None):
        self.search_results = search_results or {}  # query -> list of track items
        self.search_errors = search_errors or {}  # query -> exception
        self.search_calls = []
        self.artists_calls = []

    def search(self, q, type, limit):  # noqa: A002 - mirrors spotipy signature
        self.search_calls.append(q)
        if q in self.search_errors:
            raise self.search_errors[q]
        return {"tracks": {"items": self.search_results.get(q, [])}}

    def artists(self, ids):
        self.artists_calls.append(list(ids))
        return {"artists": [{"id": i, "genres": [f"genre-{i}"]} for i in ids]}


def _release(artists_sort, title, genres=("Electronic",)):
    return SimpleNamespace(artists_sort=artists_sort, title=title, genres=list(genres))


class FakeDiscogs:
    def __init__(self, pages=None, errors=None):
        self.pages = pages or {}  # (artist, track) -> list of releases
        self.errors = list(errors or [])  # exceptions raised by successive calls, then pages
        self.calls = []

    def search(self, track, artist, type):  # noqa: A002 - mirrors discogs_client signature
        self.calls.append((artist, track))
        if self.errors:
            raise self.errors.pop(0)
        results = self.pages.get((artist, track), [])
        return SimpleNamespace(page=lambda n: results)


# --- Issue 10: thresholds -----------------------------------------------------------------------


def test_spotify_compare_rejects_unrelated_result():
    assert not gds.compare_spotify_return_result(
        "Glue", "Bicep", "Completely Different", "Someone Else"
    )


def test_spotify_compare_accepts_close_result():
    assert gds.compare_spotify_return_result("Glue (Original)", "Bicep", "Glue", "Bicepp")


def test_discogs_evaluate_rejects_unrelated_result():
    assert not gds.discogs_evaluate_search_result(
        "Bicep", "Glue", _release("Someone Else", "Completely Different")
    )


def test_discogs_evaluate_accepts_exact_artist():
    assert gds.discogs_evaluate_search_result("Bicep", "Glue", _release("Bicep", "Other"))


# --- Issues 11 & 12: Spotify search / genre lookups ---------------------------------------------


@pytest.mark.parametrize("ids", [None, "", [], np.nan, np.array([])])
def test_genres_from_missing_ids_is_none(ids):
    sp = FakeSpotify()
    assert gds.spotify_get_artist_genres_from_ID(sp, ids) is None
    assert sp.artists_calls == []


def test_genres_from_ids_accepts_numpy_array():
    sp = FakeSpotify()
    result = gds.spotify_get_artist_genres_from_ID(sp, np.array(["a", "b"]))
    assert result == [["genre-a"], ["genre-b"]]


def test_non_400_spotify_error_returns_none():
    query = "track:Glue artist:Bicep"
    sp = FakeSpotify(search_errors={query: SpotifyException(429, -1, "rate limited")})
    assert gds.spotify_search_song_ID(sp, "Bicep", "Glue") is None


def test_timeout_returns_none():
    sp = FakeSpotify(search_errors={"track:Glue artist:Bicep": ReadTimeout()})
    assert gds.spotify_search_song_ID(sp, "Bicep", "Glue") is None


def test_400_falls_back_to_artist_search():
    sp = FakeSpotify(
        search_results={"artist:Bicep": [_track("Glue", ["Bicep"])]},
        search_errors={"track:Glue artist:Bicep": SpotifyException(400, -1, "bad query")},
    )
    assert gds.spotify_search_song_ID(sp, "Bicep", "Glue") == ["id-Bicep"]
    assert sp.search_calls == ["track:Glue artist:Bicep", "artist:Bicep"]


def test_fetch_artist_genres_batches_and_caches():
    sp = FakeSpotify()
    ids = [f"a{i}" for i in range(120)] + ["a0", "a1"]  # duplicates fetched once
    cache = {}
    gds.spotify_fetch_artist_genres(sp, ids, cache)
    assert [len(batch) for batch in sp.artists_calls] == [50, 50, 20]
    assert len(cache) == 120

    gds.spotify_fetch_artist_genres(sp, ids, cache)
    assert len(sp.artists_calls) == 3  # nothing refetched


# --- Spotify flow: dedupe + checkpoint resume ---------------------------------------------------


def _df():
    return pd.DataFrame(
        {
            "Artist": ["Bicep", "Bicep", "Bicep", np.nan],
            "Artist0": ["Bicep", "Bicep", "Bicep", np.nan],
            "TrackName": ["Glue", "Glue", "Apricots", "Track"],
        }
    )


def test_spotify_flow_dedupes_and_maps_back(tmp_path):
    sp = FakeSpotify(search_results={"track:Glue artist:Bicep": [_track("Glue", ["Bicep"])]})
    df = gds.spotify_functional_flow(
        _df(), sp=sp, search_cache_path=tmp_path / "s.json", genre_cache_path=tmp_path / "g.json"
    )
    assert sorted(sp.search_calls) == ["track:Apricots artist:Bicep", "track:Glue artist:Bicep"]
    assert df["ArtistIDs"].tolist() == [["id-Bicep"], ["id-Bicep"], None, None]
    assert df["ArtistGenre"].tolist() == [[["genre-id-Bicep"]], [["genre-id-Bicep"]], None, None]
    assert sp.artists_calls == [["id-Bicep"]]


def test_spotify_flow_resumes_from_cache_and_retries_errors(tmp_path):
    search_cache = tmp_path / "s.json"
    glue = gds.pair_key("Bicep", "Glue")
    search_cache.write_text(json.dumps({glue: ["id-Bicep"]}), encoding="utf-8")
    sp = FakeSpotify(
        search_errors={"track:Apricots artist:Bicep": SpotifyException(500, -1, "server")}
    )

    gds.spotify_functional_flow(
        _df(), sp=sp, search_cache_path=search_cache, genre_cache_path=tmp_path / "g.json"
    )

    assert sp.search_calls == ["track:Apricots artist:Bicep"]  # Glue came from the cache
    saved = json.loads(search_cache.read_text(encoding="utf-8"))
    assert saved == {glue: ["id-Bicep"]}  # failed search not cached, so retried next run


# --- Discogs: retry, errors, dedupe, rename -----------------------------------------------------


def test_discogs_retries_on_429(monkeypatch):
    sleeps = []
    monkeypatch.setattr(gds.time, "sleep", sleeps.append)
    d = FakeDiscogs(
        pages={("Bicep", "Glue"): [_release("Bicep", "Glue", ["Electronic"])]},
        errors=[DiscogsHTTPError("Too many", 429), DiscogsHTTPError("Too many", 429)],
    )
    assert gds.discogs_search_track_artist("Bicep", "Glue", d) == ["Electronic"]
    assert len(d.calls) == 3
    assert sleeps == [gds.DISCOGS_BACKOFF_SECONDS, 2 * gds.DISCOGS_BACKOFF_SECONDS]


def test_discogs_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(gds.time, "sleep", lambda s: None)
    d = FakeDiscogs(errors=[DiscogsHTTPError("Too many", 429)] * (gds.DISCOGS_MAX_RETRIES + 1))
    assert gds.discogs_search_track_artist("Bicep", "Glue", d) is None
    assert len(d.calls) == gds.DISCOGS_MAX_RETRIES + 1


@pytest.mark.parametrize(
    "error", [DiscogsHTTPError("Server error", 500), RequestsConnectionError("offline")]
)
def test_discogs_other_errors_return_none(error):
    assert gds.discogs_search_track_artist("Bicep", "Glue", FakeDiscogs(errors=[error])) is None


def test_discogs_flow_dedupes_and_names_column(tmp_path):
    d = FakeDiscogs(pages={("Bicep", "Glue"): [_release("Bicep", "Glue", ["Electronic"])]})
    df = gds.discogs_functional_flow(_df(), d=d, cache_path=tmp_path / "d.json")
    assert sorted(d.calls) == [("Bicep", "Apricots"), ("Bicep", "Glue")]
    assert "DiscogsTrackID" not in df.columns
    assert df["DiscogsGenres"].tolist() == [["Electronic"], ["Electronic"], None, None]


def test_discogs_flow_resumes_from_cache(tmp_path):
    cache_path = tmp_path / "d.json"
    d = FakeDiscogs(pages={("Bicep", "Glue"): [_release("Bicep", "Glue")]})
    gds.discogs_functional_flow(_df(), d=d, cache_path=cache_path)

    d2 = FakeDiscogs()
    df = gds.discogs_functional_flow(_df(), d=d2, cache_path=cache_path)
    assert d2.calls == []
    assert df["DiscogsGenres"].iloc[0] == ["Electronic"]
