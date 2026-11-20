import matplotlib

matplotlib.use("Agg")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

import src.clean_br_data as cbd  # noqa: E402
from src.utils.data_loading import ProjectPaths  # noqa: E402

# --- Issue 2: mojibake fixes -------------------------------------------------------------------


def test_mojibake_fixes_are_all_pairs():
    assert all(isinstance(fix, tuple) and len(fix) == 2 for fix in cbd.MOJIBAKE_FIXES)


def test_fix_mojibake_does_not_corrupt_later_fixes():
    result = cbd.fix_mojibake(pd.Series(["Ã‚me", "Ã…re:gone", "Ã…MRTÃœM", "Bicep"]))
    assert result.tolist() == ["Ame", "Åre:gone", "ÅMRTÜM", "Bicep"]


# --- Issue 1: unknown placeholders become NaN ---------------------------------------------------


def test_null_unknown_names():
    series = pd.Series(["ID", "Id", "id", "?", "??", " ID ", "", "?ME", "Idris", "Who?", np.nan])
    result = cbd.null_unknown_names(series)
    assert result.isna().tolist() == [True] * 7 + [False, False, False, True]
    assert result.iloc[7:10].tolist() == ["?ME", "Idris", "Who?"]


def test_null_generic_remix_words():
    series = pd.Series(["Original", "extended club", "Radio", "VIP", "Ame", "Club Kid", np.nan])
    result = cbd.null_generic_remix_words(series)
    assert result.isna().tolist() == [True, True, True, True, False, False, True]


def test_null_unknown_names_all_nan_column():
    series = pd.Series([np.nan, np.nan])
    assert cbd.null_unknown_names(series).isna().all()


# --- Issue 3: splitting B2Bs / collabs ----------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ben UFO and Joy Orbison", ["Ben UFO", "Joy Orbison"]),
        ("Ben UFO And Joy Orbison", ["Ben UFO", "Joy Orbison"]),
        ("Ben UFO b2B Joy Orbison", ["Ben UFO", "Joy Orbison"]),
        ("Ben UFO x Joy Orbison", ["Ben UFO", "Joy Orbison"]),
        ("A & B & C", ["A", "B", "C"]),
        ("Echo & The Bunnymen", ["Echo & The Bunnymen"]),
        ("Brandon", ["Brandon"]),
    ],
)
def test_split_names(raw, expected):
    parts = cbd.split_names(pd.Series([raw]), "DJ")
    assert parts.iloc[0].dropna().tolist() == expected


def test_split_names_respects_no_split_list():
    series = pd.Series(["above & beyond & Bicep", "Chase & Status", np.nan])
    parts = cbd.split_names(series, "Artist", ["Above & Beyond", "Chase & Status"])
    assert list(parts.columns) == ["Artist0", "Artist1"]
    assert parts.iloc[0].tolist() == ["Above & Beyond", "Bicep"]
    assert parts.iloc[1].dropna().tolist() == ["Chase & Status"]
    assert parts.iloc[2].isna().all()


# --- Issue 4: fuzzy merging ---------------------------------------------------------------------


def _pairs(rows):
    return pd.DataFrame(rows, columns=["Artist1", "Artist2", "StringSimilarity"])


def test_score_artist_pairs_is_case_insensitive_and_counts_each_pair_once():
    names = ["Bicep", "BICEP", "Joy Orbison", "Objekt"]
    pairs, hist, _ = cbd.score_artist_pairs(names, review_threshold=90, chunk_size=2)
    assert hist.sum() == len(names) * (len(names) - 1) // 2
    assert pairs[["Artist1", "Artist2"]].to_numpy().tolist() == [["Bicep", "BICEP"]]


def test_build_merge_map_chains_transitively_to_most_frequent():
    pairs = _pairs([("A", "B", 90), ("B", "C", 90), ("A", "C", 50)])
    counts = pd.Series({"A": 1, "B": 5, "C": 2})
    assert cbd.build_merge_map(pairs, counts, threshold=80) == {"A": "B", "C": "B"}


def test_build_merge_map_tie_breaks_alphabetically():
    pairs = _pairs([("Zed", "Zedd", 90)])
    counts = pd.Series({"Zed": 3, "Zedd": 3})
    assert cbd.build_merge_map(pairs, counts) == {"Zedd": "Zed"}


def test_build_merge_map_overrides():
    pairs = _pairs([("A", "B", 90), ("C", "D", 10)])
    counts = pd.Series({"A": 2, "B": 1, "C": 2, "D": 1})
    overrides = pd.DataFrame(
        [("A", "B", "never"), ("C", "D", "always")], columns=["artist_a", "artist_b", "action"]
    )
    assert cbd.build_merge_map(pairs, counts, overrides=overrides) == {"D": "C"}


def test_load_merge_overrides_from_json(tmp_path):
    path = tmp_path / "overrides.json"
    path.write_text('{"never": [["A", "B"]], "always": [["C", "D"]]}', encoding="utf-8")
    overrides = cbd.load_merge_overrides(path)
    assert overrides.to_numpy().tolist() == [["A", "B", "never"], ["C", "D", "always"]]


def test_load_merge_overrides_missing_file(tmp_path):
    assert cbd.load_merge_overrides(tmp_path / "missing.json").empty


def test_load_merge_overrides_rejects_unknown_action(tmp_path):
    path = tmp_path / "overrides.json"
    path.write_text('{"sometimes": [["A", "B"]]}', encoding="utf-8")
    with pytest.raises(ValueError, match="sometimes"):
        cbd.load_merge_overrides(path)


# --- Issue 5: clean_data runs without globals and doesn't mutate input --------------------------


@pytest.fixture
def tmp_paths(tmp_path, monkeypatch):
    paths = ProjectPaths(root=tmp_path)
    paths.processed.dir.mkdir(parents=True)
    monkeypatch.setattr(cbd, "PATHS", paths)
    return paths


def test_clean_data_end_to_end(tmp_paths):
    raw = pd.DataFrame(
        {
            "Number": ["1", "2", "3"],
            "DJ": ["Ben UFO and Joy Orbison", "Ben UFO and Joy Orbison", "Ã‚me"],
            "Artist": ["Bicep", "ID", "BICEP & Objekt"],
            "TrackName": ["Bicep - Glue", "ID - ID", "X - Track (Ame Remix)"],
            "Genre": [np.nan, np.nan, "Techno"],
            "Date": ["2020", "2020", "2021"],
        },
        index=[0, 0, 1],  # duplicate index, as produced by pd.concat in main.py
    )
    raw_copy = raw.copy()

    result = cbd.clean_data(raw, save_visualisation=False, no_split_names=[], merge_overrides=None)

    pd.testing.assert_frame_equal(raw, raw_copy)  # input untouched
    assert len(result) == 3  # no row multiplication from the duplicate index
    assert result.loc[0, ["DJ0", "DJ1"]].tolist() == ["Ben UFO", "Joy Orbison"]
    assert result.loc[2, "DJ0"] == "Ame"
    assert pd.isna(result.loc[1, "Artist0"]) and pd.isna(result.loc[1, "TrackName"])
    assert result.loc[2, "RemixOrEdit0"] == "Ame"
    assert len(set(result["Artist0"].dropna())) == 1  # Bicep / BICEP merged
    assert not (result == "").any().any()
    assert tmp_paths.processed.fuzzy_matching.exists()
