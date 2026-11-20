import json
import logging
import re
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from rapidfuzz import fuzz
from rapidfuzz.process import cdist

from src.utils.data_loading import PATHS

# TODO - remove "Unreleased" from names and artists

logger = logging.getLogger(__name__)

# Encoding errors caught by hand.
# ?-Ziq --> Mu-Ziq; ?Ztek -> AZtek; ?kkord --> Akkord; Ã˜ [Phase] --> Ø [Phase];
# RÃ¸dhÃ¥d --> Rødhåd;
# Ã‚me --> Ame; Ã…re:gone --> Åre:gone; Ã…MRTÃœM --> ÅMRTÜM; Ã…NTÃ†GÃ˜NIST --> ÅNTÆGØNIST
MOJIBAKE_FIXES: list[tuple[str, str]] = [
    ("?-Ziq", "Mu-Ziq"),
    ("?Ztek", "AZtek"),
    ("?kkord", "Akkord"),
    ("Ã˜ [Phase]", "Ø [Phase]"),
    ("RÃ¸dhÃ¥d", "Rødhåd"),
    ("Ã‚me", "Ame"),
    ("Ã…re:gone", "Åre:gone"),
    ("Ã…MRTÃœM", "ÅMRTÜM"),
    ("Ã…NTÃ†GÃ˜NIST", "ÅNTÆGØNIST"),
    ("ÃŽÂ¼-Ziq", "Mu-Ziq"),
    ("Î¼-Ziq", "Mu-Ziq"),
]

# Whole-cell placeholders for an unknown artist/track: "?", "??", "ID", "Id", "id", or blank.
# "?ME" is a real artist and is deliberately not matched.
UNKNOWN_NAME_PATTERN = r"\s*(?:\?+|id)?\s*"

# Separators between multiple artists / B2B DJs. Not split when followed by "the"
# (e.g. "Echo & The Bunnymen", "Kool & The Gang").
NAME_SPLIT_PATTERN = re.compile(r"\s+(?:&|and|b2b|x)\s+(?!the\b)", re.IGNORECASE)

# Words left behind in RemixOrEdit once "Mix"/"Edit"/etc. are stripped, e.g. "(Original Mix)"
# -> "Original". A cell made up only of these words is not a remixer.
GENERIC_REMIX_PATTERN = (
    r"\s*(?:(?:original|extended|radio|club|album|single|short|long|full|main|clean|explicit"
    r"|vip)\s*)+"
)

FUZZY_MATCH_THRESHOLD = 80  # pairs scoring above this are merged
FUZZY_REVIEW_THRESHOLD = 65  # pairs scoring above this are saved for manual review

ARTIST_COL_REGEX = r"(DJ\d+)|(RemixOrEdit\d+)|(Artist\d+)"


def identify_remix_or_edit(track_name):
    """Superseded by pandas str functions"""
    regex_get_remix = re.compile(r"\(.*\)")
    return regex_get_remix.findall(track_name)


def regression_check_cleaning(df_start: pd.DataFrame, df_end: pd.DataFrame) -> pd.DataFrame:
    """Finds values that were wiped from the dataframe for human assessment"""
    df_end = df_end[df_start.columns]  # only check matching columns
    condition = df_start.notna() & df_end.isna()
    return df_start.loc[condition.any(axis=1), :]


def load_no_split_names(path: Path | None = None) -> list[str]:
    """Names that must never be split into multiple artists. Lines starting with '#' are ignored."""
    path = path or PATHS.resources.no_split_names
    if not path.exists():
        return []
    lines = (line.strip() for line in path.read_text(encoding="utf-8").splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def load_merge_overrides(path: Path | None = None) -> pd.DataFrame:
    """Manual fuzzy-merge overrides, read from JSON of the form
    ``{"never": [["Artist A", "Artist B"], ...], "always": [["Artist C", "Artist D"], ...]}``.

    Returns a DataFrame with columns artist_a, artist_b, action (never | always).
    """
    path = path or PATHS.resources.artist_merge_overrides
    columns = ["artist_a", "artist_b", "action"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    raw = json.loads(path.read_text(encoding="utf-8"))
    unknown_actions = set(raw) - {"never", "always"}
    if unknown_actions:
        raise ValueError(f"Unknown override actions in {path}: {unknown_actions}")
    rows = []
    for action, pairs in raw.items():
        for pair in pairs:
            if len(pair) != 2:
                raise ValueError(f"Override in {path} must be a pair of names, got {pair}")
            rows.append((pair[0], pair[1], action))
    return pd.DataFrame(rows, columns=columns)


def fix_mojibake(series: pd.Series) -> pd.Series:
    for broken, fixed in MOJIBAKE_FIXES:
        series = series.str.replace(broken, fixed, regex=False)
    return series


def null_unknown_names(series: pd.Series) -> pd.Series:
    """Replace whole-cell unknown placeholders (?, ID, blank) with NaN."""
    if series.isna().all():
        return series
    is_unknown = series.str.fullmatch(UNKNOWN_NAME_PATTERN, case=False)
    return series.mask(is_unknown.fillna(False).astype(bool))


def null_generic_remix_words(series: pd.Series) -> pd.Series:
    """Replace RemixOrEdit leftovers like "Original" or "Extended Club" with NaN."""
    if series.isna().all():
        return series
    is_generic = series.str.fullmatch(GENERIC_REMIX_PATTERN, case=False)
    return series.mask(is_generic.fillna(False).astype(bool))


def split_names(series: pd.Series, prefix: str, no_split: list[str] = ()) -> pd.DataFrame:
    """Split collabs/B2Bs into columns prefix0, prefix1, ... leaving `no_split` names intact."""
    protected = series.copy()
    tokens = {}
    for i, name in enumerate(no_split):
        token = f"NOSPLIT{i}TOKEN"
        tokens[token] = name
        protected = protected.str.replace(re.escape(name), token, case=False, regex=True)

    parts = protected.str.split(NAME_SPLIT_PATTERN, expand=True, regex=True)
    for col in parts.columns:
        if parts[col].isna().all():
            continue
        parts[col] = parts[col].str.strip().str.replace(r"\s+", " ", regex=True)
        for token, name in tokens.items():
            parts[col] = parts[col].str.replace(token, name, regex=False)
    parts.columns = [f"{prefix}{i}" for i in range(parts.shape[1])]
    return parts


def score_artist_pairs(
    names: list[str], review_threshold: float = FUZZY_REVIEW_THRESHOLD, chunk_size: int = 1000
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Case-insensitive fuzz.ratio over every unique pair of names, computed in chunks.

    Returns pairs scoring above `review_threshold`, plus a 30-bin histogram (counts, bin edges)
    over all unique pairs.
    """
    names_arr = np.asarray(names, dtype=object)
    lowered = [name.lower() for name in names]
    bin_edges = np.linspace(0, 100, 31)
    hist = np.zeros(30, dtype=np.int64)
    pair_frames = []

    for start in range(0, len(lowered), chunk_size):
        chunk = lowered[start : start + chunk_size]
        scores = cdist(chunk, lowered, scorer=fuzz.ratio, dtype=np.float32, workers=-1)
        row_ids = np.arange(start, start + len(chunk))[:, None]
        upper = (
            np.arange(len(lowered))[None, :] > row_ids
        )  # each unordered pair once, no self pairs
        hist += np.histogram(scores[upper], bins=bin_edges)[0]

        i, j = np.nonzero(upper & (scores > review_threshold))
        pair_frames.append(
            pd.DataFrame(
                {
                    "Artist1": names_arr[i + start],
                    "Artist2": names_arr[j],
                    "StringSimilarity": scores[i, j],
                }
            )
        )

    pairs = (
        pd.concat(pair_frames, ignore_index=True)
        if pair_frames
        else pd.DataFrame(columns=["Artist1", "Artist2", "StringSimilarity"])
    )
    return pairs, hist, bin_edges


def build_merge_map(
    pairs: pd.DataFrame,
    name_counts: pd.Series,
    threshold: float = FUZZY_MATCH_THRESHOLD,
    overrides: pd.DataFrame | None = None,
) -> dict[str, str]:
    """Cluster similar names (connected components) and map each to its cluster's canonical name.

    Canonical name = most frequent spelling in the cluster (ties broken alphabetically).
    Overrides: `never` removes the direct link between two names; `always` adds one.
    """
    parent: dict[str, str] = {}

    def find(name: str) -> str:
        parent.setdefault(name, name)
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    def union(a: str, b: str) -> None:
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    never, always = set(), []
    if overrides is not None:
        for a, b, action in overrides[["artist_a", "artist_b", "action"]].itertuples(index=False):
            if action == "never":
                never.add(frozenset((a, b)))
            else:
                always.append((a, b))

    links = pairs.loc[pairs["StringSimilarity"] > threshold, ["Artist1", "Artist2"]]
    for a, b in links.itertuples(index=False):
        if frozenset((a, b)) not in never:
            union(a, b)
    for a, b in always:
        union(a, b)

    for pair in never:
        if len(pair) != 2:
            continue
        a, b = tuple(pair)
        if a in parent and b in parent and find(a) == find(b):
            logger.warning(f"'{a}' and '{b}' are never-merge but are linked via other names")

    clusters: dict[str, list[str]] = {}
    for name in parent:
        clusters.setdefault(find(name), []).append(name)

    merge_map = {}
    for members in clusters.values():
        canonical = min(members, key=lambda n: (-name_counts.get(n, 0), n))
        merge_map.update({m: canonical for m in members if m != canonical})
    return merge_map


def _plot_histograms(
    pairs: pd.DataFrame, hist: np.ndarray, bin_edges: np.ndarray, save_visualisation: bool
) -> None:
    fig, ax = plt.subplots(figsize=(40, 40))
    ax.stairs(hist, bin_edges, fill=True)
    ax.axvline(FUZZY_MATCH_THRESHOLD, color="red", linestyle="--", linewidth=2)
    ax.set_title("Histogram of Values")
    ax.set_yscale("log")  # to try elucidate the tail
    ax.set_xlabel("Value")
    ax.set_ylabel("Frequency")
    plt.show()
    if save_visualisation:
        fig.savefig(PATHS.images.similarity_histogram)

    fig_tail, ax_tail = plt.subplots(figsize=(40, 40))
    ax_tail.axvline(FUZZY_MATCH_THRESHOLD, color="red", linestyle="--", linewidth=2)
    ax_tail.set_title("Tail of Histogram of Values")
    ax_tail.set_yscale("log")
    ax_tail.set_xlabel("Value")
    ax_tail.set_ylabel("Frequency")
    pairs["StringSimilarity"].hist(ax=ax_tail, bins=30)
    plt.show()
    if save_visualisation:
        fig_tail.savefig(PATHS.images.similarity_histogram_tail)


def clean_data(
    df: pd.DataFrame,
    save_visualisation: bool = True,
    no_split_names: list[str] | None = None,
    merge_overrides: pd.DataFrame | None = None,
) -> pd.DataFrame:
    # Work on a copy with a clean index (main.py concatenates per-set frames, which repeats index
    # values and would make the column merges below multiply rows).
    df = df.reset_index(drop=True).copy()
    raw_df = df.copy()
    no_split_names = load_no_split_names() if no_split_names is None else no_split_names
    merge_overrides = load_merge_overrides() if merge_overrides is None else merge_overrides

    # Remove artist from the track name - regex remove everything before "\-"
    df["TrackName"] = df["TrackName"].str.replace(r".*\-\s", "", regex=True)
    df["TrackName"] = df["TrackName"].str.strip()
    df["TrackName"] = df["TrackName"].str.replace(r"\s+", " ", regex=True)
    # From here we don't really care about the track name except to search beatport.

    df["Artist"] = fix_mojibake(df["Artist"])
    df["DJ"] = fix_mojibake(df["DJ"])

    df["RemixOrEdit"] = df["TrackName"].str.extract(r"\((.*?)\)", expand=False)
    df["RemixOrEdit"] = df["RemixOrEdit"].str.replace(
        r"(Remix)|(\()|(\))|(Edit)|(bootleg)|(Version)|(Vocal Mix)|(Mix)"
        r"|(Instrumental Mix)|(Instrumental)|(Dub)",
        "",
        case=False,
        regex=True,
    )
    df["RemixOrEdit"] = null_generic_remix_words(df["RemixOrEdit"].str.strip())

    # Split collabs, B2Bs and multiple remixers into numbered columns
    df = pd.concat(
        [
            df,
            split_names(df["Artist"], "Artist", no_split_names),
            split_names(df["DJ"], "DJ", no_split_names),
            split_names(df["RemixOrEdit"], "RemixOrEdit", no_split_names),
        ],
        axis=1,
    )

    # Unknown artists/tracks become NaN so they never form a node
    name_cols = ["Artist", "DJ", "TrackName", "RemixOrEdit", *df.filter(regex=ARTIST_COL_REGEX)]
    for col in name_cols:
        df[col] = null_unknown_names(df[col])

    wiped = regression_check_cleaning(df_start=raw_df, df_end=df)
    print(f"{len(wiped)} rows lost at least one value during cleaning, e.g.:\n{wiped.head(10)}")
    df.to_csv(PATHS.processed.preprocessed_br_data, encoding="utf-8")

    # Fuzzy matching of artist names. This is a static, point-in-time dataset, so score every
    # pair, choose a cutoff by inspecting the distribution, and merge clusters above it.
    artist_cols = df.filter(regex=ARTIST_COL_REGEX).columns
    start = perf_counter()
    name_counts = pd.concat([df[col] for col in artist_cols]).value_counts()
    pairs, hist, bin_edges = score_artist_pairs(name_counts.index.tolist())
    print(f"Took {(perf_counter() - start) / 60:.2f}mins to do string similarity.")

    _plot_histograms(pairs, hist, bin_edges, save_visualisation)
    pairs.to_csv(PATHS.processed.fuzzy_matching, index=False)

    merge_map = build_merge_map(pairs, name_counts, overrides=merge_overrides)
    print(f"Merging {len(merge_map)} artist spellings into canonical names.")
    for col in artist_cols:
        df[col] = df[col].map(merge_map).fillna(df[col])

    print(f"Took {(perf_counter() - start) / 60:.2f}mins to do all of artist fuzzy matching.")
    return df


if __name__ == "__main__":
    raw_df = pd.read_csv(PATHS.raw.tracklist_1001_set_lists)
    final_df = clean_data(raw_df, save_visualisation=True)

    final_df.to_csv(PATHS.processed.cleaned_boiler_room_data_csv, encoding="utf-8", index=False)
    final_df.to_parquet(PATHS.processed.cleaned_boiler_room_data_parquet, index=False)
    # number of missing genres - whether it is worth getting these from the beatport API
    print(final_df.isna().sum())
