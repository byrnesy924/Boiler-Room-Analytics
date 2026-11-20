# Graph Schema

> **Status: describes the graph AS CURRENTLY IMPLEMENTED (as of 2026-11-20), not the intended design.**
> Derived from `src/clean_br_data.py`, `src/network_analytics.py`, `src/main.py` and `src/genre_download_script.py`.
> Open defects are listed in [Known issues](#known-issues).
>
> **If you change how nodes, edges, or their attributes are built, update this file in the same change.**

## Overview

| Property | Value |
|---|---|
| Node types | 1 (artist name — the role is carried by the edge type, not the node) |
| Edge types | 4: `played`, `remixed`, `collab`, `b2b` (column `EdgeType`) |
| Direction | Directed (`played`, `remixed`); undirected (`collab`, `b2b`, stored once with `Source < Target`) |
| Multiplicity | One row per (`Source`, `Target`, `EdgeType`); parallel plays are collapsed into `Weight` |
| Weights | `Weight` = number of track rows (plays) that produced the edge |
| Self-loops | Dropped |
| Community detection | Weighted Louvain (`nx.community.louvain_communities`, `weight="Weight"`, `seed=SEED`) on a directed projection |
| Visualisation | Graphistry (`show_arrows=False`) |

Builders (all in `src/network_analytics.py`, shared by `network_analytics.py` `__main__` and `main.py`):
- `build_play_edges(df)` → one typed edge per track row and relation (a multigraph edge list with track details).
- `aggregate_edges(play_edges)` → collapses parallel edges into `Weight`.
- `create_edglist(df)` → `(edges, nodes)`: `aggregate_edges(build_play_edges(df))` plus the node list.
- `assign_communities(edges, nodes, seed=SEED)` → `(edges, nodes)` with community columns.
- `to_multidigraph(edges)` → `nx.MultiDiGraph` keyed by `EdgeType`.
- `to_weighted_digraph(edges)` → `nx.DiGraph` used for Louvain (see [Direction, multiplicity, weights](#direction-multiplicity-weights)).

`SEED = 42` lives in `src/config.py`.

## Upstream: track rows (`clean_br_data.clean_data`)

The graph is built from a table where **one row = one track played in one Boiler Room set**.
Columns consumed by the graph:

| Column | Origin | Derivation |
|---|---|---|
| `DJ` | scraper | Raw set performer string (unsplit) |
| `Artist` | scraper | Raw track artist string (unsplit) |
| `TrackName` | scraper | Artist prefix (`... - `) stripped, whitespace normalised |
| `Genre` | scraper | 1001tracklists genre, as scraped |
| `Date` | scraper | Set date |
| `RemixOrEdit` | `TrackName` | Text inside first `(...)`, with Remix/Edit/bootleg/Version/Mix/Dub etc. removed. Cells left with only generic words (`Original`, `Extended`, `Radio`, `Club`, `VIP`, … — `GENERIC_REMIX_PATTERN`) become NaN. |
| `Artist0..N` | `Artist` | `split_names`: split on ` & `, ` and `, ` x `, ` b2b ` (case-insensitive) |
| `DJ0..N` | `DJ` | Same split (B2B partners) |
| `RemixOrEdit0..N` | `RemixOrEdit` | Same split (co-remixers) |

Splitting (`NAME_SPLIT_PATTERN`) does not split when the separator is followed by "the" (e.g. `Kool & The Gang`), nor inside names listed in `src/resources/no_split_names.txt`.

Name normalisation:
1. A hand-written mojibake fix list (`MOJIBAKE_FIXES`), applied to `Artist`/`DJ` before splitting.
2. Whole-cell unknown placeholders — `?`, `??`, `ID`/`Id`/`id`, blank — become NaN in `Artist`, `DJ`, `TrackName`, `RemixOrEdit` and all split columns. `?` inside a name is kept (`?ME`).
3. Fuzzy de-duplication of `Artist\d+`, `DJ\d+`, `RemixOrEdit\d+`: every pair of names is scored with case-insensitive `rapidfuzz.fuzz.ratio`; pairs scoring `> 80` are linked, linked names are grouped into clusters (connected components, so A~B and B~C put A, B, C together), and every name in a cluster is replaced by its most frequent spelling (ties: alphabetical). Manual overrides in `src/resources/artist_merge_overrides.json` (`never` removes a direct link, `always` adds one).

Output: `data/processed/cleaned_boiler_room_data.{csv,parquet}`.

### Genre enrichment (`genre_download_script.py`)

Run in `main.py` (Discogs then Spotify) and in the script's own `__main__` (reads `data/processed/cleaned_boiler_room_data.parquet`, writes `data/processed/cleaned_boiler_room_data_with_genre.parquet`). Each unique (artist, track) pair is searched once; results are checkpointed to JSON caches in `data/processed/` (`spotify_search_cache.json`, `spotify_artist_genres_cache.json`, `discogs_search_cache.json`) so a rerun resumes. Adds row-level columns:

| Column | Type | Contents |
|---|---|---|
| `DiscogsGenres` | `list[str]` \| `None` | Discogs release *genres* (`Release.genres`) of the first matching release. `None` when `Artist`/`TrackName` is missing, no match was found, or the search failed. |
| `ArtistForSearch` | `str` | Non-null `Artist\d` values sorted alphabetically and joined with `,` (Spotify search key). |
| `ArtistIDs` | `list[str]` \| `None` | Spotify artist IDs of the first matching track. `None` when not searchable, no match, or the search failed. |
| `ArtistGenre` | `list[list[str] \| None]` \| `None` | Spotify genres **per artist** (one inner list per ID in `ArtistIDs`, `None` for an ID whose lookup failed). Spotify exposes artist genres only, not track genres. |

A match is accepted when the artist matches exactly or the mean `fuzz.ratio` of track and artist is `> 60` (`MATCH_THRESHOLD`).

**These columns are NOT carried onto graph edges or nodes** (README Known Issue 14, deferred). `network_analytics.py` reads `cleaned_boiler_room_data.parquet` (pre-enrichment), and the aggregated edges carry no track details. Only the scraped `Genre` reaches the graph, and only on the per-play edge list from `build_play_edges`.

## Nodes

`nodes`: unique values of `Source` ∪ `Target` across all edges. A name only becomes a node if it participates in at least one (non-self-loop) edge.

| Attribute | Type | Description |
|---|---|---|
| `Node` | `str` | **Node ID.** Artist name from any `DJ\d+`, `Artist\d+`, or `RemixOrEdit\d+` column. |
| `LouvainCommunity` | int | Louvain community index, numbered by community size (0 = largest; ties by alphabetically first member). |
| `CommunitySize` | int | Member count of the node's community. |

There is a **single node type**: the same name acting as DJ, producer and remixer is one node. Its roles can be read from the types of its edges.

## Edges

`build_play_edges` emits, for **every track row** and every relation below, one edge per pair of non-null names in the two column groups. `aggregate_edges` then collapses identical (`Source`, `Target`, `EdgeType`) rows into one with a `Weight`.

| `EdgeType` | Source columns | Target columns | Directed | Meaning |
|---|---|---|---|---|
| `played` | `DJ\d+` | `Artist\d+` | Yes | DJ played the artist's track |
| `played` | `DJ\d+` | `RemixOrEdit\d+` | Yes | DJ played a remix/edit by the remixer |
| `remixed` | `RemixOrEdit\d+` | `Artist\d+` | Yes | Remixer reworked the artist's track |
| `collab` | `Artist\d+` | `Artist\d+` | No | Co-producers on the track |
| `b2b` | `DJ\d+` | `DJ\d+` | No | B2B partners — counted once **per track in the set** |

Co-remixers (`RemixOrEdit` – `RemixOrEdit`) are no longer linked to each other directly; they connect through the shared artist (`remixed`) and DJ (`played`). Featured artists are currently recorded as remixers (README Known Issue 16).

### Attributes

Aggregated edges (`create_edglist`, used for community detection and Graphistry):

| Attribute | Type | Description |
|---|---|---|
| `Source`, `Target` | `str` | Endpoints (node IDs). For undirected types, `Source < Target`. |
| `EdgeType` | `str` | `played` \| `remixed` \| `collab` \| `b2b` |
| `Weight` | int | Number of track rows that produced this edge |
| `CommunitySource`, `CommunityTarget` | int | Community of each endpoint (added by `assign_communities`) |
| `IsInterCommunity` | bool | `CommunitySource != CommunityTarget` |
| `LouvainCommunity` | int | Equal to `CommunitySource` (used for edge colour) |
| `CommunitySize` | int | Size of the source node's community |

Per-play edges (`build_play_edges`, before aggregation) instead carry `Source`, `Target`, `EdgeType`, `RowID` (row position in the cleaned table) and the track details `DJ`, `Artist`, `TrackName`, `Genre`, `RemixOrEdit`, `Date` (raw, unsplit strings).

### Direction, multiplicity, weights

- **Stored graph:** `to_multidigraph` builds an `nx.MultiDiGraph` with one edge per (`Source`, `Target`, `EdgeType`), keyed by `EdgeType`, with `Weight`.
- **Community detection graph:** `to_weighted_digraph` builds an `nx.DiGraph`: undirected types (`collab`, `b2b`) are added in both directions, and weights are summed across edge types for each (`Source`, `Target`). Louvain uses directed modularity on this graph.
- **Self-loops** (the same name on both sides, e.g. a DJ playing their own track or an artist remixing themselves) are dropped.

## Pipeline differences

| | `network_analytics.py` | `main.py` |
|---|---|---|
| Input | `data/processed/cleaned_boiler_room_data.parquet` | In-memory, after genre enrichment |
| Graph building | `create_edglist` + `assign_communities` | Same |
| Size filter | Keeps nodes in communities with `CommunitySize > 15`, and edges whose both endpoints are kept | None |
| Graphistry background | `'light grey'` | `'white'` |

Both call `register_graphistry()` only when run as a script, not on import.

## Known issues

1. **Genre data does not reach the graph** (README Known Issue 14, deferred).
2. **Featured artists are recorded as remixers** (README Known Issue 16).
3. **Inter-community edges are not visually distinguished.** `IsInterCommunity` is computed but the Graphistry encoding still colours edges by `LouvainCommunity` (= source community).
4. **Fuzzy clusters can over-chain.** Connected components join names that are only linked through intermediate spellings; review `data/processed/FuzzyMatching.csv` and add `never` overrides where needed.
