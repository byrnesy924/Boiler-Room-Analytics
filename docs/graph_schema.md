# Graph Schema

> **Status: describes the graph AS CURRENTLY IMPLEMENTED (as of 2026-11-20), not the intended design.**
> Derived solely from `clean_br_data.py`, `network_analytics.py`, `main.py` and `genre_download_script.py`.
> Known defects are listed in [Known issues](#known-issues) — they affect node identity and edge semantics.
>
> **If you change how nodes, edges, or their attributes are built, update this file in the same change.**

## Overview

| Property | Value |
|---|---|
| Node types | 1 (artist name — no role distinction) |
| Edge types | 1 stored (semantic type is implicit, not recorded) |
| Direction | Undirected |
| Multiplicity | Multigraph in the edge DataFrame; collapsed to simple graph for community detection |
| Weights | None |
| Self-loops | Possible |
| Community detection | Louvain (`nx.community.louvain_communities`), no seed |
| Visualisation | Graphistry (`show_arrows=False`) |

Builders:
- `network_analytics.create_edglist(df, artist_cols)` → `(df_of_edges, df_of_nodes)`
- Community assignment: inline in `network_analytics.py` `__main__` and duplicated in `main.py`

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
| `RemixOrEdit` | `TrackName` | Text inside first `(...)`, with Remix/Edit/bootleg/Version/Mix/Dub etc. removed |
| `Artist0..N` | `Artist` | Split on ` & ` (collaborators) |
| `DJ0..N` | `DJ` | Split on ` & `, ` B2B `, ` b2b ` (B2B partners) — see issue 3 |
| `RemixOrEdit0..N` | `RemixOrEdit` | Split on ` & `, ` and `, ` And ` |

Name normalisation applied to `Artist\d+`, `DJ\d+`, `RemixOrEdit\d+`:
1. A hand-written mojibake fix list (applied to `Artist`/`DJ` before splitting).
2. `?` (except in `?ME`) and exact `ID`/`Id` replaced with `""`.
3. Fuzzy de-duplication: all pairs of names with `rapidfuzz.fuzz.ratio > 80` are merged, mapping the alphabetically later name onto the earlier one. Single pass, non-transitive.

Output: `Data/cleaned_boiler_room_data.{csv,parquet}`.

### Genre enrichment (`genre_download_script.py`) — state uncertain

Run in `main.py` (Discogs then Spotify) and in the script's own `__main__` (writes `Data/cleaned_boiler_room_data_with_genre.parquet`). Adds row-level columns:

| Column | Type | Contents |
|---|---|---|
| `DiscogsTrackID` | `list[str]` \| `None` \| `""` | **Misnamed** — holds the Discogs release *genres* (`Release.genres`) of the first matching release, not an ID. `""` when `Artist` or `TrackName` is blank; `None` when no match. |
| `ArtistForSearch` | `str` | `Artist\d` values sorted alphabetically and joined with `,` (Spotify search key). |
| `ArtistIDs` | `list[str]` \| `None` \| `""` | Spotify artist IDs of the first matching track. |
| `ArtistGenre` | `list[list[str]]` \| `None` | Spotify genres **per artist** (one inner list per ID in `ArtistIDs`). Spotify exposes artist genres only, not track genres. |

**These columns are NOT carried onto graph edges or nodes.** `create_edglist` selects a fixed column list (see Edges), and `network_analytics.py` reads `cleaned_boiler_room_data.parquet` (pre-enrichment). Only the scraped `Genre` reaches the graph.

## Nodes

`df_of_nodes`: unique values of `Node1` ∪ `Node2` across all edges. A name only becomes a node if it participates in at least one edge.

| Attribute | Type | Description |
|---|---|---|
| `Node` | `str` | **Node ID.** Artist name from any `DJ\d+`, `Artist\d+`, or `RemixOrEdit\d+` column. |
| `LouvainCommunity` | float (int-valued) | Louvain community index. `main.py` names it `Louvain_Community`. |
| `CommunitySize` | float | Member count of the node's community. `main.py` names it `Community_size`. |

There is a **single node type**: the same name acting as DJ, producer and remixer is one node, and the role is not recorded.

## Edges

`df_of_edges`: for **every row** and **every unordered pair of artist columns** `(col_i, col_j)` with `i < j` in `artist_cols` order where **both values are non-null**, emit one edge `Node1 = row[col_i]`, `Node2 = row[col_j]`.

`artist_cols = df.filter(regex=r"(DJ\d+)|(RemixOrEdit\d+)|(Artist\d+)").columns` — column order (Artist*, DJ*, RemixOrEdit*) determines which endpoint is `Node1`; this ordering carries **no direction semantics**.

### Implicit edge meanings (not stored)

| Column pair | Meaning |
|---|---|
| Artist – Artist | Co-producers / collaborators on the track |
| Artist – DJ | DJ played the artist's track |
| Artist – RemixOrEdit | Remixer reworked the artist's track |
| DJ – DJ | B2B partners — emitted once **per track in the set** |
| DJ – RemixOrEdit | DJ played a remix/edit by the remixer |
| RemixOrEdit – RemixOrEdit | Co-remixers |

### Attributes

| Attribute | Type | Description |
|---|---|---|
| `Node1`, `Node2` | `str` | Endpoints (node IDs) |
| `index` | int | Source row index (from `reset_index()`) |
| `DJ` | `str` | Raw, unsplit DJ string |
| `Artist` | `str` | Raw, unsplit artist string |
| `TrackName` | `str` | Cleaned track name |
| `Genre` | `str` | Scraped 1001tracklists genre (often missing) |
| `RemixOrEdit` | `str` | Unsplit remix/edit credit |
| `Date` | — | Set date |
| `LouvainCommunity` | float | Community of an endpoint (see issue 4). `main.py`: `Louvain_Community` |
| `CommunitySize` | float | Size of that community. **Not set in `main.py`.** |

### Direction, multiplicity, weights

- **Edge DataFrame:** undirected multigraph — one edge per (track row × column pair); repeated plays produce parallel edges.
- **NetworkX graph** (`nx.from_pandas_edgelist`, default `nx.Graph`): undirected **simple** graph. Parallel edges collapse, no weight is computed, edge attributes are dropped. Playing an artist 50 times = once.
- **Self-loops** occur when the same name is in two columns of a row (e.g. a DJ playing their own track).
- This contradicts the CLAUDE.md convention ("edges are DIRECTED unless stated") — **this graph is explicitly undirected.**

## Pipeline differences

| | `network_analytics.py` | `main.py` |
|---|---|---|
| Input | `Data/cleaned_boiler_room_data.parquet` | In-memory, after genre enrichment |
| Community column names | `LouvainCommunity`, `CommunitySize` | `Louvain_Community`, `Community_size` |
| Edge `CommunitySize` | Set | Not set |
| Size filter | Keeps communities with `CommunitySize > 15` (nodes and edges) | None |
| Graphistry background | `'light grey'` | `'white'` |

## Known issues

1. **Empty-string hub node.** `?`/`ID` are replaced with `""` rather than NaN, so they pass `dropna` and every unknown artist becomes one node `""`.
2. **Missing comma in mojibake fix list** (`clean_br_data.py`, `("Ã‚me" "Ame")`). This is a single string, so the loop replaces every `"Ã"` with `"‚"`, corrupting names and breaking subsequent fixes.
3. **B2B split regex** contains an escaped pipe (`\sand\s\|\sAnd\s`), so `" and "` / `" And "` never split; such DJ pairs stay as one node.
4. **Edge community is arbitrary for inter-community edges** — it is overwritten by whichever endpoint's community is iterated last.
5. **Louvain is unseeded** → communities are not reproducible (violates `SEED=42` convention).
6. **Fuzzy merging is single-pass and non-transitive**; distinct artists with ratio > 80 are merged.
7. **Genre script match thresholds:** `fuzz.ratio` returns 0–100 but the threshold is `> 0.6`, so almost any search result is accepted as a match (Spotify and Discogs).
8. **Genre script robustness:** `spotify_get_artist_genres_from_ID` iterates `ArtistIDs` when it is `None` (TypeError); a non-400 `SpotifyException` leaves `test_search` unbound.
9. `create_edglist` is annotated `-> pd.DataFrame` but returns a tuple `(df_of_edges, df_of_nodes)`.
