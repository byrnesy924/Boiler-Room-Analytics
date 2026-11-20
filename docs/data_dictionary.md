# Data Dictionary

Dataset: DJ set tracklists (the "Boiler Room" series). This dictionary documents the **processed data**, `cleaned_boiler_room_data.csv`, which is the file analysis and graph building should use.

> **Counts are stale.** All counts, null rates and distinct values below were measured on the file produced *before* the cleaning fixes of 2026-11-20 (README Known Issues 1–5 and 15). The cleaning now splits names differently, nulls more placeholders and clusters spellings, so column counts (`Artist0..N`, `DJ0..N`, `RemixOrEdit0..N`) and most figures will change. Re-run `src/clean_br_data.py` and refresh this file. The column *descriptions* reflect the current code.

## Data lineage

| Role | File | Description |
|---|---|---|
| **Input (raw)** | `data/raw/1001_tracklist_set_lists.csv` | Raw tracklists as collected from 1001Tracklists. 6 columns, 14,212 rows. Read-only source of truth. |
| **Intermediate** | `data/processed/preprocessed_br_data.csv` | After splitting and nulling, before fuzzy artist merging. |
| **Processed (this dictionary)** | `data/processed/cleaned_boiler_room_data.csv` and `.parquet` | Cleaned and parsed version of the input. |
| **Review output** | `data/processed/FuzzyMatching.csv` | Every artist-name pair scoring `> 65` (`Artist1`, `Artist2`, `StringSimilarity`), for checking the fuzzy merge. |
| **Genre-enriched** | `data/processed/cleaned_boiler_room_data_with_genre.parquet` | Processed file plus Spotify/Discogs columns; see [graph_schema.md](graph_schema.md#genre-enrichment-genre_download_scriptpy). |

The processed file has the same rows in the same order as the input (row *n* in one is row *n* in the other; the index is reset at the start of cleaning). There is no explicit row key, so rows can be matched by position or by (`DJ`, `Date`, `Number`). The latter has duplicates (see Known issues).

What the processing changes, compared with the input (`src/clean_br_data.py`):
- **`TrackName`** goes from `Artist - Title (Remix info)` to title only (the input always contains ` - `).
- **Encoding** of some names is repaired with a hand-written fix list (`MOJIBAKE_FIXES`, applied to `Artist` and `DJ`).
- **Unknown placeholders** — whole cells of `?`, `??`, `ID`/`Id`/`id` or blank — become nulls in `Artist`, `DJ`, `TrackName`, `RemixOrEdit` and all split columns. A `?` inside a name is kept (e.g. `?ME`).
- **Multi-valued fields are split** into numbered columns `Artist0..N`, `DJ0..N`, `RemixOrEdit0..N` on ` & `, ` and `, ` x `, ` b2b ` (case-insensitive), except before "the" (`Kool & The Gang`) and inside names listed in `src/resources/no_split_names.txt`.
- **Generic remix leftovers** (`Original`, `Extended`, `Radio`, `Club`, `VIP`, …) in `RemixOrEdit` become nulls.
- **Spelling variants are merged** in the split columns: names with case-insensitive `fuzz.ratio > 80` are clustered and replaced by the cluster's most frequent spelling; overrides in `src/resources/artist_merge_overrides.json`. `Artist`, `DJ` and `RemixOrEdit` (the unsplit strings) are not merged.
- `Genre`, `Number` and `Date` are unchanged.

## Processed file overview

| | |
|---|---|
| Rows | 14,212 (one row = one track play in one set) |
| Columns | 23 |
| Date range | 2010-08-17 to 2024-10-27 |
| Sets | 767 |
| Exact duplicate rows | 8 |

**Set identity.** There is no set ID. A set is the pair (`DJ`, `Date`). Up to 11 different DJs share a single date, so `Date` alone is not a set key.

---

## Columns: `cleaned_boiler_room_data.csv`

### Core columns

| Column | Type | Nulls | Distinct | Description |
|---|---|---|---|---|
| `TrackName` | string | 375 (2.6%) | 12,507 | Track **title only**, with the artist prefix removed. Remix or edit info stays in parentheses (e.g. `Speed It Up (Dr. Bounce Remix)`). Null for unidentified tracks (368 of the 375 nulls; the other 7 were not investigated). |
| `Artist` | string | 27 (0.2%) | 8,559 | Credited artist(s) as one string. Multiple artists are joined by `&`, `vs.`, `feat.` or `x` (e.g. `Trevor & Simon`). The individual names are in `Artist0..N`; `vs.` and `feat.` are not split on. Encoding mostly repaired. |
| `Genre` | string | 9,302 (65.5%) | 33 | Genre label for the track from a controlled list, e.g. Techno (1,247), House (840), Electronica (393), Hip Hop (235), Bassline / Garage / Grime (228). Missing for about two thirds of tracks. |
| `Number` | float | 0 | 132 | Position of the track within the set (1 = first). Stored as float but all values are whole numbers (1 to 132). |
| `DJ` | string | 0 | 668 | The DJ(s) who played the set as one string. Joint sets use `&` (e.g. `Skream & Mala`; 824 rows). Prefer `DJ0..N`, which are split and spelling-merged. |
| `Date` | string | 0 | 439 | Set date, ISO `YYYY-MM-DD`. All values parse as valid dates. Parse to datetime on load. |

### Split artist columns

| Column | Non-null | Description |
|---|---|---|
| `Artist0` | 14,185 | First or primary artist of the track. |
| `Artist1` | 1,559 | Second artist (e.g. `Trevor & Simon` becomes `Trevor`, `Simion`). |
| `Artist2` to `Artist8` | 128 / 16 / 5 / 2 / 2 / 1 / 1 | Further artists. Slots 3 to 8 are very sparse. |

Most tracks have a single artist (89% have no `Artist1`). Names here are not always identical to the matching part of `Artist`: in about 530 single-artist rows `Artist0` differs from `Artist`, and the reason was not investigated. 11 values have stray whitespace.

### Split DJ columns

| Column | Non-null | Description |
|---|---|---|
| `DJ0` | 14,212 | First DJ of the set. Always populated. 656 distinct. |
| `DJ1` | 824 | Second DJ in a joint set. 35 distinct. |
| `DJ2` | 164 | Third DJ. |
| `DJ3` | 105 | Fourth DJ. |

In the pre-fix file, `DJ1` was populated exactly where `DJ` contained ` & `, because ` and ` never split. It is now also populated for ` and `, ` x ` and ` b2b ` joint sets. Across `DJ0..3` there were 686 distinct DJ names.

### Remix and edit columns

| Column | Non-null | Description |
|---|---|---|
| `RemixOrEdit` | 3,063 (21.6%) | Remixer or editor credit parsed from the first parenthetical in the title, as the full string (e.g. `sim0ne & Melo Nada`). Null when only generic words such as `Original` or `Extended` remain. A `(feat. X)` parenthetical currently lands here (README Known Issue 16). |
| `RemixOrEdit0` | 3,063 | First remixer or editor. Equals `RemixOrEdit` when there is only one. |
| `RemixOrEdit1` | 153 | Second remixer. |
| `RemixOrEdit2` | 4 | Third remixer. |

---

## Graph-modelling notes

These are suggestions based on the data, not facts from a schema.

- **Natural node types:** DJ (`DJ0..3`), Artist (`Artist0..8`), Track (title plus artist), Set (`DJ` + `Date`), Genre, Remixer (`RemixOrEdit0..2`).
- **Natural edges:** DJ played Track in Set; Track by Artist; Track remixed by Remixer; consecutive tracks within a set (`Number` *n* to *n+1*); Track has Genre; co-credits between artists or DJs.
- **DJs and artists overlap.** 492 names appear as both a DJ and an artist or remixer, and 1,892 rows have `Artist` equal to `DJ` (a DJ playing their own track). Decide whether these are one entity type ("Person") or two.
- Build from the split columns (`Artist0..N`, `DJ0..N`), not the combined strings.
- The implemented graph is described in [graph_schema.md](graph_schema.md): one node type, typed and weighted edges (`played`, `remixed`, `collab`, `b2b`).

## Known issues and questions

1. **Duplicate rows.** 8 exact duplicate rows and 11 duplicate (`DJ`, `Date`, `Number`) combinations. The cause is one set (Loods, 2018-09-15), which appears to be listed twice: 22 rows with positions only up to 11. Every other set has `Number` running 1 to *n* with no gaps. Typo variants exist inside the duplicate (`Steamworks` / `Streamworks`).
2. **Encoding repair is incomplete.** The raw input has `?` characters and mojibake (`yunÃ¨ pinku`, `VillaseÃ±or`). In the pre-fix file, `DJ` had a different wrong result (`yun‚¨ pinku`, `Villase‚±or`; 273 rows) caused by a missing comma in the fix list, which turned every `Ã` into `‚` (README Known Issue 2, now fixed). Mojibake not on the hand-written list is still left as-is, and the old pipeline stripped every `?` from names whereas the current one keeps a `?` inside a name.
3. **Entity resolution is partly automated.** Name variants of the same entity exist (e.g. `Rollercoaster (NL)`, `Trevor & Simon` split as `Trevor` and `Simion`, whitespace variants). Fuzzy clustering merges close spellings in the split columns, but can also join distinct names; check `FuzzyMatching.csv` and add overrides before counting distinct artists.
4. **Unidentified tracks.** The 368 `ID` tracks have null `TrackName` and null `Artist` where the artist was also unknown (now also for `?` and blank placeholders). Exclude them from track and artist nodes, but they still count as positions in a set.
5. **Missing `Genre`.** 65.5% null. 679 of 767 sets have at least one genre, with a median of one third of tracks labelled per set. 360 of 3,080 artists have more than one genre, so genre is attached to the track, not the artist. It is unconfirmed whether missing means "unlabelled" or "unknown".
6. **One lost artist name.** The track at Estoc, 2021-07-15, position 17 has the raw title `NA - Cellar Theme`. The artist `NA` was read as missing, so `Artist` is null there.
7. **Uneven coverage over time.** Rows per year range from 29 (2012) to 4,064 (2023); 2013 has none; 2024 is partial (to 27 October). Time-based analysis needs normalising by number of sets.
8. **`Number` is float.** Convert to integer on load.


