# Data Dictionary

Dataset: DJ set tracklists (the "Boiler Room" series). This dictionary documents the **processed data**, `cleaned_boiler_room_data.csv`, which is the file analysis and graph building should use. Counts below come from the full file, not a sample.

## Data lineage

| Role | File | Description |
|---|---|---|
| **Input (raw)** | `1001_tracklist_set_lists.csv` | Raw tracklists as collected from 1001Tracklists. 6 columns, 14,212 rows. Kept unmodified as the source of truth. |
| **Processed (this dictionary)** | `cleaned_boiler_room_data.csv` | Cleaned and parsed version of the input. 23 columns, 14,212 rows. |

The processed file has the same rows in the same order as the input (row *n* in one is row *n* in the other). There is no explicit row key, so rows can be matched by position or by (`DJ`, `Date`, `Number`). The latter has duplicates (see Known issues).

What the processing changed, compared with the input:
- **`TrackName`** went from `Artist - Title (Remix info)` to title only (the input always contains ` - `).
- **Encoding** of names was partly repaired (see Known issues).
- **`ID` placeholders** (unidentified tracks) became nulls instead of the literal text `ID`.
- **Multi-valued fields were split** into numbered columns: `Artist0..8`, `DJ0..3`, `RemixOrEdit`, `RemixOrEdit0..2`.
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
| `Artist` | string | 27 (0.2%) | 8,559 | Credited artist(s) as one string. Multiple artists are joined by `&`, `vs.`, `feat.` or `x` (e.g. `Trevor & Simon`). The individual names are in `Artist0..8`. Encoding mostly repaired. |
| `Genre` | string | 9,302 (65.5%) | 33 | Genre label for the track from a controlled list, e.g. Techno (1,247), House (840), Electronica (393), Hip Hop (235), Bassline / Garage / Grime (228). Missing for about two thirds of tracks. |
| `Number` | float | 0 | 132 | Position of the track within the set (1 = first). Stored as float but all values are whole numbers (1 to 132). |
| `DJ` | string | 0 | 668 | The DJ(s) who played the set as one string. Joint sets use `&` (e.g. `Skream & Mala`; 824 rows). Encoding repair is unreliable for non-ASCII names; prefer `DJ0..3`. |
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

`DJ1` is populated in exactly the rows where `DJ` contains ` & `. Across `DJ0..3` there are 686 distinct DJ names.

### Remix and edit columns

| Column | Non-null | Description |
|---|---|---|
| `RemixOrEdit` | 3,063 (21.6%) | Remixer or editor credit parsed from the parenthetical in the title, as the full string (e.g. `sim0ne & Melo Nada`). |
| `RemixOrEdit0` | 3,063 | First remixer or editor. Equals `RemixOrEdit` when there is only one. |
| `RemixOrEdit1` | 153 | Second remixer. |
| `RemixOrEdit2` | 4 | Third remixer. |

---

## Graph-modelling notes

These are suggestions based on the data, not facts from a schema.

- **Natural node types:** DJ (`DJ0..3`), Artist (`Artist0..8`), Track (title plus artist), Set (`DJ` + `Date`), Genre, Remixer (`RemixOrEdit0..2`).
- **Natural edges:** DJ played Track in Set; Track by Artist; Track remixed by Remixer; consecutive tracks within a set (`Number` *n* to *n+1*); Track has Genre; co-credits between artists or DJs.
- **DJs and artists overlap.** 492 names appear as both a DJ and an artist or remixer, and 1,892 rows have `Artist` equal to `DJ` (a DJ playing their own track). Decide whether these are one entity type ("Person") or two.
- Build from the split columns (`Artist0..8`, `DJ0..3`), not the combined strings.

## Known issues and questions

1. **Duplicate rows.** 8 exact duplicate rows and 11 duplicate (`DJ`, `Date`, `Number`) combinations. The cause is one set (Loods, 2018-09-15), which appears to be listed twice: 22 rows with positions only up to 11. Every other set has `Number` running 1 to *n* with no gaps. Typo variants exist inside the duplicate (`Steamworks` / `Streamworks`).
2. **Encoding repair is incomplete.** The raw input has `?` characters and mojibake (`yunÃ¨ pinku`, `VillaseÃ±or`). The processed `DJ` column replaced some of these with a different wrong result (`yun‚¨ pinku`, `Villase‚±or`; 273 rows differ from the input), while `DJ0` has the correct `Villaseñor`. `DJ0` looks better but was not checked exhaustively. `Artist` has no `?` characters left.
3. **Entity resolution is needed.** Name variants of the same entity exist (e.g. `Rollercoaster (NL)`, `Trevor & Simon` split as `Trevor` and `Simion`, whitespace variants). Check spelling variants before counting distinct artists.
4. **Unidentified tracks.** The 368 `ID` tracks have null `TrackName` and null `Artist` where the artist was also unknown. Exclude them from track and artist nodes, but they still count as positions in a set.
5. **Missing `Genre`.** 65.5% null. 679 of 767 sets have at least one genre, with a median of one third of tracks labelled per set. 360 of 3,080 artists have more than one genre, so genre is attached to the track, not the artist. It is unconfirmed whether missing means "unlabelled" or "unknown".
6. **One lost artist name.** The track at Estoc, 2021-07-15, position 17 has the raw title `NA - Cellar Theme`. The artist `NA` was read as missing, so `Artist` is null there.
7. **Uneven coverage over time.** Rows per year range from 29 (2012) to 4,064 (2023); 2013 has none; 2024 is partial (to 27 October). Time-based analysis needs normalising by number of sets.
8. **`Number` is float.** Convert to integer on load.


