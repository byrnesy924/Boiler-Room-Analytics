# Data quality

Implement checks as code in `src/` (e.g. `src/quality/checks.py`) returning pass/fail + counts, with tests in `tests/`. A failed critical check stops the pipeline; warnings are logged.

## Per layer
**Crawl / raw (S3)**
- HTTP status and non-empty body; payload parses; expected fields present (page structure may have changed - this is the most common real failure).
- Page count vs expected; duplicate fetches detected by `payload_hash`.

**Bronze**
- Row count equals files loaded; no load wrote zero rows; `scraped_at` fresh (freshness check).

**Silver**
- Uniqueness of keys; nulls in required columns; FK orphans = 0.
- Domain checks: valid dates (not future), track position >= 1 and contiguous per set, sane string lengths.
- Dedup stats: rows in vs out, with a threshold on how much may collapse.
- Entity resolution: count of unmatched/ambiguous names reported, not silently dropped.

**Gold**
- No self-loops unless intended; every edge endpoint exists in `nodes`; weights > 0.
- Row counts within tolerance of previous run (drift check); degree distribution summary.
- Edge direction documented and tested on a tiny fixture graph.

## Reporting
- Each run writes a quality report row/table: check name, layer, status, observed, threshold, run_id.
- Show the user a compact summary (`.shape`, counts), never full tables.
- When a check fails, explain what it means, likely cause, and the fix options before changing anything.

## Tests
- Fixture HTML/JSON samples (small, in `tests/fixtures/`) for parser tests; fixed `SEED=42` where randomness exists.
