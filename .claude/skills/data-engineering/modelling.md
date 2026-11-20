# Database modelling

Check docs/graph_schema.md and docs/data_dictionary.md first; extend them, don't contradict them.

## Layers (Postgres schemas)
- **bronze**: one table per source payload type, columns as-scraped (text), plus `source_url`, `scraped_at`, `s3_key`, `payload_hash`. Append-only. No cleaning.
- **silver**: typed, deduplicated, normalised entities: `sets`, `tracks`, `artists`, `set_tracks` (set_id, position, track_id). Natural keys + surrogate keys, FKs enforced, `NOT NULL` where truthful.
- **gold**: analysis-ready: `nodes`, `edges` (src_id, dst_id, weight, edge_type), plus any aggregates. Rebuildable entirely from silver.

## Principles to teach as they come up
- Primary keys, natural vs surrogate keys, foreign keys, and why constraints are a data-quality tool.
- Idempotent loads: unique constraint + `INSERT ... ON CONFLICT DO UPDATE`.
- Normalisation (3NF for silver) vs deliberate denormalisation (gold).
- Indexes: add for join/filter columns actually used; check with `EXPLAIN`.
- Entity resolution: track/artist name variants -> canonical id; keep a mapping table, never overwrite bronze.
- Directed edges: store direction explicitly; document what src->dst means (e.g. played-before).

## Migrations
- SQL migration files in version control (e.g. `src/db/migrations/001_init.sql`), applied in order, never edited after being applied. Show the SQL and explain before running.
- Transforms bronze->silver->gold as plain SQL or small Python functions, each re-runnable.
