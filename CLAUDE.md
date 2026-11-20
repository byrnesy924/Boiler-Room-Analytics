# Boiler Room Analytics – graph analytics on Boiler Room DJ set tracklists

## Commands (always via uv)
- Env: `uv sync`
- Test: `uv run pytest tests/test_<file>.py` (single file; full suite only when asked)
- Lint: `uv run ruff check --fix <files> && uv run ruff format <files>` (changed files only)

## Layout
- Legacy pipeline: scripts in `src/` (data_crawlers/scrape → clean_br_data → genre_download_script → network_analytics), run by `src/main.py`. Don't refactor unless asked.
- Paths: `src/utils/data_loading.py` (`PATHS`). Hand-maintained inputs: `src/resources/`.
- New code goes in `src/`, tests in `tests/`. Notebooks only call `src/`.
- Schema: docs/graph_schema.md, docs/data_dictionary.md (read only when relevant).

## Data & graph conventions
- `data/raw/` files are read-only; derived output → `data/processed/`.
- `SEED=42` (`src/config.py`) for anything random.
- Edges are DIRECTED unless stated; check before centrality/community code.

## Token budget (important)
- Never open: `*.html`, `1001_tracklist_htmls/`, `*.log`, `data/`, `Images/`, `uv.lock`, .ipynb outputs.
  To inspect: `head -c 2000`, `grep -m 5`, `tail -n 20`.
- Never print full DataFrames/edge lists; use `.head()`, `.shape`, `.describe()`.
- Read only the lines you need (offset/limit); don't re-read files you just edited.
- No subagents unless I ask. Ask before multi-file refactors.
- Keep replies brief; no recap of diffs I can see.

## Compaction
Preserve: modified files, current goal, test command.
