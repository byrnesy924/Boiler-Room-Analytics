---
name: data-engineering
description: Use for cloud data engineering on this project - AWS infrastructure (Lambda/Glue crawlers, S3, RDS/Aurora Postgres, optional graph store), bronze/silver/gold modelling, schema and database design, and data quality checks. Teaches by walking through each step. Not for graph analytics/centrality/community analysis (that is src/network_analytics).
---

# Data engineering (AWS) - teaching mode

Target architecture (confirm before building, don't assume it is final):

    websites -> Lambda/Glue crawler -> S3 (raw html/json, immutable)
             -> Postgres bronze (raw tables) -> silver (clean, typed, deduped)
             -> gold (edges/nodes tables) -> graph store / networkx on top

## How to work: shadow mode
The user is learning by watching. For every non-trivial step:
1. **Why**: one or two sentences on the concept and why this choice (not a lecture).
2. **Plan**: inputs -> action -> outputs -> how we verify.
3. **Do**: make the change in small, reviewable pieces.
4. **Check**: run the verification and show the result (counts, `.head()`, `plan` output).
5. **Takeaway**: one line on what to remember, plus any alternative not chosen and its trade-off.
Define each AWS/DB term the first time it appears. Ask before moving to the next stage. If the user says "just do it", drop steps 1 and 5 but keep 2-4.

## Core rules
- Raw is immutable: land crawled payloads in S3 untouched (keyed by source/date/id), then derive everything else from them. Re-runs must be idempotent (upserts / `ON CONFLICT`, deterministic keys).
- Infrastructure as code only (ask once: Terraform vs CDK, then stay consistent). No console-only changes.
- Never apply, deploy, create billable resources, or delete anything without showing the plan/diff and getting explicit approval. State the rough cost driver of each resource.
- No credentials in code or repo: IAM roles for Lambda/Glue, Secrets Manager/SSM for DB creds, least-privilege policies (name the exact actions/resources).
- Crawling: check robots.txt/ToS, rate limit, set a clear User-Agent, retry with backoff, make each page fetch independently retryable (SQS or Step Functions Map, not one giant loop). Existing crawl logic lives in `src/` (legacy); reuse, don't refactor unless asked.
- New code in `src/`, tests in `tests/`, run via `uv`. Local dev first (SQLite/Postgres in Docker, `moto`/LocalStack for AWS) before touching real AWS.
- Keep it simple: single small Postgres (RDS or Aurora Serverless v2) is the default. Don't introduce Neptune, Redshift, EMR, or Kafka without stating why plain Postgres is insufficient.

## Decision defaults (explain, then confirm)
| Question | Default | Switch when |
|---|---|---|
| Crawler compute | Lambda (<15 min/invocation) | long/heavy jobs -> Glue Python shell |
| Orchestration | EventBridge schedule + Step Functions | single job -> EventBridge only |
| Raw store | S3 + Postgres bronze tables | |
| Warehouse layers | Postgres schemas `bronze`, `silver`, `gold` | volume outgrows one instance |
| Graph | gold `edges`/`nodes` tables + networkx | multi-hop traversal at scale -> Neptune |
| Edges | DIRECTED unless stated | |

## Detail files (read only when relevant)
- [aws.md](aws.md) - service wiring, IAM, networking gotchas, cost guardrails
- [modelling.md](modelling.md) - schemas, keys, indexes, bronze/silver/gold, migrations
- [data-quality.md](data-quality.md) - checks per layer, how to implement and report them

## Definition of done for any stage
Tests pass (`uv run pytest tests/test_<file>.py`), data-quality checks run and results shown, IaC plan reviewed, and a short "what we built and why" note added to `docs/` if the architecture changed.
