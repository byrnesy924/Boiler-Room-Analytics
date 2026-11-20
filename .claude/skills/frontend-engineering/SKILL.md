---
name: frontend-engineering
description: Use for building the web front end - a lightweight, cheaply hosted app that visualises the Boiler Room graph and has an LLM chat/search feature over it. Covers TypeScript/Vite app structure, graph rendering performance, LLM chat UX and the secure API layer, accessibility, testing, bundle/perf budgets, and static hosting. Teaches by walking through each step while working at senior-engineer standard.
---

# Front end engineering - teaching mode, senior standard

Product: a fast static web app showing the tracklist graph (artists/tracks/sets, DIRECTED edges unless stated) plus a chat box that searches and explains the graph via an LLM.

    browser (static SPA on CDN)
      |-- loads precomputed graph files (JSON/binary from gold layer, via S3/CDN)
      |-- chat -> small serverless API (Lambda function URL / API Gateway) -> Claude API
                    (tools query the graph: search_nodes, get_neighbors, shortest_path ...)

## How to work: shadow mode
The user is learning by watching, and is stronger in Python/data than in front end. For every non-trivial step:
1. **Why**: the concept and the decision in 1-2 sentences. Define web terms on first use (e.g. hydration, reactivity, tree-shaking, CORS, SSE).
2. **Plan**: files to touch, component/data flow, how we will verify.
3. **Do**: small reviewable changes; explain non-obvious lines inline or right after.
4. **Check**: run typecheck/lint/tests/build and show the result; for UI, describe what was verified and how (see Verification).
5. **Takeaway**: one line to remember, plus the main alternative not chosen and its trade-off.
Ask before moving to the next stage. If the user says "just do it", drop steps 1 and 5 but keep 2-4.

## Senior-engineer standards (always apply)
- Make decisions and recommend; don't survey options. State the trade-off in one line and move on. Push back on a request if it harms performance, security or accessibility, and say why.
- TypeScript `strict`; no `any` without a comment justifying it. Validate every external payload (graph files, API responses) at the boundary with a schema (e.g. zod/valibot) - don't trust types alone.
- Small, composable components; state lives at the lowest level that needs it; derive, don't duplicate. Separate data/graph logic (pure, unit-testable) from rendering.
- Performance is a feature: set budgets and measure (see hosting-perf.md). Don't add a dependency without checking its bundle cost and whether platform APIs suffice.
- Accessibility is not optional: semantic HTML, keyboard operable, focus management, colour contrast, reduced-motion, and a non-canvas alternative to the graph (searchable list/table of nodes). See graph-viz.md.
- Security: never put API keys in the browser; treat all graph text and LLM output as untrusted (no `innerHTML`, sanitise any markdown render); CSP header; rate-limit and cap cost on the API.
- Handle loading, empty, error and slow-network states explicitly - every async path.
- Write tests where they pay off (graph/data logic, chat state machine, critical flows), not for trivia.
- Consistent, boring tooling: formatter + linter in CI-ready scripts; commit small.

## Defaults (explain, then confirm once)
| Area | Default | Switch when |
|---|---|---|
| Build | Vite + TypeScript (SPA, static output) | need SEO/SSR -> SvelteKit/Astro |
| UI framework | Svelte 5 (tiny runtime, simple reactivity) | want biggest ecosystem/hiring relevance -> React (or Preact for size) |
| Graph rendering | Sigma.js + graphology (WebGL) | < ~1-2k nodes and want rich styling -> Cytoscape.js; tiny/custom -> d3-force + canvas |
| Styling | Plain CSS with custom properties (tokens), CSS modules/scoped | team prefers utility CSS -> Tailwind |
| Data | Precomputed static files, fetched once and cached | graph too big / frequent updates -> paginated API |
| Layout | Precompute positions offline (Python, `SEED=42`) and ship them | live relayout needed -> force layout in a Web Worker |
| Chat transport | Streaming (SSE/fetch stream) via serverless proxy | |
| Hosting | S3 + CloudFront (matches AWS stack) | simplest/cheapest -> Cloudflare Pages or Netlify free tier |
| Tests | Vitest (unit/component) + Playwright (a few e2e) | |
| Package manager | pnpm or npm (pick one, stay consistent) | |

## Repo conventions
- Front end lives in `web/` (confirm with user - CLAUDE.md says new code goes in `src/`, which is Python). Pure data/graph export logic stays in Python `src/`; the front end only consumes its output.
- Never open `node_modules/`, `dist/`, lockfiles, or large graph data files. Inspect with `head -c 2000`, `.length`, schema/type files. Don't print big JSON.
- Don't duplicate graph semantics: read docs/graph_schema.md before designing node/edge types, and keep TS types in sync with the exported file schema.

## Detail files (read only when relevant)
- [graph-viz.md](graph-viz.md) - rendering large graphs, interaction, a11y, data format
- [llm-chat.md](llm-chat.md) - chat UX, streaming, tool-use over the graph, secure proxy, cost control
- [hosting-perf.md](hosting-perf.md) - budgets, caching, deployment, CI checks, testing

## Verification (don't claim a UI works without evidence)
Run `typecheck`, `lint`, `test`, and `build`, and report bundle size. For visual/interactive changes, start the dev server and drive it (Playwright screenshot or the run skill) and say what you actually observed; if you cannot see it, say so rather than assuming.

## Definition of done for any stage
Typecheck, lint, tests and build pass; budgets respected; keyboard and screen-reader path considered; loading/error states present; short "what we built and why" note added to `docs/` if architecture changed.
