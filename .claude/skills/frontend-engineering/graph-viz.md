# Graph visualisation

## Data contract (agree this first, with the Python side)
- Export from gold as versioned static files: `nodes.json`/`edges.json` (or one compact file; consider columnar arrays or binary later if size demands). Include `schemaVersion`.
- Nodes: `id`, `label`, `type`, precomputed `x`,`y`, optional `size`/`community`/centrality. Edges: `source`,`target`,`weight`,`directed`.
- Precompute layout, community and centrality offline in Python (`SEED=42`), so the browser does no heavy computation and renders identically every time.
- Gzip/brotli at the CDN; check transfer size, not raw size. Validate the file against a schema on load.

## Rendering
- Sigma.js (WebGL) + graphology for thousands to 100k+ nodes. Canvas/SVG force graphs fall over past a few thousand nodes.
- Keep graph state outside the UI framework's reactivity (a plain graphology instance); push only small derived state (selection, filters, camera) into components. Don't make 50k nodes reactive.
- Level of detail: hide labels until zoomed in or node is large/selected; thin or hide low-weight edges by default; draw arrowheads only when zoomed in (edges are directed).
- Any live force layout runs in a Web Worker and stops itself; prefer none.

## Interaction
- Search-to-focus, click to select, neighbour highlight (dim the rest), filter by node type/community/weight threshold, URL-synced selection (`?node=id`) so views are shareable.
- Details panel for the selected node (plain DOM, not canvas) - this is also where chat can link to nodes.
- Chat answers reference node ids; clicking a cited node centres the camera on it (the bridge between the two features).

## Accessibility
- The canvas is not accessible by itself. Provide a keyboard-operable searchable list/table of nodes and a neighbours list for the selection; announce selection changes via an `aria-live` region.
- Don't encode meaning in colour alone (also shape/label); check contrast; respect `prefers-reduced-motion` (no animated camera tweens); visible focus rings.
- Colour palettes: colour-blind safe categorical colours; use the dataviz skill when choosing palettes.

## Performance checks
- Measure time-to-first-render of the graph and frame rate while panning on a mid-range laptop and a throttled-CPU profile. Add a dev-only stats overlay.
- Lazy-load the graph library and data after first paint (shell loads instantly, with a skeleton).
