# Hosting, performance, quality gates

## Budgets (propose, then enforce in the build)
- Initial JS (gzip) <= ~100 KB excluding the lazy-loaded graph chunk; graph library + data load after first paint.
- LCP < 2.5 s on throttled mobile, CLS ~0, INP < 200 ms. Check with Lighthouse and the browser performance panel.
- Fail the build (or CI) if budgets regress. Inspect bundles with a visualiser (e.g. rollup-plugin-visualizer) and explain what is large.

## Techniques
- Code-split by route/feature (`import()`), preload critical assets, `font-display: swap` or use system fonts, self-host fonts, avoid layout shift with reserved sizes.
- Images as SVG/AVIF/WebP, sized. No large UI kits; prefer platform features (CSS grid, `<dialog>`, `fetch`, `AbortController`).
- Cache: hashed asset filenames with `Cache-Control: immutable, max-age=31536000`; `index.html` short/no-cache; graph data files versioned by hash or `schemaVersion` in the URL. Brotli at the CDN.

## Hosting (cheap)
- Default S3 (private) + CloudFront with Origin Access Control, HTTPS via ACM, SPA fallback to `index.html`, security headers (CSP, `X-Content-Type-Options`, `Referrer-Policy`) via response headers policy. Cost is typically pennies/month.
- Simpler alternative: Cloudflare Pages/Netlify free tier. Recommend based on whether the user wants everything in AWS IaC (teachable, one stack) or the least setup.
- Same IaC rules as the data-engineering skill: infrastructure as code only, show plan, ask before apply, budget alarm, tags, teardown path. Deploy = build, sync to S3, invalidate CloudFront; script it, don't do it by hand.

## Quality gates (scripts in package.json)
- `typecheck`, `lint` (ESLint + typescript-eslint), `format` (Prettier or Biome), `test` (Vitest), `e2e` (Playwright, a handful of critical flows incl. keyboard-only), `build`, `preview`.
- Accessibility check in e2e (axe) on main views. Pin Node via `.nvmrc`/`engines`. Commit the lockfile but never open it.

## Windows notes
- Use PowerShell-compatible scripts (avoid bash-only npm scripts; use `cross-env` if setting env vars). Watch path separators and line endings (`.gitattributes`).
