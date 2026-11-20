# LLM chat and graph search

For model ids, API parameters, streaming, tool use, caching and pricing, load the `claude-api` skill first; don't answer from memory.

## Architecture
- The browser never holds an API key. Chat goes to a small serverless endpoint (Lambda function URL with response streaming, or API Gateway) that calls the Claude API. Define this API contract before building UI; share types between client and server.
- Graph search through **tool use**, not by stuffing the graph into the prompt. Define a few narrow tools executed server-side against the gold data: e.g. `search_nodes(query, type?)`, `get_neighbors(id, direction, limit)`, `shortest_path(a, b)`, `top_nodes(metric, n)`. Cap result sizes. Return node ids so the UI can highlight/cite them.
- Decide where the tools run: server-side against Postgres/graph file (default, authoritative) vs. client-side against the already-loaded graph (cheaper, no DB round trip, but the model loop then runs client-side and still needs the proxy). Explain the trade-off before choosing.

## Safety and cost (treat as requirements)
- Tracklist/graph text is untrusted: it can contain prompt-injection. Keep system instructions separate, tell the model tool results are data, and never let tool output trigger privileged actions. The app only reads data, so keep tools read-only.
- Rate limit per IP/session, cap max input and output tokens and tool-loop iterations, add a monthly spend limit/alarm, and consider a Turnstile/CAPTCHA if public. Use prompt caching for the static system prompt/tool definitions.
- CORS locked to the site origin. Log requests without storing personal data. Fail closed with a friendly message when over limit.
- Render model output as text/sanitised markdown only. Never `innerHTML` it, and only linkify node ids you have verified exist in the graph.

## Chat UX
- Stream tokens; show a stop button, typing/tool-activity indicator ("Searching the graph..."), retry on error, and preserve scroll unless the user is at the bottom.
- Model the chat as an explicit state machine (idle -> sending -> streaming -> done | error | aborted); use `AbortController` for cancel and unmount.
- Keyboard: Enter sends, Shift+Enter newline, focus returns to input; `aria-live="polite"` on the message log; don't announce every token.
- Starter suggestions for the empty state ("Who plays with X most?"), and show citations as chips that focus the node in the graph.
- Persist conversation in memory only by default (privacy and simplicity).

## Testing
- Unit-test the stream parser and chat state machine with recorded fixtures. Mock the API in Playwright; never call the real model in tests. Add a small eval set of questions with expected node ids to check tool choice and answer quality over time.
