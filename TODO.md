# TODO — disconnect / broken-pipe handling + cleanup

Context: `proxy.py` in this directory. Last edits left duplicate method
definitions (Python keeps the *last* one), so current behavior is
unpredictable despite compiling. Do not ship until §1 is done.

## 1. Critical: de-duplicate `ProxyHandler` (P0)

`proxy.py` currently defines these more than once (verified via
`grep -n "def "`):
- `_enter_gate` — 2–3 copies, later one wins, one copy contains stub-tags code
  pasted into the gate
- `_serve_root` — 2–3 copies, shadowed copies contain stub-tags JSON
- `_handle` — a second definition starts mid-way; its first lines look like
  the tail of `_enter_gate` (`with _gate_lock: … _gate_waiters …`)

Fix: there must be exactly one of each:
`__init__` attrs, `_log`, `_is_client_alive`, `_serve_stub_tags`,
`_serve_root`, `_enter_gate`, `_handle`, `_respond`, `_serve_status`,
`log_message`. Easiest: rewrite `proxy.py` cleanly from scratch rather than
patching duplicates. Verify with:
```bash
grep -n "def \(_enter_gate\|_serve_root\|_handle\)" proxy.py   # each = 1 hit
python3 -m py_compile proxy.py
```

Also check: `import socket` was added at top (needed for `MSG_PEEK`); confirm
it stayed after rewrite. `import select` is imported inside `_is_client_alive`
— hoist to top-level.

## 2. Client disconnect: abort upstream & release slot (P0)

Requirement: if no one is receiving, don't block anything further, but
rate limiting still applies.

Current state:
- Streaming loop catches `BrokenPipeError/ConnectionResetError/OSError` on
  `wfile.write`, sets `_client_gone`, closes upstream. **Not wired to
  `_is_client_alive()`** — `_is_client_alive()` exists (non-blocking
  `select` + `recv(MSG_PEEK)==b""`) but is never called.
- Non-streaming path does `resp.read()` in one shot — a disconnect there is
  only discovered *after* the full generation finishes.

Fix in `_handle`, streaming branch (chunked / no `Content-Length`):
- Before each `resp.read(8192)`, call `if not self._is_client_alive(): break`
  and skip writing the terminating `0\r\n\r\n` chunk.
- On break: `conn.close()` (in existing `finally`), `_client_gone=True`, log
  `client-disconnected`.
- The outer `finally: if self._holds_slot: release_slot()` must run
  immediately — test that `/status` `running now` drops to 0 within ~1s of a
  `timeout`/`curl` kill, not after generation completes.

Non-streaming branch: acceptable to leave as-is (finite response), but add a
post-`resp.read()` `wfile.write` try/except that sets `_client_gone`. Full
early-abort would require reading upstream in chunks even for
`Content-Length` responses — optional.

Queued case: if a client disconnects while *waiting* in `_enter_gate`, the
handler still blocks up to `queue_timeout_seconds` (300s default). Simple
improvement (optional): poll `select`-based timeout in 0.5s slices in the
semaphore wait and bail with 503 if the client died. Not required for P0.

Rate limiting: `rate_limited(identity)` is called *before* the gate and
upstream, so an abandoned request already consumes quota. Keep that order —
do not refund on disconnect. Verify: fire N abandoned requests, confirm
429 triggers at `RATE_LIMIT/min`.

## 3. `GET /` → plain `OK` (P1)

Requirement: suppress the upstream "Ollama is running" text; return `OK`.

Current state: `_serve_root()` returns `b"OK"` as `text/plain; charset=utf-8`
with `Connection: close`, no upstream call, no gate. Verified once
(`curl http://127.0.0.1:11435/` → `OK`). The method is defined multiple
times (§1) so confirm the single surviving definition matches this.

- In `_handle`, short-circuit before auth/rate-limit/gate:
  `if path == "/" and method == "GET": _serve_root(); _record("/", …); return`
- Do **not** put `/` through `_enter_gate` or upstream.
- Keep it in `lightweight_paths` as a safety net if stubbing is disabled.

## 4. Already-verified behaviors (do not regress)

- `/api/tags` stub: at startup fetch full list, extract only the forced
  model's entry, cache it, refresh every `tags_cache_refresh_seconds` (300).
  Non-admins get exactly that one real entry; admins get pass-through.
  Verified: `llama3.2:latest`, size `2019393189`, real digest/details.
- Lightweight GETs (`/`, `/api/tags`, `/api/version`, `/api/ps`) skip the
  gate via `needs_gate()`. Inference paths serialize.
- Concurrency: semaphore `MAX_CONCURRENT` held for whole upstream exchange,
  released in `finally` only when `_holds_slot` is true. Queue full /
  timeout → 503 with `queue rejected` counter on `/status`.
- Logging: one line/request to stdout (journal under systemd), no
  headers/bodies, `client-disconnected` suffix when set. Journal caps
  (`SystemMaxUse=100M` etc.) in the unit file.
- Rate limit 20/min, `/api/pull` 403 for non-admins, model forcing — all
  tested and working.

## 5. Config sanity

`config.json` currently shows `"max_concurrent_requests": 10`. The machine
is a Core i5 / 8 GB box — intended value was **1**. Confirm whether 10 was
deliberate or a test leftover; reset to 1 before production.

## 6. Test plan after rewrite

```bash
python3 proxy.py &
curl -s http://127.0.0.1:11435/                    # → OK, text/plain
curl -s http://127.0.0.1:11435/api/tags            # → single llama3.2 entry
# disconnect: start a long stream, kill client, check slot frees
curl -s -X POST http://127.0.0.1:11435/api/generate \
  -d '{"model":"gemma4:e2b","prompt":"say hi","stream":true}' & sleep 1; \
curl -s http://127.0.0.1:11435/status | grep -E "running now|queued now"
# (kill the first curl) then re-check: running now back to 0 quickly
# rate-limit counts abandoned requests
for i in $(seq 1 25); do curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:11435/api/tags; done
# expect 429 after 20
```
