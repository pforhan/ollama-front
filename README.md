# ollama-front

Reverse proxy in front of your local Ollama instance with:

- **`GET /` → `root_response`** — plain-text `root_response` (default `OK`) as a
  liveness probe, no upstream call, no gate, no
  auth/rate-limit. (Your upstream's "Ollama is running" text is suppressed.)
- **Probe mitigation** — before auth, rate-limit, gate, or upstream, paths
  are validated: blocked extensions (`.php`, `.env`, `.git`, …) and prefixes
  (`/wp-admin`, `/admin`, `/cgi-bin`, …), a strict `max_path_length`
  (default 100), and a strict prefix (`/`, `/status`, `/api/`, `/v1/` only).
  Probes get an nginx-style `444` drop (connection closed, no reply) and
  never reach Ollama. Dropped probes and stale (default 10-minute-old)
  upstream `404`s collapse into single placeholder lines on `/status`, so
  scans can't fill request history or requests-by-path.
- **Rate limiting** — `rate_limit_per_minute` (default **20**/min, anonymous + `standard`
  keys) as a **single global sliding window** (request starts, regardless of source IP or key).
  `admin`/`unlimited` are exempt from the `429`, but not from the queue. Abandoned/
  disconnected requests still consume quota — they are counted before the gate and
  upstream, with no refund on disconnect.
- **Blocked mutations** — `/api/pull`, `/api/create`, `/api/copy`, `/api/delete`,
  `/api/push` and `/api/blobs/*` are disabled for everyone except `admin` keys
  (403). They carry arbitrary model names/digests, so forcing makes no sense —
  they are blocked instead.
- **Forced model** — non-admin requests to generate/chat/embed and `/api/show` have their
  `model` field rewritten to the configured `forced_model` (default `llama3.2`), so
  `/api/show` can only ever read the forced model.
- **Serialized concurrency** — at most `max_concurrent_requests` run inside Ollama at
  once (tune in `config.json`; production value is **1**); extra requests wait in a
  bounded queue (`max_queue_size`, `queue_timeout_seconds`) and get `503` when
  full/timed out, counted as `queue rejected` on `/status`. **Every tier queues** —
  `admin`/`unlimited`/`standard`/anonymous alike (API key grants no queue exemption).
  Lightweight GETs (`/`, `/api/version`, `/api/ps`, `/api/tags`, `/v1/models`) plus
  `POST /api/show` skip the gate entirely, regardless of tier. `POST /api/show` is info-only,
  like `/api/tags` — it never runs inference.
- **Client-disconnect handling** — streaming responses poll for a dead client
  before each blocking upstream read (`select` + `recv(MSG_PEEK)`), so if nobody is
  receiving, the proxy stops generating, closes the upstream connection, and releases
  its concurrency slot immediately (`running now` on `/status` drops within ~1s).
  The request log gets a `client-disconnected` suffix.
- **`/status` live view** — besides totals and history, the status page lists the
  requests currently running inside Ollama and currently queued (method, path,
  tier, client, elapsed/waiting time, queue position), plus slot/queue occupancy
  against `max_concurrent_requests` / `max_queue_size`. Client shows the real source
  IP (or Tailscale login) when behind `tailscale serve`/`funnel` — see below.
- **`/api/tags` + `/v1/models` stubs** — at startup the proxy fetches the full tag list from Ollama
  (Ollama has no per-model filter), extracts **only the forced model's real entry**, and
  caches it. Non-admins always get that single cached entry — instant, no Ollama
  roundtrip, and never revealing other (blocked) models. `/v1/models` and
  `/v1/models/:id` (OpenAI-compatible) are stubbed the same way, and refresh every
  `tags_cache_refresh_seconds` (default 300s). Admins get the real lists.
- **API-key tiers** (see `api_keys.json.example`):
  - `admin` — no rate limit, no model forcing, can pull/create/copy/delete/push models (still queues)
  - `unlimited` — no rate limit; model still forced, downloads still blocked (still queues)
  - `standard` — full restrictions
  - no key — treated as `standard`

## Run it

```bash
python3 proxy.py
```

Or install as a service:

```bash
sudo cp ollama-front.service /etc/systemd/system/
sudo systemctl enable --now ollama-front
```

The proxy listens on `0.0.0.0:11435` and forwards to Ollama on `127.0.0.1:11434`.
Edit `config.json` to change ports, the forced model, or the rate limit.

### Alternate config

The config path can be chosen at launch without touching the file on disk:

```bash
python3 proxy.py --config /path/to/config.json
OLLAMA_FRONT_CONFIG=/path/to/config.json python3 proxy.py
```

Resolution order: `--config` flag, then `OLLAMA_FRONT_CONFIG` env var, then
`config.json` next to `proxy.py`. Useful for tests — see below.

## API keys

Copy `api_keys.json.example` to `api_keys.json` and edit. Format is `{"tier": ["key1", "key2"]}`.
Clients send the key via `Authorization: Bearer <key>` or the `X-API-Key` header.
The file is reloaded automatically when it changes — no restart needed.

## Expose with Tailscale Funnel

```bash
sudo tailscale funnel --bg 11435
```

Then use `https://<your-machine>.<tailnet>.ts.net` as your Ollama base URL.

> Behind `tailscale serve`/`funnel`, `tailscaled` connects over loopback, so the raw
> TCP peer is always `127.0.0.1`. The proxy recovers the real source IP from
> `X-Forwarded-For` (and `Tailscale-User-Login` for tailnet serve) **only** when the
> peer is `127.0.0.1`/`::1` — a directly-exposed proxy won't accept forged headers.
> `tailscale serve --tcp=443` (raw TCP) carries no header and loses the source IP.

## Testing

```bash
python3 test_proxy.py            # full run, including the streaming disconnect test
python3 test_proxy.py --skip-stream
python3 test_proxy.py --upstream-port 11434
```

`test_proxy.py` starts its own throwaway proxy on an ephemeral port via
`--config` (your real `config.json` is never modified), then verifies:
de-duplication of `ProxyHandler` methods, `GET /` → `root_response` (default `OK`), the `/api/tags` stub
(non-admin sees exactly the forced model; admin sees the full list), rate limiting
(429 after 20/min), 401/403 handling, and that killing a streaming client frees its
concurrency slot within ~5s with a `client-disconnected` log line.

Manual smoke test:

```bash
python3 proxy.py &
curl -s http://127.0.0.1:11435/                    # → root_response (default OK), text/plain
curl -s http://127.0.0.1:11435/api/tags            # → single llama3.2 entry
curl -s -X POST http://127.0.0.1:11435/api/generate \
  -d '{"model":"gemma4:e2b","prompt":"say hi","stream":true}' & sleep 1
curl -s http://127.0.0.1:11435/status | grep -E "running now|queued now"
# (kill the first curl) then re-check: running now back to 0 quickly
for i in $(seq 1 25); do curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:11435/api/tags; done
# note: /api/tags stub is answered before the rate limiter; expect 429 on proxied paths instead
```
