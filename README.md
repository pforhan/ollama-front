# ollama-front

Reverse proxy in front of your local Ollama instance with:

- **Rate limiting** — 20 requests/minute (anonymous + `standard` keys)
- **Blocked downloads** — `/api/pull` is disabled for everyone except `admin` keys
- **Forced model** — non-admin requests to generate/chat/embed endpoints have their
  `model` field rewritten to `llama3.2`
- **Serialized concurrency** — at most `max_concurrent_requests` (default **1**) run
  inside Ollama at once; extra requests wait in a bounded queue (`max_queue_size`,
  `queue_timeout_seconds`) and get `503` when full/timed out. `/status` bypasses the gate.
  Lightweight GETs (`/`, `/api/version`, `/api/ps`, `/api/tags`) skip the gate entirely.
- **`/api/tags` stub** — at startup the proxy fetches the full tag list from Ollama
  (Ollama has no per-model filter), extracts **only the forced model's real entry**, and
  caches it. Non-admins always get that single cached entry — instant, no Ollama
  roundtrip, and never revealing other (blocked) models. Refreshes every
  `tags_cache_refresh_seconds` (default 300s). Admins get the real tag list.
- **API-key tiers** (see `api_keys.json`):
  - `admin` — no restrictions (no rate limit, no model forcing, can pull models)
  - `unlimited` — no rate limit; model still forced, downloads still blocked
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

## API keys

Edit `api_keys.json`. Format is `{"tier": ["key1", "key2"]}`.
Clients send the key via `Authorization: Bearer <key>` or the `X-API-Key` header.
The file is reloaded automatically when it changes — no restart needed.

## Expose with Tailscale Funnel

```bash
sudo tailscale funnel --bg 11435
```

Then use `https://<your-machine>.<tailnet>.ts.net` as your Ollama base URL.
