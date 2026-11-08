# ollama-front

A reverse proxy in front of a local [Ollama](https://ollama.com/) instance. It
adds access control, request shaping, and operational visibility so you can
safely expose a single model from Ollama without giving callers unrestricted
control over the machine.

By default the proxy listens on `0.0.0.0:11435` (all interfaces) and forwards
to Ollama at `127.0.0.1:11434`.

## Ollama features

### Forced model

For non-admin requests, the proxy rewrites the `model` field on
generate/chat/embed requests and `POST /api/show` to the configured
`forced_model` (default `llama3.2`). This prevents callers from selecting other
local models.

When `reported_context_length` is set (for example, `32768`), the proxy:

- caps reported context in `/api/show` (`model_info.*.context_length`), and
- defensively caps `options.num_ctx` on forwarded requests.

Set `reported_context_length` to `0` or leave it unset to report upstream
context as-is.

### Blocked model-management mutations

The following endpoints are disabled for everyone except `admin` API keys
(returns `403`):

- `/api/pull`
- `/api/create`
- `/api/copy`
- `/api/delete`
- `/api/push`
- `/api/blobs/*`

These operations carry arbitrary model names/digests and are blocked rather than
model-forced.

### Model list stubs

For non-admin callers, `/api/tags`, `/v1/models`, and `/v1/models/:id` return a
cached entry for **only** the forced model. At startup the proxy fetches the
full tag list from Ollama, extracts the forced model's entry, and refreshes the
cache every `tags_cache_refresh_seconds` (default `300`). This avoids revealing
other local models and avoids an upstream roundtrip for common discovery calls.

Admins receive the real upstream model lists.

### Concurrency and queueing

Ollama requests that can consume inference resources are serialized through a
bounded gate:

- `max_concurrent_requests` — requests allowed to run inside Ollama at once
- `max_queue_size` — maximum waiting requests
- `queue_timeout_seconds` — how long a request may wait in the queue

When the queue is full or a request times out waiting, the proxy returns `503`.
**All tiers queue**, including `admin` and `unlimited`; API keys do not bypass
the queue.

Lightweight requests skip the gate entirely, regardless of tier: `GET /`,
`/api/version`, `/api/ps`, `/api/tags`, `/v1/models`, and `POST /api/show`.

### Rate limiting

`rate_limit_per_minute` (default `20`) applies as a single global sliding
window across request starts, regardless of source IP or API key.
`admin` and `unlimited` keys are exempt from `429` responses, but they are not
exempt from queueing. Abandoned/disconnected requests still consume quota.

### API key tiers

API keys are defined in `api_keys.json` as `{"tier": ["key1", "key2"]}`.
Clients send a key with `Authorization: Bearer <key>` or the `X-API-Key`
header. Requests without a key are treated as `standard`.

| Tier | Rate limit exempt | Model forced | Model mutations allowed | Queues |
| --- | ---: | ---: | ---: | ---: |
| `admin` | Yes | No | Yes | Yes |
| `unlimited` | Yes | Yes | No | Yes |
| `standard` | No | Yes | No | Yes |
| no key | No | Yes | No | Yes |

## Status page

The status page (default path: `/proxy-status`) provides a live operational view,
including:

- request totals and recent request history
- requests currently running inside Ollama
- requests currently queued
- slot/queue occupancy against `max_concurrent_requests` / `max_queue_size`

For active and queued requests it shows method, path, tier, client,
elapsed/waiting time, and queue position.

When running behind `tailscale serve`/`funnel`, the proxy reports the real
source IP (or Tailscale login) instead of loopback. It trusts forwarded
identity headers only when the direct peer is `127.0.0.1`/`::1`; a directly
exposed proxy does not accept forged headers.

## Probe mitigation

### Custom root message

`GET /` returns a plain-text response (`root_response`, default `OK`) without
calling upstream, without auth/rate-limit checks, and without using a
concurrency slot. The upstream `Ollama is running` text is not forwarded.

### Path validation

Before auth, rate limiting, queueing, or upstream forwarding, the proxy
validates request paths. Requests that look like scans/probes are dropped with
an nginx-style `444` response (connection closed, no reply) and never reach
Ollama.

Validation includes:

- blocked file extensions (for example `.php`, `.env`, `.git`)
- blocked path prefixes (for example `/wp-admin`, `/admin`, `/cgi-bin`)
- a strict `max_path_length` (default `100`)
- a strict allowed-path policy (`/`, the status page path, `/api/`, `/v1/`)

Recent rejected probe paths are retained in request history for
`probe_collapse_after_seconds` (default 10 minutes), then collapsed into a
single placeholder line so scans do not dominate request history or per-path
counts.

## Run it

```bash
python3 proxy.py
python3 proxy.py --config /path/to/config.json
OLLAMA_FRONT_CONFIG=/path/to/config.json python3 proxy.py
```

Or install as a service (auto-detects Linux + systemd vs. macOS + launchd):

```bash
./install.sh                    # system unit on Linux (sudo), agent on macOS
./install.sh --user             # per-user service, no sudo
./install.sh --dry-run          # show what would be installed, change nothing
./install.sh --status           # show service state and recent logs
./install.sh --uninstall        # remove the service
```

`install.sh` renders the unit/plist from the current checkout path, user, and
`python3`, checks for `config.json` / `api_keys.json`, and verifies the proxy
answers on its listen port. On unsupported systems it warns and shows how to run
`proxy.py` manually.

### Configuration

Edit `config.json` to change ports, the forced model, rate limits, queueing, or
other behavior.

See [the configuration example](config.json.example) for all the default settings. These
values are used if no `config.json` exists.

#### Alternate config

You can select a config file at launch without modifying the default file:

```bash
python3 proxy.py --config /path/to/config.json
OLLAMA_FRONT_CONFIG=/path/to/config.json python3 proxy.py
```

Resolution order: `--config`, then `OLLAMA_FRONT_CONFIG`, then `config.json`
next to `proxy.py`.

## API keys

Copy `api_keys.json.example` to `api_keys.json` and edit it. The file is
reloaded automatically when it changes; no restart is needed.

```json
{
  "admin": ["replace-with-admin-key"],
  "unlimited": ["replace-with-unlimited-key"],
  "standard": ["replace-with-standard-key"]
}
```

## Expose with Tailscale Funnel

A handy usage for this proxy is to connect to a tailscale funnel so you can 
access an ollama instance from anywhere.

```bash
sudo tailscale funnel --bg 11435
```

Then use `https://<your-machine>.<tailnet>.ts.net` as your Ollama base URL.

## Testing

```bash
python3 test_proxy.py                  # full run
python3 test_proxy.py --skip-stream    # skip streaming disconnect test
python3 test_proxy.py --upstream-port 11434
```

`test_proxy.py` starts a throwaway proxy on an ephemeral port using `--config`
(your real `config.json` is not modified) and verifies core behavior including
the root response, forced-model tag stubs, rate limiting, auth handling, and
that killing a streaming client releases its concurrency slot promptly.
