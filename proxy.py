#!/usr/bin/env python3
"""Reverse proxy for Ollama with rate limiting, model forcing, and API-key tiers."""

import argparse
import json
import os
import sys
import time
import threading
import http.client
import html
import select
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections import defaultdict, deque
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def _resolve_config_path(argv=None):
    """Config path: --config PATH > $OLLAMA_FRONT_CONFIG > <script-dir>/config.json."""
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=None)
    try:
        ns, _unknown = parser.parse_known_args(argv)
    except SystemExit:
        ns = argparse.Namespace(config=None)
    if ns.config:
        return Path(ns.config).resolve()
    env = os.environ.get("OLLAMA_FRONT_CONFIG")
    if env:
        return Path(env).resolve()
    return BASE_DIR / "config.json"


CONFIG_PATH = _resolve_config_path()
KEYS_PATH = BASE_DIR / "api_keys.json"

with CONFIG_PATH.open() as f:
    CONFIG = json.load(f)

UPSTREAM_HOST = CONFIG["upstream_host"]
UPSTREAM_PORT = CONFIG["upstream_port"]
LISTEN_HOST = CONFIG["listen_host"]
LISTEN_PORT = CONFIG["listen_port"]
FORCED_MODEL = CONFIG["forced_model"]
RATE_LIMIT = CONFIG["rate_limit_per_minute"]
BLOCKED_DOWNLOAD_PATHS = set(CONFIG.get("blocked_download_paths", ["/api/pull"]))
STUB_TAGS = CONFIG.get("stub_tags", True)  # answer /api/tags locally (blocked model only)
TAGS_CACHE_REFRESH_SECONDS = CONFIG.get("tags_cache_refresh_seconds", 300)  # background TTL (0 = startup only)
# Lightweight GETs that never load/run a model — they skip the concurrency gate.
LIGHTWEIGHT_PATHS = set(CONFIG.get("lightweight_paths",
                       ["/", "/api/tags", "/api/version", "/api/ps"]))
# --- concurrency gate: serialize inference so a small box isn't oversubscribed ---
MAX_CONCURRENT = int(CONFIG.get("max_concurrent_requests", 1))
QUEUE_TIMEOUT = float(CONFIG.get("queue_timeout_seconds", 300))  # seconds to wait before 503
MAX_QUEUE = int(CONFIG.get("max_queue_size", 20))                # max waiting requests before 503
LOG_REQUESTS = CONFIG.get("log_requests", True)
# Cap verbose logs: only log security-relevant events by default, not health checks.
LOG_PATHS = set(CONFIG.get("log_paths")) if CONFIG.get("log_paths") else None  # None = log all

# --- concurrency gate: at most MAX_CONCURRENT requests run inside Ollama at once.
# Exceeding requests wait in a bounded queue; overflows/timeouts get 503.
_gate_sem = threading.BoundedSemaphore(MAX_CONCURRENT)
_gate_lock = threading.Lock()
_gate_waiters = 0
_gate_busy = 0  # requests currently inside upstream (for /status)


def _inc_busy(delta):
    global _gate_busy
    with _gate_lock:
        _gate_busy = max(0, _gate_busy + delta)


def release_slot():
    _inc_busy(-1)
    _gate_sem.release()


def queue_depth():
    with _gate_lock:
        return _gate_waiters


def busy_count():
    with _gate_lock:
        return _gate_busy


def needs_gate(path, method):
    """Inference paths queue; lightweight/info GETs do not.
    /api/tags is stubbed upstream of the gate and never reaches here, but
    list it as lightweight for safety if stubbing is disabled."""
    if path in LIGHTWEIGHT_PATHS and method == "GET":
        return False
    return True


# --- /api/tags stub cache ---
# At startup we fetch the full tag list from Ollama (Ollama has no per-model
# filter), extract ONLY the forced model's entry, and cache it. Non-admin
# /api/tags requests replay the cached entry — no Ollama roundtrip, no queue,
# and never revealing other (blocked) models.
_tags_entry = None
_tags_entry_lock = threading.Lock()
_tags_fetched_at = 0.0


def _fetch_tags_entry():
    """Fetch /api/tags from Ollama; return the forced model's entry, or None."""
    conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=5)
    try:
        conn.request("GET", "/api/tags")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        models = data.get("models", [])
        for m in models:
            # Match exact tag or bare name ("llama3.2" vs "llama3.2:latest").
            if m.get("name") == FORCED_MODEL or m.get("model") == FORCED_MODEL:
                return m
        # Some Ollama builds report ":latest" suffix; accept name without suffix.
        for m in models:
            base = (m.get("name") or "").split(":")[0]
            if base == FORCED_MODEL.split(":")[0]:
                return m
        return models[0] if models else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _refresh_tags_cache():
    global _tags_entry, _tags_fetched_at
    entry = _fetch_tags_entry()
    if entry is not None:
        with _tags_entry_lock:
            _tags_entry = entry
            _tags_fetched_at = time.time()
    return entry


def _get_cached_tags_entry():
    # Opportunistic TTL refresh so the stub stays accurate after pulls.
    if TAGS_CACHE_REFRESH_SECONDS and time.time() - _tags_fetched_at > TAGS_CACHE_REFRESH_SECONDS:
        _refresh_tags_cache()
    with _tags_entry_lock:
        return _tags_entry

# Endpoints whose JSON request body carries a "model" field.
MODEL_BODY_PATHS = (
    "/api/generate",
    "/api/chat",
    "/api/embed",
    "/api/embeddings",
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
)

_keys_lock = threading.Lock()
_keys_cache = {"mtime": 0, "data": {}}


def load_keys():
    """Reload api_keys.json when the file changes. Maps key -> tier."""
    try:
        mtime = KEYS_PATH.stat().st_mtime
    except FileNotFoundError:
        return {}
    with _keys_lock:
        if mtime != _keys_cache["mtime"]:
            with KEYS_PATH.open() as f:
                raw = json.load(f)
            # Support both {"key": "tier"} and {"tier": ["key1", ...]}
            keys = {}
            for k, v in raw.items():
                if isinstance(v, list):
                    for key in v:
                        keys[key] = k
                else:
                    keys[k] = v
            _keys_cache.update(mtime=mtime, data=keys)
        return dict(_keys_cache["data"])


def get_tier(headers):
    key = None
    auth = headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        key = auth[7:].strip()
    if not key:
        key = headers.get("X-API-Key", "").strip()
    if not key:
        return None, "anonymous"  # treated as standard
    tier = load_keys().get(key)
    if tier is None:
        return key, "invalid"
    return key, tier


# --- sliding-window rate limiter ---
_hits = defaultdict(deque)
_hits_lock = threading.Lock()

# --- observability for /status page ---
_START_TIME = time.time()
_stats_lock = threading.Lock()
_stats = {
    "total_requests": 0,
    "by_status": defaultdict(int),
    "by_tier": defaultdict(int),
    "by_path": defaultdict(int),
    "blocked_pulls": 0,
    "rate_limited": 0,
    "invalid_keys": 0,
    "upstream_errors": 0,
    "queue_rejected": 0,
    "queue_timed_out": 0,
    "first_request": None,
    "last_request": None,
    "recent": deque(maxlen=50),
}


def _record(path, tier, status, duration):
    now = time.time()
    with _stats_lock:
        _stats["total_requests"] += 1
        _stats["by_status"][status] += 1
        _stats["by_tier"][tier] += 1
        _stats["by_path"][path] += 1
        if status == 401:
            _stats["invalid_keys"] += 1
        elif status == 403:
            _stats["blocked_pulls"] += 1
        elif status == 429:
            _stats["rate_limited"] += 1
        elif status == 503:
            _stats["queue_rejected"] += 1
        elif status >= 500:
            _stats["upstream_errors"] += 1
        if _stats["first_request"] is None:
            _stats["first_request"] = now
        _stats["last_request"] = now
        _stats["recent"].appendleft({
            "time": time.strftime("%H:%M:%S", time.localtime(now)),
            "method": "",
            "path": path,
            "status": status,
            "tier": tier,
            "duration": round(duration, 3),
        })


def _rate_snapshot():
    now = time.monotonic()
    with _hits_lock:
        snap = []
        for ident, q in list(_hits.items()):
            cutoff = now - 60
            while q and q[0] < cutoff:
                q.popleft()
            snap.append({"identity": ident, "used": len(q), "limit": RATE_LIMIT})
        snap.sort(key=lambda x: x["identity"])
        return snap


def rate_limited(identity):
    now = time.monotonic()
    with _hits_lock:
        q = _hits[identity]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            return True
        q.append(now)
        return False


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    _status = 0
    _wait_seconds = 0.0
    _holds_slot = False
    _client_gone = False

    def _log(self, path, tier, extra=""):
        if not LOG_REQUESTS:
            return
        if LOG_PATHS is not None and path not in LOG_PATHS:
            return
        # One compact line per request: IP, method, path, status, tier.
        # No headers/bodies logged — keeps volume + SSD wear minimal.
        wait_note = f" waited={self._wait_seconds}s" if self._wait_seconds else ""
        if self._client_gone:
            extra += " client-disconnected"
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {self.client_address[0]} "
              f"{self.command} {path} -> {self._status} tier={tier}{extra}{wait_note}", flush=True)

    def _is_client_alive(self):
        # Non-blocking peek: if the client closed the socket, select() is readable
        # and recv(MSG_PEEK) reports EOF. Does not consume anything.
        try:
            if select.select([self.connection], [], [], 0)[0]:
                if self.connection.recv(1, socket.MSG_PEEK) == b"":
                    self._client_gone = True
                    return False
        except (OSError, ValueError):
            self._client_gone = True
            return False
        return True

    def _serve_stub_tags(self):
        # Replay the cached real entry for the forced model (fetched at startup).
        # Only the forced model is ever visible to non-admins, so they can never
        # discover unblocked models or trigger a download by requesting one.
        entry = _get_cached_tags_entry()
        if entry is None:
            # Fallback: Ollama unreachable at startup — serve an empty list.
            entry = {"name": FORCED_MODEL, "model": FORCED_MODEL,
                     "modified_at": "2026-01-01T00:00:00Z", "size": 0, "digest": "",
                     "details": {"family": "stub", "parameter_size": ""}}
        self._status = 200
        self._respond(200, {"models": [entry]})

    def _serve_root(self):
        self._status = 200
        try:
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, OSError):
                self._client_gone = True
        except (BrokenPipeError, ConnectionResetError, OSError):
            self._client_gone = True

    def _enter_gate(self, path, tier):
        """Block until a concurrency slot frees; False if queue full or timed out."""
        if not needs_gate(path, self.command):
            return True  # lightweight GET: no slot taken
        global _gate_waiters
        # fast-path: no waiting if a slot is free, don't count as waiter
        if _gate_sem.acquire(blocking=False):
            _inc_busy(1)
            self._holds_slot = True
            return True
        with _gate_lock:
            if _gate_waiters >= MAX_QUEUE:
                self._status = 503
                self._respond(503, {"error": f"proxy busy: queue full (max {MAX_QUEUE} waiting, {MAX_CONCURRENT} running)"})
                _record(path, tier, 503, 0.0)
                return False
            _gate_waiters += 1
        t0 = time.monotonic()
        try:
            got = _gate_sem.acquire(timeout=QUEUE_TIMEOUT)
            if got:
                _inc_busy(1)
                self._holds_slot = True
            else:
                self._status = 503
                self._respond(503, {"error": f"proxy busy: timed out after {int(QUEUE_TIMEOUT)}s waiting (limit {MAX_CONCURRENT} concurrent)"})
                _record(path, tier, 503, time.monotonic() - t0)
            return got
        finally:
            self._wait_seconds = round(time.monotonic() - t0, 3)
            with _gate_lock:
                _gate_waiters -= 1

    def _handle(self):
        start = time.monotonic()
        # Root liveness probe: plain-text OK, no upstream call, no queue.
        if self.path.split("?", 1)[0] == "/" and self.command == "GET":
            self._serve_root()
            _record("/", "anonymous", 200, 0.0)
            return
        start = time.monotonic()
        # Status page is handled locally (not proxied) so it works even when Ollama is down.
        if self.path.split("?", 1)[0] == "/status" and self.command == "GET":
            self._serve_status()
            return
        # Stub /api/tags locally: non-admins only ever see the forced model.
        # Answers instantly, never hits Ollama, never queues. Admins see the real list.
        if self.path.split("?", 1)[0] == "/api/tags" and STUB_TAGS:
            key0, tier0 = get_tier(self.headers)
            if tier0 != "admin":
                self._serve_stub_tags()
                _record("/api/tags", tier0, 200, 0.0)
                return
        key, tier = get_tier(self.headers)

        if tier == "invalid":
            self._status = 401
            self._respond(401, {"error": "invalid api key"})
            return

        # Block downloads / pulls for non-admins.
        path = self.path.split("?", 1)[0]
        if path in BLOCKED_DOWNLOAD_PATHS and tier != "admin":
            self._status = 403
            self._respond(403, {"error": f"{path} is disabled"})
            return

        # Rate limit anonymous + standard.
        if tier in ("anonymous", "standard"):
            identity = key or self.client_address[0]
            if rate_limited(identity):
                self._status = 429
                self._respond(429, {"error": f"rate limit exceeded ({RATE_LIMIT}/min)"})
                return

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None

        # Concurrency gate: hold a slot for the whole upstream exchange.
        # Status page bypasses it (it never holds a slot).
        if not self._enter_gate(path, tier):
            return
        try:
            # Force model for non-admins.
            if tier != "admin" and body and path in MODEL_BODY_PATHS:
                try:
                    payload = json.loads(body)
                    if isinstance(payload, dict):
                        payload["model"] = FORCED_MODEL
                        body = json.dumps(payload).encode()
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass

            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in ("host", "content-length", "connection")}
            if body is not None:
                headers["Content-Length"] = str(len(body))

            try:
                conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=600)
                conn.request(self.command, self.path, body=body, headers=headers)
                resp = conn.getresponse()
            except (ConnectionRefusedError, OSError) as e:
                self._status = 502
                self._respond(502, {"error": f"upstream unavailable: {e}"})
                return

            self._status = resp.status
            self.send_response(resp.status)
            content_length = resp.getheader("Content-Length")
            for k, v in resp.getheaders():
                if k.lower() in ("transfer-encoding", "connection", "content-length"):
                    continue
                self.send_header(k, v)
            if content_length is not None:
                self.send_header("Content-Length", content_length)
            else:
                self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            try:
                if content_length is not None:
                    try:
                        self.wfile.write(resp.read())
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        self._client_gone = True
                else:
                    # Stream (chunked input from upstream); re-chunk to client.
                    # Poll for client disconnect before each blocking read so an
                    # abandoned stream frees its slot promptly instead of waiting
                    # for the next write to fail or generation to finish.
                    client_dead = False
                    while True:
                        if not self._is_client_alive():
                            client_dead = True
                            break
                        chunk = resp.read(8192)
                        if not chunk:
                            break
                        self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                        self.wfile.flush()
                    if not client_dead:
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                # Client went away mid-response. Record it, then abort the upstream
                # connection so Ollama stops generating for nobody. The `finally`
                # below releases the concurrency slot immediately.
                self._client_gone = True
            finally:
                conn.close()
        finally:
            if self._holds_slot:
                release_slot()
                self._holds_slot = False
        duration = time.monotonic() - start
        self._log(path, tier, f" {round(duration, 3)}s")
        _record(path, tier, self._status, duration)
        # keep method in recent-request record
        with _stats_lock:
            if _stats["recent"]:
                _stats["recent"][0]["method"] = self.command

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = _handle

    def _respond(self, status, obj):
        # Drain any unread request body so it isn't parsed as a new request.
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
        except (ValueError, OSError):
            pass
        data = json.dumps(obj).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, OSError):
            self._client_gone = True

    def _serve_status(self):
        # Probe upstream health (short timeout so the page never hangs).
        try:
            upstream = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=1)
            upstream.request("GET", "/api/tags")
            r = upstream.getresponse()
            r.read()
            upstream_ok = 200 <= r.status < 300
            upstream_note = f"HTTP {r.status}"
            upstream.close()
        except Exception as e:
            upstream_ok = False
            upstream_note = f"{type(e).__name__}: {e}"

        now = time.time()
        uptime_s = now - _START_TIME
        h, rem = divmod(int(uptime_s), 3600)
        m, s = divmod(rem, 60)

        with _stats_lock:
            total = _stats["total_requests"]
            by_status = dict(_stats["by_status"])
            by_tier = dict(_stats["by_tier"])
            by_path = dict(_stats["by_path"])
            blocked = _stats["blocked_pulls"]
            limited = _stats["rate_limited"]
            invalid = _stats["invalid_keys"]
            upstream_errs = _stats["upstream_errors"]
            queue_rejected = _stats["queue_rejected"]
            first = _stats["first_request"]
            last = _stats["last_request"]
            recent = list(_stats["recent"])

        busy_now = busy_count()
        queued_now = queue_depth()

        rate_rows = _rate_snapshot()
        keys = load_keys()
        tiers = defaultdict(int)
        for tier in keys.values():
            tiers[tier] += 1

        def fmt_time(ts):
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "—"

        def esc(x):
            return html.escape(str(x))

        status_rows = "".join(
            f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>" for k, v in sorted(by_status.items())
        ) or "<tr><td colspan=2>no requests yet</td></tr>"
        tier_rows = "".join(
            f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>" for k, v in sorted(by_tier.items())
        ) or "<tr><td colspan=2>no requests yet</td></tr>"
        path_rows = "".join(
            f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>"
            for k, v in sorted(by_path.items(), key=lambda x: -x[1])
        ) or "<tr><td colspan=2>no requests yet</td></tr>"
        recent_rows = "".join(
            f"<tr><td>{esc(r['time'])}</td><td>{esc(r['method'])}</td>"
            f"<td>{esc(r['path'])}</td><td>{esc(r['status'])}</td>"
            f"<td>{esc(r['tier'])}</td><td>{esc(r['duration'])}s</td></tr>"
            for r in recent
        ) or "<tr><td colspan=6>no requests yet</td></tr>"
        rate_html_rows = "".join(
            f"<tr><td>{esc(r['identity'])}</td><td>{r['used']}/{r['limit']}</td>"
            f"<td class={'hot' if r['used'] >= r['limit'] else ''}>"
            f"{'LIMITED' if r['used'] >= r['limit'] else 'ok'}</td></tr>"
            for r in rate_rows
        ) or "<tr><td colspan=3>no active rate-limit windows</td></tr>"
        tier_key_rows = "".join(
            f"<tr><td>{esc(t)}</td><td>{n}</td></tr>" for t, n in sorted(tiers.items())
        ) or "<tr><td colspan=2>no API keys configured</td></tr>"

        health_class = "ok" if upstream_ok else "bad"
        body = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>ollama-front status</title>
<meta http-equiv="refresh" content="10">
<style>
body{{font-family:system-ui,sans-serif;margin:2rem;max-width:900px;line-height:1.4}}
h1{{font-size:1.4rem}} h2{{font-size:1rem;margin-top:1.5rem}}
table{{border-collapse:collapse;min-width:280px}} td,th{{border:1px solid #ccc;padding:.3rem .6rem;text-align:left}}
.ok{{color:green;font-weight:bold}} .bad{{color:red;font-weight:bold}} .hot{{color:red}}
.cards{{display:flex;gap:1rem;flex-wrap:wrap}} .card{{border:1px solid #ddd;border-radius:6px;padding:.6rem 1rem}}
.card b{{font-size:1.4rem}} .card span{{display:block;color:#666;font-size:.8rem}}
</style></head><body>
<h1>ollama-front status</h1>
<p>Uptime: {h}h {m}m {s}s (since {esc(fmt_time(_START_TIME))})</p>
<p>Upstream Ollama ({esc(UPSTREAM_HOST)}:{UPSTREAM_PORT}): <span class="{health_class}">{esc(upstream_note)}</span></p>
<div class="cards">
<div class="card"><b>{total}</b><span>total requests</span></div>
<div class="card"><b>{esc(FORCED_MODEL)}</b><span>forced model</span></div>
<div class="card"><b>{MAX_CONCURRENT}</b><span>max concurrent in Ollama</span></div>
<div class="card"><b>{busy_now}</b><span>running now</span></div>
<div class="card"><b>{queued_now}</b><span>queued now</span></div>
<div class="card"><b>{queue_rejected}</b><span>queue rejected (503)</span></div>
<div class="card"><b>{RATE_LIMIT}/min</b><span>rate limit</span></div>
<div class="card"><b>{limited}</b><span>rate-limited (429)</span></div>
<div class="card"><b>{blocked}</b><span>blocked pulls (403)</span></div>
<div class="card"><b>{invalid}</b><span>invalid keys (401)</span></div>
<div class="card"><b>{upstream_errs}</b><span>upstream errors (5xx)</span></div>
</div>
<h2>Request history</h2>
<p>First: {esc(fmt_time(first))} &nbsp;·&nbsp; Last: {esc(fmt_time(last))}</p>
<table><tr><th>Time</th><th>Method</th><th>Path</th><th>Status</th><th>Tier</th><th>Duration</th></tr>{recent_rows}</table>
<h2>Requests by status</h2><table>{status_rows}</table>
<h2>Requests by tier</h2><table>{tier_rows}</table>
<h2>Requests by path</h2><table>{path_rows}</table>
<h2>Rate-limit windows (current minute)</h2>
<table><tr><th>IP / key</th><th>Used</th><th>State</th></tr>{rate_html_rows}</table>
<h2>API keys configured</h2><table><tr><th>Tier</th><th>Key count</th></tr>{tier_key_rows}</table>
<p><small>Auto-refreshes every 10s. API keys are never displayed.</small></p>
</body></html>"""
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        # Suppress BaseHTTPRequestHandler's default verbose access log;
        # _log() above is the single source of truth.
        pass


if __name__ == "__main__":
    # Warm the /api/tags stub cache so the first non-admin call is instant.
    _refresh_tags_cache()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), ProxyHandler)
    print(f"ollama-front listening on {LISTEN_HOST}:{LISTEN_PORT}, "
          f"proxying to {UPSTREAM_HOST}:{UPSTREAM_PORT}, forcing model '{FORCED_MODEL}', "
          f"concurrency={MAX_CONCURRENT}, queue={MAX_QUEUE}x{int(QUEUE_TIMEOUT)}s")
    server.serve_forever()
