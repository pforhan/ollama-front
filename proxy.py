#!/usr/bin/env python3
"""Reverse proxy for Ollama with global rate limiting, model forcing, and API-key tiers.

Rate limit is a single global sliding window (starts per minute) for
anonymous+standard traffic. All tiers that reach upstream share the same
concurrency queue (gate) — admin/unlimited are exempt only from the 429
rate limit, not from queueing.
Client source for logs/status comes from X-Forwarded-For /
Tailscale-User-Login only when the TCP peer is loopback (Tailscale
serve/funnel), otherwise the raw peer IP — so a directly-exposed proxy
can't be spoofed via those headers."""

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
BLOCKED_DOWNLOAD_PATHS = set(CONFIG.get("blocked_download_paths",
                             ["/api/pull", "/api/create", "/api/copy", "/api/delete", "/api/push"]))
STUB_TAGS = CONFIG.get("stub_tags", True)  # answer /api/tags + /v1/models locally (blocked model only)
TAGS_CACHE_REFRESH_SECONDS = CONFIG.get("tags_cache_refresh_seconds", 300)  # background TTL (0 = startup only)
# Lightweight GETs that never load/run a model — they skip the concurrency gate.
LIGHTWEIGHT_PATHS = set(CONFIG.get("lightweight_paths",
                       ["/", "/api/tags", "/api/version", "/api/ps", "/v1/models"]))
# --- concurrency gate: serialize inference so a small box isn't oversubscribed ---
MAX_CONCURRENT = int(CONFIG.get("max_concurrent_requests", 1))
QUEUE_TIMEOUT = float(CONFIG.get("queue_timeout_seconds", 300))  # seconds to wait before 503
MAX_QUEUE = int(CONFIG.get("max_queue_size", 20))                # max waiting requests before 503
GATE_POLL_INTERVAL = 0.5  # queued waiters re-check client liveness at this cadence
LOG_REQUESTS = CONFIG.get("log_requests", True)
# Plain-text body served for GET / (liveness probe). Kept as a simple string.
ROOT_RESPONSE = CONFIG.get("root_response", "OK")
if ROOT_RESPONSE is None:
    ROOT_RESPONSE = "OK"
elif not isinstance(ROOT_RESPONSE, str):
    ROOT_RESPONSE = str(ROOT_RESPONSE)
# Cap verbose logs: only log security-relevant events by default, not health checks.
LOG_PATHS = set(CONFIG.get("log_paths")) if CONFIG.get("log_paths") else None  # None = log all

# --- concurrency gate: at most MAX_CONCURRENT requests run inside Ollama at once.
# Exceeding requests wait in a bounded queue; overflows/timeouts get 503.
_gate_sem = threading.BoundedSemaphore(MAX_CONCURRENT)
_gate_lock = threading.RLock()  # RLock: helpers like _inc_busy nest inside lock sections
_gate_waiters = 0
_gate_busy = 0  # requests currently inside upstream (for /status)
# Per-request detail for the /status page: the same requests counted by
# _gate_busy/_gate_waiters, with method/path/tier/client + timing so the
# page shows *what* is running/queued, not just how many.
_gate_running = []  # entries holding a slot (inside upstream)
_gate_queued = []   # entries waiting for a slot, in arrival order


def _inc_busy(delta):
    global _gate_busy
    with _gate_lock:
        _gate_busy = max(0, _gate_busy + delta)


def _remove_gate_entry(entries, entry):
    """Remove entry by identity (two requests can share all field values).
    Caller must hold _gate_lock."""
    for i, e in enumerate(entries):
        if e is entry:
            del entries[i]
            return True
    return False


def _gate_snapshot():
    """Atomic (busy, queued, running-copy, queued-copy) for /status.

    Running copies gain "elapsed" (seconds since the slot was taken);
    queued copies gain "position" (1-based arrival order) and "waiting"
    (seconds since joining the queue). Copies are safe to format outside
    the lock.
    """
    with _gate_lock:
        now_mono = time.monotonic()
        running = []
        for e in list(_gate_running):
            d = dict(e)
            base = e.get("started_mono") or e.get("queued_mono") or now_mono
            d["elapsed"] = max(0.0, now_mono - base)
            running.append(d)
        queued = []
        for i, e in enumerate(list(_gate_queued)):
            d = dict(e)
            d["position"] = i + 1
            base = e.get("queued_mono") or now_mono
            d["waiting"] = max(0.0, now_mono - base)
            queued.append(d)
        return _gate_busy, _gate_waiters, running, queued


def release_slot(entry=None):
    with _gate_lock:
        if entry is not None:
            _remove_gate_entry(_gate_running, entry)
    _inc_busy(-1)
    _gate_sem.release()


def queue_depth():
    with _gate_lock:
        return _gate_waiters


def busy_count():
    with _gate_lock:
        return _gate_busy


def needs_gate(path, method):
    """Inference paths queue for ALL tiers (admin/unlimited/standard/anonymous).
    Lightweight/info GETs do not, regardless of tier, because they never load
    a model. This is path-based, not key-based.
    /api/tags and /v1/models are stubbed upstream of the gate for non-admins
    and never reach here, but list them as lightweight for safety if stubbing
    is disabled.
    /api/show (POST) is likewise info-only: it reads the model manifest
    without running inference, so it never queues — but its `model`
    field is still forced (see MODEL_BODY_PATHS) so non-admins can only
    ever read the forced model."""
    if path in LIGHTWEIGHT_PATHS and method == "GET":
        return False
    if path == "/api/show":
        return False
    if path == "/v1/models" or path.startswith("/v1/models/"):
        return False
    return True


def _is_blocked_path(path):
    """Mutation/download paths blocked for non-admins.

    Exact match against BLOCKED_DOWNLOAD_PATHS (default: pull/create/copy/
    delete/push) plus the blob prefix: /api/blobs/<digest> carries the
    digest in the URL, so an exact-set match can never catch it."""
    if path in BLOCKED_DOWNLOAD_PATHS:
        return True
    if path == "/api/blobs" or path.startswith("/api/blobs/"):
        return True
    return False


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


# --- /v1/models stub cache (OpenAI-compatible, like /api/tags) ---
# Same idea as tags: cache only the forced model's entry from upstream
# /v1/models so non-admins never discover other models. Reuses the tags
# TTL (TAGS_CACHE_REFRESH_SECONDS).
_v1_entry = None
_v1_entry_lock = threading.Lock()
_v1_fetched_at = 0.0


def _fetch_v1_entry():
    """Fetch /v1/models from Ollama; return forced model's entry, or None."""
    conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=5)
    try:
        conn.request("GET", "/v1/models")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        models = data.get("data", [])
        for m in models:
            if m.get("id") == FORCED_MODEL:
                return m
        for m in models:
            base = (m.get("id") or "").split(":")[0]
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


def _refresh_v1_cache():
    global _v1_entry, _v1_fetched_at
    entry = _fetch_v1_entry()
    if entry is not None:
        with _v1_entry_lock:
            _v1_entry = entry
            _v1_fetched_at = time.time()
    return entry


def _get_cached_v1_entry():
    if TAGS_CACHE_REFRESH_SECONDS and time.time() - _v1_fetched_at > TAGS_CACHE_REFRESH_SECONDS:
        _refresh_v1_cache()
    with _v1_entry_lock:
        return _v1_entry

# Endpoints whose JSON request body carries a "model" field.
MODEL_BODY_PATHS = (
    "/api/generate",
    "/api/chat",
    "/api/embed",
    "/api/embeddings",
    "/api/show",
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


# --- global sliding-window rate limiter ---
# Single bucket for all anonymous+standard traffic, regardless of source IP or key.
# (Per-IP/per-key buckets were misleading behind Tailscale serve/funnel, where
# every client looks like 127.0.0.1, and leaked raw keys on /status.)
_hits = deque()
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
        # Non-destructive count: GET /status shouldn't mutate the limiter.
        # Only rate_limited() prunes expired starts.
        used = sum(1 for t in _hits if now - t <= 60)
    return [{"identity": "global", "used": used, "limit": RATE_LIMIT}]


def rate_limited():
    now = time.monotonic()
    with _hits_lock:
        while _hits and now - _hits[0] > 60:
            _hits.popleft()
        if len(_hits) >= RATE_LIMIT:
            return True
        _hits.append(now)
        return False


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    _status = 0
    _wait_seconds = 0.0
    _holds_slot = False
    _gate_entry = None
    _client_gone = False

    def _client_ip(self):
        """Real source IP for logging/status.

        When Tailscale serve/funnel is in front, tailscaled opens a new
        loopback TCP connection, so client_address is always 127.0.0.1/::1
        and the original IP is in X-Forwarded-For. Trust that header only
        when the peer is loopback — otherwise anyone who can reach the proxy
        directly could forge it.
        """
        peer = self.client_address[0]
        if peer in ("127.0.0.1", "::1"):
            xff = self.headers.get("X-Forwarded-For", "").strip()
            if xff:
                first = xff.split(",")[0].strip()
                if first:
                    return first
        return peer

    def _client_label(self):
        """Display label for logs /status: Tailscale login + IP when available.

        Prefer Tailscale-User-Login (tailnet serve only) because it's a stable
        human identity vs a 100.x IP. Same loopback-only trust rule as _client_ip.
        """
        peer = self.client_address[0]
        if peer in ("127.0.0.1", "::1"):
            login = self.headers.get("Tailscale-User-Login", "").strip()
            if login:
                ip = self._client_ip()
                return f"{login} ({ip})" if ip and ip != login else login
        return self._client_ip()

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
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {self._client_label()} "
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

    def _serve_stub_v1_models(self):
        entry = _get_cached_v1_entry()
        if entry is None:
            entry = {"id": FORCED_MODEL, "object": "model",
                     "created": 0, "owned_by": "library"}
        self._status = 200
        self._respond(200, {"object": "list", "data": [entry]})

    def _serve_stub_v1_model(self):
        # Single-model GET: always return the forced model (like show forcing).
        entry = _get_cached_v1_entry()
        if entry is None:
            entry = {"id": FORCED_MODEL, "object": "model",
                     "created": 0, "owned_by": "library"}
        self._status = 200
        self._respond(200, entry)

    def _serve_root(self):
        self._status = 200
        try:
            body = ROOT_RESPONSE.encode("utf-8")
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
        """Block until a concurrency slot frees; False if queue full, timed out, or client gone.

        Universal gate: every tier queues here (no per-key exemption). Only
        lightweight GETs — and POST /api/show, which never runs inference —
        skip it via needs_gate()."""
        if not needs_gate(path, self.command):
            return True  # lightweight info endpoint: no slot taken
        global _gate_waiters
        # One entry per gated request, tracked in _gate_running/_gate_queued
        # (and removed by _handle via release_slot) so /status shows detail.
        entry = {
            "method": self.command,
            "path": path,
            "tier": tier,
            "client": self._client_label(),
            "queued_at": time.time(),
            "queued_mono": time.monotonic(),
            "started_at": None,
            "started_mono": None,
            "wait_seconds": 0.0,
        }
        self._gate_entry = entry
        # fast-path: no waiting if a slot is free, don't count as waiter
        if _gate_sem.acquire(blocking=False):
            entry["started_at"] = time.time()
            entry["started_mono"] = time.monotonic()
            with _gate_lock:
                _gate_running.append(entry)
                _inc_busy(1)
            self._holds_slot = True
            return True
        with _gate_lock:
            if _gate_waiters >= MAX_QUEUE:
                self._gate_entry = None
                self._status = 503
                self._respond(503, {"error": f"proxy busy: queue full (max {MAX_QUEUE} waiting, {MAX_CONCURRENT} running)"})
                _record(path, tier, 503, 0.0)
                return False
            _gate_waiters += 1
            _gate_queued.append(entry)
        t0 = time.monotonic()
        deadline = t0 + QUEUE_TIMEOUT
        try:
            got = False
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                # Poll in short slices so a client that disconnects while
                # queued bails promptly instead of occupying a queue entry
                # until the full queue timeout expires.
                slice_timeout = min(GATE_POLL_INTERVAL, remaining)
                if _gate_sem.acquire(timeout=slice_timeout):
                    got = True
                    break
                if time.monotonic() >= deadline:
                    break
                if not self._is_client_alive():
                    self._status = 503
                    self._log(path, tier, f" {round(time.monotonic() - t0, 3)}s queued-disconnected")
                    # Best-effort: the socket is already dead, so _respond's
                    # write will fail harmlessly and set _client_gone (already
                    # set by _is_client_alive). Still call it for consistent
                    # 503 semantics + queue_rejected accounting.
                    self._respond(503, {"error": "proxy busy: client disconnected while queued"})
                    _record(path, tier, 503, time.monotonic() - t0)
                    return False
            if got:
                entry["started_at"] = time.time()
                entry["started_mono"] = time.monotonic()
                entry["wait_seconds"] = max(0.0, entry["started_mono"] - entry["queued_mono"])
                with _gate_lock:
                    _remove_gate_entry(_gate_queued, entry)
                    _gate_running.append(entry)
                    _inc_busy(1)
                self._holds_slot = True
            else:
                self._gate_entry = None
                self._status = 503
                self._respond(503, {"error": f"proxy busy: timed out after {int(QUEUE_TIMEOUT)}s waiting (limit {MAX_CONCURRENT} concurrent)"})
                _record(path, tier, 503, time.monotonic() - t0)
            return got
        finally:
            self._wait_seconds = round(time.monotonic() - t0, 3)
            with _gate_lock:
                _gate_waiters -= 1
                _remove_gate_entry(_gate_queued, entry)

    def _handle(self):
        start = time.monotonic()
        # Root liveness probe: plain-text, no upstream call, no queue.
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
        # Stub /v1/models and /v1/models/:id locally (same idea as tags).
        # Non-admins only ever see the forced model, even when they ask for
        # another id (like /api/show forcing). Admins see the real list.
        if STUB_TAGS:
            _p = self.path.split("?", 1)[0]
            if _p == "/v1/models" or _p.startswith("/v1/models/"):
                key0, tier0 = get_tier(self.headers)
                if tier0 != "admin":
                    if _p == "/v1/models":
                        self._serve_stub_v1_models()
                    else:
                        self._serve_stub_v1_model()
                    _record(_p, tier0, 200, 0.0)
                    return
        key, tier = get_tier(self.headers)

        if tier == "invalid":
            self._status = 401
            self._respond(401, {"error": "invalid api key"})
            return

        # Block mutations/downloads for non-admins (pull/create/copy/delete/
        # push/blobs) — they carry arbitrary model names/digests and forcing
        # would be unsafe (e.g. /api/create would overwrite the forced model).
        path = self.path.split("?", 1)[0]
        if _is_blocked_path(path) and tier != "admin":
            self._status = 403
            self._respond(403, {"error": f"{path} is disabled"})
            return

        # Rate limit anonymous + standard globally (admin/unlimited exempt from 429,
        # but NOT from the queue below — everyone hits the gate).
        if tier in ("anonymous", "standard"):
            if rate_limited():
                self._status = 429
                self._respond(429, {"error": f"rate limit exceeded ({RATE_LIMIT}/min)"})
                return

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None

        # Concurrency gate: hold a slot for the whole upstream exchange.
        # Universal — every tier (admin/unlimited/standard/anonymous) queues here;
        # only lightweight GETs and POST /api/show skip it (path-based, see
        # needs_gate), and /, /status, non-admin /api/tags stub never reach here.
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
                release_slot(self._gate_entry)
                self._gate_entry = None
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

        busy_now, queued_now, running_now, queued_list = _gate_snapshot()

        rate_rows = _rate_snapshot()
        keys = load_keys()
        tiers = defaultdict(int)
        for tier in keys.values():
            tiers[tier] += 1

        def fmt_time(ts):
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "—"

        def fmt_hms(ts):
            return time.strftime("%H:%M:%S", time.localtime(ts)) if ts else "—"

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
        running_rows = "".join(
            f"<tr><td>{esc(fmt_hms(r.get('started_at') or r.get('queued_at')))}</td>"
            f"<td>{esc(r['method'])}</td><td>{esc(r['path'])}</td>"
            f"<td>{esc(r['tier'])}</td><td>{esc(r['client'])}</td>"
            f"<td>{r.get('elapsed', 0.0):.1f}s</td>"
            f"<td>{r.get('wait_seconds', 0.0):.1f}s</td></tr>"
            for r in running_now
        ) or '<tr><td colspan="7">nothing running</td></tr>'
        queued_rows = "".join(
            f"<tr><td>#{r['position']}</td>"
            f"<td>{esc(fmt_hms(r.get('queued_at')))}</td>"
            f"<td>{esc(r['method'])}</td><td>{esc(r['path'])}</td>"
            f"<td>{esc(r['tier'])}</td><td>{esc(r['client'])}</td>"
            f"<td>{r.get('waiting', 0.0):.1f}s</td></tr>"
            for r in queued_list
        ) or '<tr><td colspan="7">nothing queued</td></tr>'

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
<h2>Currently running ({busy_now})</h2>
<p>{busy_now} of {MAX_CONCURRENT} slots in use · {queued_now} of {MAX_QUEUE} queue places filled (queue timeout {int(QUEUE_TIMEOUT)}s)</p>
<table><tr><th>Started</th><th>Method</th><th>Path</th><th>Tier</th><th>Client</th><th>Elapsed</th><th>Waited</th></tr>{running_rows}</table>
<h2>Currently queued ({queued_now})</h2>
<table><tr><th>#</th><th>Queued</th><th>Method</th><th>Path</th><th>Tier</th><th>Client</th><th>Waiting</th></tr>{queued_rows}</table>
<h2>Request history</h2>
<p>First: {esc(fmt_time(first))} &nbsp;·&nbsp; Last: {esc(fmt_time(last))}</p>
<table><tr><th>Time</th><th>Method</th><th>Path</th><th>Status</th><th>Tier</th><th>Duration</th></tr>{recent_rows}</table>
<h2>Requests by status</h2><table>{status_rows}</table>
<h2>Requests by tier</h2><table>{tier_rows}</table>
<h2>Requests by path</h2><table>{path_rows}</table>
<h2>Rate-limit window (current minute)</h2>
<table><tr><th>Scope</th><th>Used</th><th>State</th></tr>{rate_html_rows}</table>
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
    # Warm the stub caches so the first non-admin call is instant.
    _refresh_tags_cache()
    _refresh_v1_cache()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), ProxyHandler)
    print(f"ollama-front listening on {LISTEN_HOST}:{LISTEN_PORT}, "
          f"proxying to {UPSTREAM_HOST}:{UPSTREAM_PORT}, forcing model '{FORCED_MODEL}', "
          f"concurrency={MAX_CONCURRENT}, queue={MAX_QUEUE}x{int(QUEUE_TIMEOUT)}s")
    server.serve_forever()
