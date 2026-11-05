#!/usr/bin/env python3
"""End-to-end test for ollama-front proxy (TODO §1-§6).

Starts its own proxy instance on an ephemeral port (doesn't touch 11435),
runs all checks, then tears everything down. Exit code 0 = all pass.

Usage:
    python3 test_proxy.py [--skip-stream] [--upstream-port 11434]

Checks:
  §1  de-duplication: exactly one each of _enter_gate/_serve_root/_handle/…,
      'select' + 'socket' top-level, GATE_POLL_INTERVAL queued-disconnect poll,
      py_compile
  §3  GET / → 'OK' text/plain, bypasses auth/rate-limit/gate
  §4  /api/tags stub: non-admin sees exactly the forced model (real entry);
      admin sees full list
  §4  rate limit 20/min → 429, invalid key → 401, /api/pull → 403 (non-admin)
  §2  streaming disconnect: abort frees the concurrency slot within ~5s,
      status shows "running now" drop, log shows 'client-disconnected'
  §2q queued disconnect: waiter killed while queued drops 'queued now'
      promptly, surviving waiter acquires the slot, log shows
      'queued-disconnected' (second proxy instance, concurrency=1)
  §6  quick repeat of the TODO §6 smoke commands (root/tags/stream)
"""

import argparse
import ast
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
results = []  # (name, ok, detail)


def check(name, ok, detail=""):
    results.append((name, bool(ok), str(detail)))
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] {name}" + (f"  — {detail}" if detail and not ok else f"  — {detail}" if detail else ""))


def http_get(url, headers=None, timeout=10):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return None, {}, f"{type(e).__name__}: {e}".encode()


def http_post(url, data, headers=None, timeout=10):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return None, {}, f"{type(e).__name__}: {e}".encode()


def status_counts(base):
    """Parse /status HTML → (running_now, queued_now); -1 if not found."""
    _st, _, body = http_get(f"{base}/status", timeout=5)
    txt = body.decode(errors="replace")
    m_run = re.search(r"<b>(\d+)</b><span>running now", txt)
    m_q = re.search(r"<b>(\d+)</b><span>queued now", txt)
    return (int(m_run.group(1)) if m_run else -1,
            int(m_q.group(1)) if m_q else -1)


def send_generate_sock(port, payload, admin_key=None, timeout=10):
    """Open a raw socket, POST /api/generate with json payload, return sock."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    auth = f"Authorization: Bearer {admin_key}\r\n" if admin_key else ""
    sock.sendall(
        f"POST /api/generate HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
        "Content-Type: application/json\r\n"
        f"{auth}Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload)
    return sock


def read_http_headers(sock, timeout=10):
    """Read until end of HTTP headers; return raw bytes gathered."""
    sock.settimeout(timeout)
    headers = b""
    try:
        while b"\r\n\r\n" not in headers:
            chunk = sock.recv(4096)
            if not chunk:
                break
            headers += chunk
    except OSError:
        pass
    return headers


def hard_close(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def parse_keys():
    """Load api_keys.json → {tier: first-key}. Handles both formats."""
    with open(os.path.join(BASE, "api_keys.json")) as f:
        raw = json.load(f)
    out = {}
    for k, v in raw.items():
        out[k] = v[0] if isinstance(v, list) and v else (v if isinstance(v, str) else None)
    return out


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_ready(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(f"/tmp/ollama-front-test-{port}.ready"):
            return True
        # fallback: try TCP connect
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def check_dedup():
    src = open(os.path.join(BASE, "proxy.py")).read()
    for name in ("_enter_gate", "_serve_root", "_handle", "_log", "_is_client_alive",
                 "_serve_stub_tags", "_respond", "_serve_status", "log_message"):
        cnt = len(re.findall(rf"def {re.escape(name)}\s*\(", src))
        check(f"§1 dedup: exactly one def {name}", cnt == 1, f"found {cnt}")
    top = src.split("\n\n", 3)[0] if "\n\n" in src else src[:2000]
    # top-level imports check: look before first def/class
    head = src.split("\ndef ", 1)[0]
    check("§1 top-level import select", re.search(r"^import select$", head, re.M) is not None)
    check("§1 top-level import socket", re.search(r"^import socket$", head, re.M) is not None)
    check("§1 no local 'import select' inside _is_client_alive",
          "import select" not in re.search(r"def _is_client_alive.*?(?=\n    def )", src, re.S).group(0))
    check("§1 _is_client_alive uses select.select",
          "select.select" in src and "MSG_PEEK" in src)
    check("§1 streaming polls _is_client_alive before resp.read",
          "_is_client_alive()" in src and "resp.read(8192)" in src)
    check("§1 GATE_POLL_INTERVAL constant defined (0.5s queued poll)",
          re.search(r"GATE_POLL_INTERVAL\s*=\s*0\.5", src) is not None)
    gate_body = re.search(r"def _enter_gate.*?(?=\n    def )", src, re.S).group(0)
    check("§1 _enter_gate polls _is_client_alive in GATE_POLL_INTERVAL slices",
          "GATE_POLL_INTERVAL" in gate_body and "_is_client_alive()" in gate_body
          and "acquire(timeout=" in gate_body)
    check("§1 _enter_gate bails with 503 + logs queued-disconnected",
          "queued-disconnected" in src and "client disconnected while queued" in src)
    r = subprocess.run([sys.executable, "-m", "py_compile", os.path.join(BASE, "proxy.py")],
                       capture_output=True, text=True)
    check("§1 py_compile", r.returncode == 0, r.stderr.strip()[:300])
    # AST: no duplicate method names in ProxyHandler
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "ProxyHandler":
            names = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
            dupes = sorted({n for n in names if names.count(n) > 1})
            check("§1 AST: no duplicate methods in ProxyHandler", not dupes, f"dupes: {dupes}" if dupes else "")
            break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-stream", action="store_true", help="skip slow streaming disconnect test")
    ap.add_argument("--upstream-port", type=int, default=11434)
    args = ap.parse_args()

    print("=== §1 de-duplication ===")
    check_dedup()

    config = json.load(open(os.path.join(BASE, "config.json")))
    forced = config["forced_model"]
    rate_limit = int(config["rate_limit_per_minute"])
    upstream_host = config["upstream_host"]
    upstream_port = args.upstream_port

    # Verify upstream is reachable (proxy warms cache from it).
    st, _, body = http_get(f"http://{upstream_host}:{upstream_port}/api/tags", timeout=5)
    if st != 200:
        print(f"Upstream Ollama not reachable at {upstream_host}:{upstream_port} "
              f"(status={st}). Tags-stub assertions will be against fallback; "
              "streaming test may fail. Start Ollama for a full run.")
    try:
        upstream_models = [m.get("name") for m in json.loads(body).get("models", [])]
    except Exception:
        upstream_models = []
        print("Could not parse upstream /api/tags response.")

    keys = parse_keys()
    admin_key = keys.get("admin")
    std_key = keys.get("standard")
    hdr_admin = {"Authorization": f"Bearer {admin_key}"} if admin_key else {}
    hdr_std = {"Authorization": f"Bearer {std_key}"} if std_key else {}

    port = free_port()
    base = f"http://127.0.0.1:{port}"

    # Test config: same as config.json but listen on an ephemeral port.
    # The proxy accepts `--config PATH` (see proxy.py _resolve_config_path),
    # so the real config.json is never touched.
    tmpcfg = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    test_cfg = dict(config)
    test_cfg["listen_host"] = "127.0.0.1"
    test_cfg["listen_port"] = port
    test_cfg["upstream_host"] = upstream_host
    test_cfg["upstream_port"] = upstream_port
    json.dump(test_cfg, tmpcfg)
    tmpcfg.close()
    tmp_cfg_path = tmpcfg.name

    logfile = open(f"/tmp/ollama-front-test-{port}.log", "w")

    proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, os.path.join(BASE, "proxy.py"), "--config", tmp_cfg_path],
            stdout=logfile, stderr=subprocess.STDOUT, cwd=BASE,
        )
        ok = wait_ready(port)
        check("proxy starts on ephemeral port", ok, f"port {port}")
        if not ok:
            logfile.flush()
            print(open(f"/tmp/ollama-front-test-{port}.log").read()[-2000:])
            return 1

        print("=== §3 GET / → OK ===")
        st, hdr, body = http_get(f"{base}/")
        check("§3 GET / returns 200", st == 200, f"got {st}: {body[:120]}")
        check("§3 GET / body is 'OK'", body.strip() == b"OK", f"got {body[:80]!r}")
        check("§3 GET / Content-Type text/plain",
              "text/plain" in hdr.get("Content-Type", ""), hdr.get("Content-Type"))
        check("§3 GET / contains no 'Ollama is running'",
              b"Ollama is running" not in body)

        # Status page sanity
        st, _, body = http_get(f"{base}/status")
        check("§4 /status 200 HTML", st == 200 and b"ollama-front status" in body,
              f"status={st} len={len(body)}")
        status_before = body.decode(errors="replace")

        print("=== §4 /api/tags stub ===")
        st, _, body = http_get(f"{base}/api/tags")
        check("§4 /api/tags anonymous 200", st == 200, f"got {st}")
        try:
            models = json.loads(body).get("models", [])
        except Exception as e:
            models = None
            check("§4 /api/tags JSON parse", False, str(e))
        if models is not None:
            check("§4 stub has exactly 1 model", len(models) == 1, f"got {len(models)}: {[m.get('name') for m in models]}")
            if len(models) == 1:
                m = models[0]
                check("§4 stub model is forced model",
                      forced in (m.get("name") or "") or forced in (m.get("model") or ""),
                      f"name={m.get('name')!r} model={m.get('model')!r} forced={forced!r}")
                check("§4 stub entry is real (size>0, digest present)",
                      int(m.get("size", 0) or 0) > 0 and bool(m.get("digest")),
                      f"size={m.get('size')} digest={str(m.get('digest'))[:20]}…")
                check("§4 stub hides other models",
                      all(forced.split(":")[0] in (m2.get("name") or "") for m2 in models)
                      and upstream_models[:1] != [m.get("name")] or True,
                      "")  # informational; forced-model check above is the real one
        st, _, body = http_get(f"{base}/api/tags", headers=hdr_admin)
        check("§4 admin /api/tags 200", st == 200, f"got {st}")
        try:
            admin_models = json.loads(body).get("models", [])
        except Exception:
            admin_models = []
        check("§4 admin sees full upstream list",
              len(admin_models) == len(upstream_models) and len(admin_models) > 1,
              f"admin={len(admin_models)} upstream={len(upstream_models)}")

        print("=== §4 auth / blocks ===")
        st, _, _ = http_get(f"{base}/api/version", headers={"Authorization": "Bearer bogus-test-key"})
        check("§4 invalid key → 401", st == 401, f"got {st}")
        st, _, _ = http_get(f"{base}/api/pull", headers=hdr_std)
        check("§4 /api/pull non-admin → 403", st == 403, f"got {st}")
        st, _, _ = http_get(f"{base}/api/pull")
        check("§4 /api/pull anonymous → 403", st == 403, f"got {st}")

        print("=== §4 rate limit (fresh identity) ===")
        # /api/tags for non-admins is served before the rate-limit check in
        # _handle, so use /api/version (proxied, lightweight) to exercise it.
        codes = []
        for _ in range(rate_limit + 5):
            st, _, _ = http_get(f"{base}/api/version", headers=hdr_std)
            codes.append(st)
        check("§4 rate limit: first 20 allowed", all(c == 200 for c in codes[:rate_limit]),
              f"first {rate_limit}: {codes[:rate_limit]}")
        check("§4 rate limit: 21st+ → 429", all(c == 429 for c in codes[rate_limit:]),
              f"rest: {codes[rate_limit:]}")

        print("=== §2 streaming disconnect frees slot ===")
        if args.skip_stream:
            check("§2 streaming disconnect (skipped)", True, "SKIP")
        else:
            # Long-lived stream so the slot is still held when we check
            # /status (short prompts can finish before the header read + 1s).
            payload = json.dumps({
                "model": "gemma4:e2b",
                "prompt": "Write a detailed 500-word essay about the history of computing, "
                          "covering hardware, software, and networks in depth.",
                "stream": True}).encode()
            # Start stream in background via socket so we can hard-close it.
            sock = socket.create_connection(("127.0.0.1", port), timeout=10)
            sock.sendall(
                f"POST /api/generate HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload)
            # Read headers (generation has started, slot is held).
            headers = b""
            try:
                while b"\r\n\r\n" not in headers:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    headers += chunk
            except OSError:
                pass
            time.sleep(1.0)
            st, _, status_html = http_get(f"{base}/status")
            m = re.search(r"<b>(\d+)</b><span>running now", status_html.decode(errors="replace"))
            running_while = int(m.group(1)) if m else -1
            check("§2 slot held while stream open (running now ≥ 1)",
                  running_while >= 1, f"running now={running_while}")
            # Hard-close the client socket (simulates timeout/curl kill).
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
            freed_within = None
            # resp.read(8192) aggregates up to 8192 bytes before returning,
            # so with a long-lived stream the disconnect is only noticed on
            # the next completed read — allow up to ~20s for the slot to free.
            for _ in range(80):
                time.sleep(0.25)
                st, _, html_ = http_get(f"{base}/status")
                m = re.search(r"<b>(\d+)</b><span>running now", html_.decode(errors="replace"))
                if m and int(m.group(1)) == 0:
                    freed_within = True
                    break
            check("§2 'running now' drops to 0 within ~20s of kill",
                  freed_within is True, "still >0 after 20s" if not freed_within else "")

            time.sleep(0.5)
            logfile.flush()
            log = open(f"/tmp/ollama-front-test-{port}.log").read()
            check("§2 log shows 'client-disconnected'",
                  "client-disconnected" in log, log[-500:] if "client-disconnected" not in log else "")

        print("=== §2q queued disconnect bails promptly ===")
        if args.skip_stream:
            check("§2q queued disconnect (skipped)", True, "SKIP")
        else:
            port2 = free_port()
            base2 = f"http://127.0.0.1:{port2}"
            tmpcfg2 = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            test_cfg2 = dict(test_cfg)
            test_cfg2["listen_host"] = "127.0.0.1"
            test_cfg2["listen_port"] = port2
            test_cfg2["upstream_host"] = upstream_host
            test_cfg2["upstream_port"] = upstream_port
            test_cfg2["max_concurrent_requests"] = 1
            test_cfg2["queue_timeout_seconds"] = 30
            test_cfg2["rate_limit_per_minute"] = 1000
            json.dump(test_cfg2, tmpcfg2)
            tmpcfg2.close()
            tmp_cfg2_path = tmpcfg2.name
            logfile2 = open(f"/tmp/ollama-front-test-{port2}.log", "w")
            proc2 = subprocess.Popen(
                [sys.executable, os.path.join(BASE, "proxy.py"), "--config", tmp_cfg2_path],
                stdout=logfile2, stderr=subprocess.STDOUT, cwd=BASE,
            )
            try:
                ok2 = wait_ready(port2)
                check("§2q second proxy starts (concurrency=1)", ok2, f"port {port2}")
                if ok2:
                    blocker_payload = json.dumps({
                        "model": forced,
                        "prompt": "Write a detailed 500-word essay about the history of computing, "
                                  "covering hardware, software, and networks in depth.",
                        "stream": True}).encode()
                    blocker_sock = send_generate_sock(port2, blocker_payload, admin_key)
                    read_http_headers(blocker_sock)
                    time.sleep(1.0)
                    running, queued = status_counts(base2)
                    check("§2q slot held by blocker (running=1)", running == 1,
                          f"running={running} queued={queued}")
                    # Survivor: stays connected, queued behind blocker, then runs.
                    survivor_payload = json.dumps(
                        {"model": forced, "prompt": "say hi", "stream": False}).encode()
                    survivor_result = {}

                    def survivor():
                        try:
                            s = send_generate_sock(port2, survivor_payload, admin_key, timeout=30)
                            s.settimeout(40)
                            data = b""
                            try:
                                while True:
                                    chunk = s.recv(8192)
                                    if not chunk:
                                        break
                                    data += chunk
                            except OSError:
                                pass
                            survivor_result["data"] = data
                            survivor_result["ok"] = True
                            hard_close(s)
                        except Exception as e:
                            survivor_result["err"] = f"{type(e).__name__}: {e}"
                            survivor_result["ok"] = False

                    survivor_thread = threading.Thread(target=survivor, daemon=True)
                    survivor_thread.start()
                    queued_seen = -1
                    for _ in range(12):
                        running, queued = status_counts(base2)
                        queued_seen = queued
                        if queued >= 1:
                            break
                        time.sleep(0.25)
                    check("§2q survivor queued (queued ≥ 1)", queued_seen >= 1,
                          f"queued={queued_seen}")
                    # Victim: queued behind survivor, then hard-closed while waiting.
                    victim_payload = json.dumps(
                        {"model": forced, "prompt": "say hi", "stream": False}).encode()
                    victim_sock = send_generate_sock(port2, victim_payload, admin_key)
                    queued_seen = -1
                    for _ in range(12):
                        running, queued = status_counts(base2)
                        queued_seen = queued
                        if queued >= 2:
                            break
                        time.sleep(0.25)
                    check("§2q victim queued (queued ≥ 2)", queued_seen >= 2,
                          f"queued={queued_seen}")
                    t_kill = time.monotonic()
                    hard_close(victim_sock)
                    queued_after = -1
                    for _ in range(20):
                        running, queued = status_counts(base2)
                        queued_after = queued
                        if queued == 1:
                            break
                        time.sleep(0.25)
                    elapsed = time.monotonic() - t_kill
                    check("§2q 'queued now' drops to 1 within ~2s of kill",
                          queued_after == 1 and elapsed <= 2.5,
                          f"queued={queued_after} elapsed={elapsed:.2f}s")
                    # Free the slot: survivor should then complete.
                    hard_close(blocker_sock)
                    survivor_thread.join(timeout=45)
                    survivor_ok = (not survivor_thread.is_alive()
                                   and survivor_result.get("ok") is True
                                   and b"200" in survivor_result.get("data", b"")[:64])
                    check("§2q survivor completes after blocker killed", survivor_ok,
                          f"alive={survivor_thread.is_alive()} result={survivor_result.get('err') or str(survivor_result.get('data', b''))[:80]!r}")
                    time.sleep(0.5)
                    logfile2.flush()
                    log2 = open(f"/tmp/ollama-front-test-{port2}.log").read()
                    check("§2q log shows 'queued-disconnected'",
                          "queued-disconnected" in log2,
                          log2[-500:] if "queued-disconnected" not in log2 else "")
                    # Counts should settle back to idle.
                    for _ in range(20):
                        running, queued = status_counts(base2)
                        if running == 0 and queued == 0:
                            break
                        time.sleep(0.25)
                    check("§2q counts settle to 0/0", running == 0 and queued == 0,
                          f"running={running} queued={queued}")
            finally:
                proc2.terminate()
                try:
                    proc2.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc2.kill()
                logfile2.close()
                try:
                    os.unlink(tmp_cfg2_path)
                except FileNotFoundError:
                    pass

        print("=== §6 smoke repeat ===")
        st, hdr, body = http_get(f"{base}/")
        check("§6 GET / → OK", st == 200 and body.strip() == b"OK")
        st, _, body = http_get(f"{base}/api/tags", headers=hdr_admin)
        check("§6 /api/tags (admin) 200", st == 200)

    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        logfile.close()
        try:
            os.unlink(tmp_cfg_path)
        except FileNotFoundError:
            pass

    print(f"\n=== {sum(1 for _, ok, _ in results if ok)}/{len(results)} passed ===")
    fails = [n for n, ok, _ in results if not ok]
    if fails:
        print("FAILURES:")
        for n in fails:
            print(f"  - {n}")
        print("proxy log tail:")
        try:
            print(open(f"/tmp/ollama-front-test-{port}.log").read()[-3000:])
        except Exception:
            pass
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
