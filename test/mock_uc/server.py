"""Strict replay server for the Unity Catalog REST API (and a recorder that builds its fixture).

Replay (default): answers only requests recorded in ``test/fixtures/uc_recording.json``,
an exact capture of a real open-source Unity Catalog v0.7.0 server (see
``test/fixtures/uc_server.sh``). Requests are matched on method, path, the full sorted
query string and the JSON body; anything else gets HTTP 418 with ``VGI_MOCK_UNRECORDED``
(not a 404, so a ``missing_ok`` listing cannot swallow it) and is listed at
``GET /__unrecorded``. ``GET /__requests`` lists every request received.

Environment:
    MOCK_TOKEN       bearer token every request must carry (default ``test-token``);
                     a missing / wrong token gets Unity Catalog's own 401.
    MOCK_FLAVOR      ``databricks`` overlays ``test/fixtures/databricks_extras.json``
                     (Databricks-only endpoints written from the documented shapes).
    MOCK_FLAKY=1     the first sighting of every third distinct request gets
                     ``429 Retry-After: 0``.
    MOCK_RECORD_UPSTREAM / MOCK_RECORD_TOKEN / MOCK_RECORD_OUT
                     record mode: proxy every request to the upstream server with the
                     upstream token and append the exchange to MOCK_RECORD_OUT (requests
                     answered by the Databricks extras are served, not recorded).

Prints ``PORT:<n>`` on stdout once listening.
"""

from __future__ import annotations

import base64
import fcntl
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
TOKEN = os.environ.get("MOCK_TOKEN", "test-token")
FLAKY = os.environ.get("MOCK_FLAKY") == "1"
UPSTREAM = os.environ.get("MOCK_RECORD_UPSTREAM", "").rstrip("/")
UPSTREAM_TOKEN = os.environ.get("MOCK_RECORD_TOKEN", "")
RECORD_OUT = os.environ.get("MOCK_RECORD_OUT", "")

LOCK = threading.Lock()
RECEIVED: list[dict[str, Any]] = []
UNRECORDED: list[dict[str, Any]] = []
SEEN: dict[str, int] = {}


def canonical_key(method: str, path: str, query: str, body: bytes) -> str:
    """Stable key: method, path, sorted query pairs, canonical JSON body."""
    pairs = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    canon_body = ""
    if body:
        try:
            canon_body = json.dumps(json.loads(body), sort_keys=True, separators=(",", ":"))
        except ValueError:
            canon_body = body.decode(errors="replace")
    return json.dumps([method, path, pairs, canon_body], separators=(",", ":"))


def load(path: Path) -> dict[str, dict[str, Any]]:
    """Load a recording: a list of exchanges -> {key: exchange}."""
    if not path.exists():
        return {}
    out = {}
    for ex in json.loads(path.read_text()):
        out[ex["key"]] = ex
    return out


EXTRAS = load(FIXTURES / "databricks_extras.json") if os.environ.get("MOCK_FLAVOR") == "databricks" else {}
EXCHANGES = {**load(FIXTURES / "uc_recording.json"), **EXTRAS}


class Handler(BaseHTTPRequestHandler):
    """Replays (or records) Unity Catalog API exchanges."""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("mock_uc: " + (fmt % args) + "\n")

    def _send(
        self, status: int, body: bytes, ctype: str = "application/json", headers: dict[str, str] | None = None
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: Any, headers: dict[str, str] | None = None) -> None:
        self._send(status, json.dumps(obj).encode(), headers=headers)

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        """Only the Databricks OAuth M2M token endpoint: client ``test-client`` / ``test-secret``."""
        length = int(self.headers.get("Content-Length") or 0)
        form = urllib.parse.parse_qs(self.rfile.read(length).decode())
        expected = "Basic " + base64.b64encode(b"test-client:test-secret").decode()
        if self.path != "/oidc/v1/token":
            self._json(405, {"error_code": "BAD_REQUEST", "message": "the worker is read-only"})
        elif self.headers.get("Authorization") != expected or form.get("grant_type") != ["client_credentials"]:
            self._json(401, {"error": "invalid_client", "error_description": "bad client credentials"})
        else:
            self._json(200, {"access_token": TOKEN, "token_type": "Bearer", "expires_in": 3600})

    def _handle(self, method: str) -> None:
        split = urllib.parse.urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if split.path == "/__requests":
            with LOCK:
                self._json(200, RECEIVED)
            return
        if split.path == "/__unrecorded":
            with LOCK:
                self._json(200, UNRECORDED)
            return
        key = canonical_key(method, split.path, split.query, body)
        entry = {"method": method, "path": split.path, "query": split.query, "body": body.decode(errors="replace")}
        with LOCK:
            RECEIVED.append(entry)
            SEEN[key] = SEEN.get(key, 0) + 1
            nth_distinct = list(SEEN).index(key) + 1
            first = SEEN[key] == 1
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._json(401, {"error_code": "UNAUTHENTICATED", "message": "No authorization found."})
            return
        if UPSTREAM and key not in EXTRAS:
            self._record(method, split, body, key)
            return
        if FLAKY and first and nth_distinct % 3 == 0:
            self._json(429, {"error_code": "REQUEST_LIMIT_EXCEEDED", "message": "slow down"}, {"Retry-After": "0"})
            return
        ex = EXCHANGES.get(key)
        if ex is None:
            with LOCK:
                UNRECORDED.append(entry)
            sys.stderr.write(f"mock_uc: UNRECORDED {key}\n")
            self._json(418, {"error_code": "VGI_MOCK_UNRECORDED", "message": f"no recorded exchange for {key}"})
            return
        self._send(int(ex["status"]), ex["response"].encode(), ex.get("content_type") or "application/json")

    def _record(self, method: str, split: urllib.parse.SplitResult, body: bytes, key: str) -> None:
        url = UPSTREAM + split.path + (f"?{split.query}" if split.query else "")
        headers = {"Accept": "application/json"}
        if UPSTREAM_TOKEN:
            headers["Authorization"] = f"Bearer {UPSTREAM_TOKEN}"
        if body:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body or None, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                status, raw, ctype = resp.status, resp.read(), resp.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as err:
            status, raw, ctype = err.code, err.read(), err.headers.get("Content-Type", "application/json")
        text = raw.decode(errors="replace")
        if UPSTREAM_TOKEN and UPSTREAM_TOKEN in text:
            raise RuntimeError("upstream echoed the token; refusing to record it")
        exchange = {"key": key, "status": status, "content_type": ctype.split(";")[0], "response": text}
        # Both replay instances may record into the same file: merge under an exclusive file lock.
        with LOCK, open(RECORD_OUT + ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            merged = load(Path(RECORD_OUT))
            merged[key] = exchange
            ordered = [merged[k] for k in sorted(merged)]
            Path(RECORD_OUT).write_text(json.dumps(ordered, indent=1, sort_keys=True) + "\n")
        self._send(status, raw, ctype)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("MOCK_PORT", "0"))), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
