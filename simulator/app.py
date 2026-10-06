"""Controllable buoy controller simulator.

Serves paged 16-bit holding registers with a configuration ``revision``.

Protocol
--------
``GET /registers?start=<addr>&count=<n>&revision=<r>``

* ``count`` is at most 64.
* The first page of a read is fetched *without* ``revision``; the response
  establishes the revision.
* Later pages carry the established revision.  If the live revision differs,
  the simulator answers ``409 revision_mismatch`` instead of serving data.

Fault injection (for verification) is configured through the admin API::

    POST /admin/reset {"revision": 1}
    POST /admin/mode  {"mode": "flap", "config": {...}}

Modes: ``normal``, ``flap`` (bump revision after every served page),
``bump-after`` (bump once after ``config.afterReads`` served reads),
``short-page``, ``duplicate-addresses``, ``out-of-range``, ``page-error``,
``timeout`` and ``drop``.
"""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from common.protocol import MAX_ADDRESS, PAGE_SIZE

SERVICE_PORT = int(os.environ.get("SIMULATOR_PORT", "8080"))

VALID_MODES = {
    "normal",
    "flap",
    "bump-after",
    "short-page",
    "duplicate-addresses",
    "out-of-range",
    "page-error",
    "timeout",
    "drop",
}


def register_value(revision, address):
    """Deterministic 16-bit value for a (revision, address).

    Consecutive revisions differ at virtually every address, so mixing pages
    from two revisions is detectable by checksum.
    """
    return (revision * 7919 + address * 31 + 17) & 0xFFFF


class ControllerState:
    def __init__(self):
        self._lock = threading.Lock()
        self.reset(1)

    def reset(self, revision):
        with self._lock:
            self.revision = int(revision)
            self.mode = "normal"
            self.config = {}
            self.reads = 0
            self.served = 0
            self.last_page_start = None

    def set_mode(self, mode, config=None):
        with self._lock:
            self.mode = mode
            self.config = dict(config or {})
            self.reads = 0

    def snapshot(self):
        with self._lock:
            return {
                "revision": self.revision,
                "mode": self.mode,
                "config": dict(self.config),
                "reads": self.reads,
                "served": self.served,
            }

    def evaluate(self, start, count, condition):
        """Resolve one page request.

        Returns ``(status, payload)`` where status 0 means "serve payload",
        409/400/500 mean respond with that error, and the special strings
        ``"drop"`` and ``"timeout"`` request transport-level faults.
        """
        with self._lock:
            self.reads += 1
            current = self.revision
            mode = self.mode
            config = dict(self.config)

            if condition is not None and condition != current:
                return 409, {
                    "error": "revision_mismatch",
                    "expected": condition,
                    "current": current,
                }

            delay = config.get("delaySeconds")
            if mode == "timeout":
                return "timeout", {"delaySeconds": float(delay or 5.0)}

            values = [
                register_value(current, start + i) for i in range(count)
            ]

            if mode == "page-error":
                return 500, {"error": "simulated_controller_fault"}

            if mode == "out-of-range":
                values[0] = 0x10000

            page = {
                "startAddress": start,
                "revision": current,
                "values": values,
            }

            if mode == "short-page":
                page["values"] = values[:-1]
            elif mode == "duplicate-addresses":
                # Echo the previous page's start address, so the union of
                # covered addresses contains a duplicate / a hole.
                prev = self.last_page_start
                page["startAddress"] = prev if prev is not None else start

            self.last_page_start = page["startAddress"]

            # Revision-changing faults apply *after* the page was served, so
            # the next conditional page is guaranteed to see a new revision.
            self.served += 1
            if mode == "flap":
                self.revision += 1
            elif mode == "bump-after":
                if self.served >= int(config.get("afterReads", 1)):
                    self.revision += 1
                    self.mode = "normal"

            if mode == "drop":
                return "drop", None

            return 0, page


STATE = ControllerState()


class Handler(BaseHTTPRequestHandler):
    server_version = "BuoySim/1.0"

    def log_message(self, fmt, *args):  # quiet structured logging
        print(f"[simulator] {self.address_string()} {fmt % args}", flush=True)

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Client gave up waiting (e.g. its page timeout fired); this is
            # expected during fault-injection cases.
            self.close_connection = True

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            state = STATE.snapshot()
            self._send_json(
                200,
                {
                    "status": "ok",
                    "revision": state["revision"],
                    "mode": state["mode"],
                },
            )
            return
        if parsed.path == "/admin/state":
            self._send_json(200, STATE.snapshot())
            return
        if parsed.path != "/registers":
            self._send_json(404, {"error": "not_found"})
            return

        query = parse_qs(parsed.query)
        try:
            start = int(query["start"][0])
            count = int(query["count"][0])
        except (KeyError, ValueError, IndexError):
            self._send_json(
                400, {"error": "bad_request", "message": "start/count required"}
            )
            return
        condition_raw = query.get("revision", [None])[0]
        condition = None
        if condition_raw is not None:
            try:
                condition = int(condition_raw)
            except ValueError:
                self._send_json(400, {"error": "bad_request"})
                return

        if start < 1 or count < 1 or count > PAGE_SIZE:
            self._send_json(
                400,
                {
                    "error": "bad_request",
                    "message": f"1 <= count <= {PAGE_SIZE}",
                },
            )
            return
        if start + count - 1 > MAX_ADDRESS:
            self._send_json(400, {"error": "address_out_of_range"})
            return

        status, payload = STATE.evaluate(start, count, condition)
        if status == "timeout":
            time.sleep(payload["delaySeconds"])
            self._send_json(504, {"error": "simulated_timeout"})
            return
        if status == "drop":
            # Tear the connection down without answering, like a dying radio.
            self.close_connection = True
            return
        self._send_json(status if status else 200, payload)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/admin/reset":
            body = self._read_body()
            if body is None:
                return
            STATE.reset(int(body.get("revision", 1)))
            self._send_json(200, {"ok": True, **STATE.snapshot()})
            return
        if parsed.path == "/admin/mode":
            body = self._read_body()
            if body is None:
                return
            mode = body.get("mode")
            if mode not in VALID_MODES:
                self._send_json(
                    400, {"error": "bad_mode", "valid": sorted(VALID_MODES)}
                )
                return
            STATE.set_mode(mode, body.get("config"))
            self._send_json(200, {"ok": True, **STATE.snapshot()})
            return
        self._send_json(404, {"error": "not_found"})

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "bad_request"})
            return None
        if length <= 0:
            return {}
        if length > 65536:
            self._send_json(413, {"error": "payload_too_large"})
            return None
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(400, {"error": "invalid_json"})
            return None
        if not isinstance(body, dict):
            self._send_json(400, {"error": "body_must_be_object"})
            return None
        return body


def main():
    server = ThreadingHTTPServer(("0.0.0.0", SERVICE_PORT), Handler)
    print(f"[simulator] listening on 0.0.0.0:{SERVICE_PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
