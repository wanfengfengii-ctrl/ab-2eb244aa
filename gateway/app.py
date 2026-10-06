"""Ocean observatory snapshot gateway.

``POST /api/snapshots`` produces traceable register snapshots from a buoy
controller that may change configuration mid-read.  A snapshot is persisted
only when every page came from one single controller revision.

* Revision change, failed precondition, timeout, missing/short page,
  duplicate addresses or out-of-range values discard the whole round and
  restart from page one.  Three unstable rounds -> HTTP 409 conflict with no
  queryable evidence.
* Repeating a ``snapshotId`` with the same parameters (concurrently or after
  a restart) forms at most one complete snapshot and otherwise replays it.
* Repeating an id with different parameters -> HTTP 409 parameter conflict.
"""

import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from common.protocol import (
    GATEWAY_UPSTREAM_TIMEOUT,
    PAGE_SIZE,
    PAGE_TIMEOUT,
    validate_params,
)
from gateway.reader import SnapshotUnstable, read_stable_snapshot
from gateway.storage import (
    InvalidSnapshotId,
    ParameterConflict,
    STATE_COMPLETE,
    STATE_CONFLICT,
    STATE_PENDING,
    SnapshotStore,
)

SERVICE_PORT = int(os.environ.get("GATEWAY_PORT", "8080"))
STORE_DIR = os.environ.get("SNAPSHOT_STORE", "/data/snapshots")
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://simulator:8080").rstrip("/")

DEVICE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}\Z")

store = SnapshotStore(STORE_DIR)


class KeyLocks:
    """Per-snapshot-id locks so concurrent identical requests coalesce."""

    def __init__(self):
        self._guard = threading.Lock()
        self._locks = {}
        self._active = {}

    def take(self, key):
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
                self._active[key] = 0
            self._active[key] += 1
            return lock

    def give(self, key):
        with self._guard:
            self._active[key] -= 1
            if self._active[key] <= 0:
                self._locks.pop(key, None)
                self._active.pop(key, None)


key_locks = KeyLocks()


def public_snapshot(record, replayed):
    return {
        "snapshotId": record["snapshotId"],
        "deviceId": record["deviceId"],
        "startAddress": record["startAddress"],
        "registerCount": record["registerCount"],
        "state": STATE_COMPLETE,
        "revision": record["revision"],
        "values": record["values"],
        "sha256": record["sha256"],
        "attempts": record["attempts"],
        "replayed": replayed,
    }


class GatewayHandler(BaseHTTPRequestHandler):
    server_version = "SnapshotGateway/1.0"

    def log_message(self, fmt, *args):
        print(f"[gateway] {self.address_string()} {fmt % args}", flush=True)

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self._send_json(200, {"status": "ok", "store": STORE_DIR})
            return
        match = re.fullmatch(r"/api/snapshots/([A-Za-z0-9_-]{1,128})", path)
        if match:
            self._replay(match.group(1))
            return
        self._send_json(404, {"error": "not_found"})

    def _replay(self, snapshot_id):
        if not store.exists(snapshot_id):
            self._send_json(404, {"error": "snapshot_not_found"})
            return
        record = store.load(snapshot_id)
        if record["state"] == STATE_COMPLETE:
            self._send_json(200, public_snapshot(record, replayed=True))
            return
        if record["state"] == STATE_CONFLICT:
            # Conflicts leave no queryable evidence.
            self._send_json(404, {"error": "snapshot_not_found"})
            return
        self._send_json(409, {"error": "snapshot_pending", "snapshotId": snapshot_id})

    def do_POST(self):
        if urlparse(self.path).path != "/api/snapshots":
            self._send_json(404, {"error": "not_found"})
            return
        body = self._read_body()
        if body is None:
            return
        parsed, error = self._parse_request(body)
        if error is not None:
            status, payload = error
            self._send_json(status, payload)
            return
        self._handle_snapshot_request(*parsed)

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "bad_request", "message": "bad length"})
            return None
        if length <= 0:
            self._send_json(400, {"error": "bad_request", "message": "empty body"})
            return None
        if length > 65536:
            self._send_json(413, {"error": "payload_too_large"})
            return None
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(400, {"error": "invalid_json"})
            return None
        if not isinstance(body, dict):
            self._send_json(400, {"error": "body_must_be_object"})
            return None
        return body

    @staticmethod
    def _parse_request(body):
        snapshot_id = body.get("snapshotId")
        device_id = body.get("deviceId")
        start_address = body.get("startAddress")
        count = body.get("registerCount")

        if not isinstance(snapshot_id, str):
            return None, (400, {"error": "invalid_snapshot_id"})
        try:
            SnapshotStore.validate_id(snapshot_id)
        except InvalidSnapshotId as exc:
            return None, (400, {"error": "invalid_snapshot_id", "message": str(exc)})

        if not isinstance(device_id, str) or not DEVICE_ID_PATTERN.match(device_id):
            return None, (400, {"error": "invalid_device_id"})

        if not isinstance(start_address, int) or isinstance(start_address, bool):
            return None, (400, {"error": "invalid_start_address"})
        if not isinstance(count, int) or isinstance(count, bool):
            return None, (400, {"error": "invalid_register_count"})

        status, message = validate_params(start_address, count)
        if status is not None:
            return None, (status, {"error": "invalid_parameters", "message": message})

        return (snapshot_id, device_id, start_address, count), None

    def _handle_snapshot_request(self, snapshot_id, device_id, start_address, count):
        lock = key_locks.take(snapshot_id)
        try:
            with lock:
                try:
                    record, created = store.create_or_get(
                        snapshot_id, device_id, start_address, count
                    )
                except ParameterConflict:
                    self._send_json(
                        409,
                        {
                            "error": "parameter_conflict",
                            "snapshotId": snapshot_id,
                            "message": (
                                "snapshotId already exists with different "
                                "parameters"
                            ),
                        },
                    )
                    return

                if record["state"] == STATE_COMPLETE:
                    # Concurrent duplicate or retried after completion:
                    # replay the one existing snapshot.
                    self._send_json(200, public_snapshot(record, replayed=True))
                    return
                if record["state"] == STATE_CONFLICT:
                    self._send_json(
                        409,
                        {
                            "error": "snapshot_conflict",
                            "snapshotId": snapshot_id,
                            "reason": record["reason"],
                            "attempts": record["attempts"],
                            "attemptReasons": record["attemptReasons"],
                        },
                    )
                    return

                # Pending (newly created, or adopted after a crash/restart).
                pages = (count + PAGE_SIZE - 1) // PAGE_SIZE
                # Per-attempt deadline: one timeout budget per page plus slack.
                attempt_deadline = max(
                    5.0, pages * PAGE_TIMEOUT + 2.0, GATEWAY_UPSTREAM_TIMEOUT
                )
                try:
                    revision, values, digest, rounds = read_stable_snapshot(
                        UPSTREAM_URL,
                        start_address,
                        count,
                        attempt_deadline,
                    )
                except SnapshotUnstable as exc:
                    record["attempts"] = len(exc.reasons)
                    record["attemptReasons"] = list(exc.reasons)
                    failed = store.commit_conflict(
                        record, "revision unstable or transport failing"
                    )
                    self._send_json(
                        409,
                        {
                            "error": "snapshot_conflict",
                            "snapshotId": snapshot_id,
                            "state": STATE_CONFLICT,
                            "reason": failed["reason"],
                            "detail": str(exc),
                            "attempts": failed["attempts"],
                            "attemptReasons": failed["attemptReasons"],
                        },
                    )
                    return

                record["attempts"] = rounds
                complete = store.commit_complete(record, revision, values, digest)
                self._send_json(
                    201 if created else 200,
                    public_snapshot(complete, replayed=False),
                )
        finally:
            key_locks.give(snapshot_id)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", SERVICE_PORT), GatewayHandler)
    print(
        f"[gateway] listening on 0.0.0.0:{SERVICE_PORT} "
        f"upstream={UPSTREAM_URL} store={STORE_DIR}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
