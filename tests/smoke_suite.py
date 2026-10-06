"""End-to-end smoke checks shared by unit-style tests and the verify service.

Targets a live gateway + controllable simulator over HTTP.  Expected register
values are obtained by reading the simulator directly page by page, so the
suite never hard-codes the controller's value formula.
"""

import hashlib
import json
import re
import threading
import time
import urllib.error
import urllib.request

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class HttpError(Exception):
    def __init__(self, status, payload):
        super().__init__(f"HTTP {status}: {payload}")
        self.status = status
        self.payload = payload


def request(url, method="GET", body=None, timeout=10):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        raise HttpError(exc.code, payload)


def wait_healthy(url, attempts=60, delay=0.5):
    last = None
    for _ in range(attempts):
        try:
            status, payload = request(url, timeout=2)
            if status == 200 and payload.get("status") == "ok":
                return True
        except Exception as exc:  # noqa: BLE001 - health polling
            last = exc
        time.sleep(delay)
    raise RuntimeError(f"service never became healthy at {url}: {last}")


def admin_reset(simulator, revision=1):
    request(f"{simulator}/admin/reset", "POST", {"revision": revision})


def admin_mode(simulator, mode, config=None):
    request(f"{simulator}/admin/mode", "POST", {"mode": mode, "config": config})


def direct_read(simulator, start, count, revision=None):
    """Read the simulator directly, one <=64-register page at a time."""
    values = []
    offset = 0
    rev = revision
    while offset < count:
        n = min(64, count - offset)
        url = f"{simulator}/registers?start={start + offset}&count={n}"
        if rev is not None:
            url += f"&revision={rev}"
        _, page = request(url)
        values.extend(page["values"])
        rev = page["revision"]
        offset += n
    return rev, values


def expected_digest(values):
    packed = b"".join(v.to_bytes(2, "big") for v in values)
    return hashlib.sha256(packed).hexdigest()


class CheckRunner:
    def __init__(self, gateway, simulator, log=print):
        self.gateway = gateway.rstrip("/")
        self.simulator = simulator.rstrip("/")
        self.log = log
        self.failures = []
        self.checked = 0

    def check(self, name, condition, detail=""):
        self.checked += 1
        if condition:
            self.log(f"PASS  {name}")
        else:
            self.failures.append(name)
            self.log(f"FAIL  {name} {detail}")

    def section(self, title):
        self.log(f"--- {title}")

    def snapshot(self, snapshot_id, start, count, device="buoy-001", timeout=20):
        return request(
            f"{self.gateway}/api/snapshots",
            "POST",
            {
                "snapshotId": snapshot_id,
                "deviceId": device,
                "startAddress": start,
                "registerCount": count,
            },
            timeout=timeout,
        )

    def run(self, restart_gateway=None):
        sections = [
            ("stable read", self._stable_read),
            ("revision-bump retry", self._revision_bump_retry),
            ("idempotent replay/concurrency",
             lambda: self._idempotent_replay_and_concurrency(restart_gateway)),
            ("parameter conflict", self._parameter_conflict),
            ("persistent instability", self._persistent_instability),
            ("transport anomalies", self._transport_anomalies),
            ("invalid requests", self._invalid_requests),
        ]
        for name, section in sections:
            try:
                section()
            except Exception as exc:  # noqa: BLE001 - turn into reported failure
                self.checked += 1
                self.failures.append(f"section:{name}")
                self.log(f"FAIL  section '{name}' raised: {exc!r}")
        return not self.failures

    def _stable_read(self):
        self.section("stable single-revision read")
        admin_reset(self.simulator, 1)
        status, body = self.snapshot("stable-001", 1000, 130)
        self.check("stable: 201 created", status == 201, body)
        revision, expected = direct_read(self.simulator, 1000, 130)
        self.check(
            "stable: revision matches controller",
            body.get("revision") == revision == 1,
            body,
        )
        self.check(
            "stable: ordered 16-bit values",
            body.get("values") == expected
            and all(isinstance(v, int) and 0 <= v <= 0xFFFF for v in expected),
            "values mismatch",
        )
        self.check(
            "stable: big-endian SHA-256",
            body.get("sha256") == expected_digest(expected)
            and bool(SHA256_RE.match(body.get("sha256", ""))),
            body.get("sha256"),
        )
        self.check("stable: attempts == 1", body.get("attempts") == 1, body)

    def _revision_bump_retry(self):
        self.section("revision change mid-read -> discard and retry")
        admin_reset(self.simulator, 1)
        admin_mode(self.simulator, "bump-after", {"afterReads": 1})
        status, body = self.snapshot("bump-001", 4000, 100)
        self.check("bump: snapshot eventually succeeds", status == 201, body)
        self.check(
            "bump: round 1 discarded, round 2 used",
            body.get("attempts") == 2,
            body.get("attempts"),
        )
        revision, expected = direct_read(self.simulator, 4000, 100)
        self.check(
            "bump: evidence all from revision 2",
            body.get("revision") == revision == 2,
            body.get("revision"),
        )
        self.check(
            "bump: values/checksum belong to revision 2 only",
            body.get("values") == expected
            and body.get("sha256") == expected_digest(expected),
            "mixed revisions",
        )

    def _idempotent_replay_and_concurrency(self, restart_gateway):
        self.section("idempotent replay, concurrency, restart")
        admin_reset(self.simulator, 3)

        results = {}

        def post(tag):
            try:
                results[tag] = self.snapshot("once-001", 7000, 512, timeout=30)
            except HttpError as exc:
                results[tag] = exc

        t1 = threading.Thread(target=post, args=("a",))
        t2 = threading.Thread(target=post, args=("b",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        if isinstance(results["a"], HttpError) or isinstance(results["b"], HttpError):
            self.check("concurrent: one 201 and one 200 replay", False, results)
        else:
            statuses = sorted([results["a"][0], results["b"][0]])
            self.check(
                "concurrent: one 201 and one 200 replay",
                statuses == [200, 201],
                statuses,
            )
            digests = {results[k][1]["sha256"] for k in results}
            self.check("concurrent: identical evidence", len(digests) == 1, digests)

        status, body = self.snapshot("once-001", 7000, 512)
        self.check(
            "retry same params: replays (200, replayed=true)",
            status == 200 and body.get("replayed") is True,
            (status, body),
        )

        status, got = request(f"{self.gateway}/api/snapshots/once-001")
        self.check(
            "GET replays identical evidence",
            status == 200
            and got.get("replayed") is True
            and got["sha256"] == body["sha256"]
            and got["values"] == body["values"],
            (status, got),
        )

        if restart_gateway is not None:
            self.section("restart durability")
            restart_gateway()
            wait_healthy(f"{self.gateway}/health")
            status, got = request(f"{self.gateway}/api/snapshots/once-001")
            self.check(
                "after restart: same snapshot replays from durable store",
                status == 200 and got["sha256"] == body["sha256"],
                (status, got),
            )

    def _parameter_conflict(self):
        self.section("same snapshotId, different parameters")
        status, body = self.snapshot("conflict-params", 1, 10)
        self.check("params: initial created", status == 201, body)
        try:
            self.snapshot("conflict-params", 1, 11)
            self.check("params: changed count -> 409", False, "no error")
        except HttpError as exc:
            self.check(
                "params: changed count -> 409",
                exc.status == 409
                and exc.payload.get("error") == "parameter_conflict",
                (exc.status, exc.payload),
            )
        try:
            self.snapshot("conflict-params", 2, 10, device="other")
            self.check("params: changed device/start -> 409", False, "no error")
        except HttpError as exc:
            self.check(
                "params: changed device/start -> 409",
                exc.status == 409,
                (exc.status, exc.payload),
            )

    def _persistent_instability(self):
        self.section("continuous revision flapping -> stable failure, no evidence")
        admin_reset(self.simulator, 1)
        admin_mode(self.simulator, "flap")
        try:
            self.snapshot("flap-001", 1, 100)
            self.check("flap: 409 conflict", False, "snapshot succeeded")
        except HttpError as exc:
            reasons = exc.payload.get("attemptReasons", [])
            self.check(
                "flap: 409 snapshot_conflict after 3 rounds",
                exc.status == 409
                and exc.payload.get("error") == "snapshot_conflict"
                and exc.payload.get("attempts") == 3
                and len(reasons) == 3
                and all(r.startswith("revision_changed") for r in reasons),
                (exc.status, exc.payload),
            )
        try:
            request(f"{self.gateway}/api/snapshots/flap-001")
            self.check("flap: nothing queryable", False, "found evidence")
        except HttpError as exc:
            self.check(
                "flap: conflict leaves no queryable result (404)",
                exc.status == 404,
                exc.status,
            )

    def _transport_anomalies(self):
        self.section("timeout / drop / malformed pages -> no persisted evidence")
        cases = [
            ("timeout-001", "timeout", {"delaySeconds": 5}, "transport_error"),
            ("drop-001", "drop", None, "transport_error"),
            ("short-001", "short-page", None, "short_page"),
            ("dup-001", "duplicate-addresses", None, "address_mismatch"),
            ("range-001", "out-of-range", None, "value_out_of_range"),
            ("perr-001", "page-error", None, "page_error"),
        ]
        for snapshot_id, mode, config, prefix in cases:
            admin_reset(self.simulator, 1)
            admin_mode(self.simulator, mode, config)
            try:
                self.snapshot(snapshot_id, 1, 100, timeout=30)
                self.check(f"{mode}: 409", False, "snapshot succeeded")
                continue
            except HttpError as exc:
                reasons = exc.payload.get("attemptReasons", [])
                ok = (
                    exc.status == 409
                    and exc.payload.get("attempts") == 3
                    and len(reasons) == 3
                    and all(r.startswith(prefix) for r in reasons)
                )
                self.check(f"{mode}: 409 with stable reason", ok, (exc.status, exc.payload))
            try:
                request(f"{self.gateway}/api/snapshots/{snapshot_id}")
                self.check(f"{mode}: not persisted", False, "found")
            except HttpError as exc:
                self.check(
                    f"{mode}: no queryable evidence", exc.status == 404, exc.status
                )

    def _invalid_requests(self):
        self.section("input validation never persists")
        admin_reset(self.simulator, 1)
        admin_mode(self.simulator, "normal")
        bad_bodies = [
            {"snapshotId": "bad-1", "deviceId": "b", "startAddress": 1},
            {"snapshotId": "bad-2", "deviceId": "b", "startAddress": 1,
             "registerCount": 0},
            {"snapshotId": "bad-3", "deviceId": "b", "startAddress": 1,
             "registerCount": 513},
            {"snapshotId": "bad-4", "deviceId": "b", "startAddress": 0,
             "registerCount": 1},
            {"snapshotId": "bad-5", "deviceId": "b", "startAddress": 65536,
             "registerCount": 2},
            {"snapshotId": "../x", "deviceId": "b", "startAddress": 1,
             "registerCount": 1},
            {"snapshotId": "bad-6", "deviceId": "", "startAddress": 1,
             "registerCount": 1},
        ]
        for body in bad_bodies:
            try:
                request(f"{self.gateway}/api/snapshots", "POST", body)
                self.check(f"invalid rejected: {body['snapshotId']}", False, "ok")
            except HttpError as exc:
                self.check(
                    f"invalid rejected: {body['snapshotId']}",
                    exc.status == 400,
                    (exc.status, exc.payload),
                )
                if exc.status == 400:
                    try:
                        request(
                            f"{self.gateway}/api/snapshots/{body['snapshotId']}"
                        )
                        self.check(
                            f"invalid not persisted: {body['snapshotId']}",
                            False,
                            "found",
                        )
                    except HttpError as miss:
                        self.check(
                            f"invalid not persisted: {body['snapshotId']}",
                            miss.status == 404,
                            miss.status,
                        )
