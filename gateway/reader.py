"""Paged, revision-consistent reader for the buoy controller.

One snapshot read spans multiple pages of at most ``PAGE_SIZE`` registers.

* The first page is fetched unconditionally and *establishes* the revision.
* Every later page carries that revision as a precondition; the controller
  rejects the page with 409 if its configuration changed in the meantime.

On any revision change, malformed/short/out-of-range page, HTTP error or
transport timeout the **entire** round is discarded and reading restarts from
the first page.  After ``MAX_ATTEMPTS`` unstable rounds :class:`SnapshotUnstable`
is raised and nothing gathered is persisted by the caller.
"""

import hashlib
import json
import os
import socket
import time
import urllib.error
import urllib.request

from common.protocol import MAX_ATTEMPTS, PAGE_SIZE

# Per-page socket timeout; overridable via environment for fast test runs.
PAGE_TIMEOUT = float(os.environ.get("PAGE_TIMEOUT", "3.0"))


class ReadFailure(Exception):
    """A single failed reading round."""

    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind
        self.message = message

    def descriptor(self):
        return f"{self.kind}: {self.message}"


class SnapshotUnstable(Exception):
    """Three rounds could not produce one single-revision snapshot."""

    def __init__(self, reasons):
        self.reasons = reasons
        joined = "; ".join(reasons)
        super().__init__(
            f"snapshot unstable after {MAX_ATTEMPTS} attempts [{joined}]"
        )


def _fetch_page(base_url, start, count, revision, deadline):
    url = f"{base_url}/registers?start={start}&count={count}"
    if revision is not None:
        url += f"&revision={revision}"

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ReadFailure("timeout", "read deadline exceeded before page fetch")
    timeout = min(PAGE_TIMEOUT, remaining)

    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            raise ReadFailure(
                "revision_changed",
                "controller rejected revision precondition (409)",
            )
        raise ReadFailure(
            "page_error", f"controller returned HTTP {exc.code}"
        )
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
        raise ReadFailure("transport_error", f"upstream unreachable: {exc}")

    try:
        page = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ReadFailure("malformed_page", "response was not valid JSON")

    if not isinstance(page, dict):
        raise ReadFailure("malformed_page", "page payload is not an object")

    values = page.get("values")
    page_start = page.get("startAddress")
    page_revision = page.get("revision")

    if not isinstance(page_start, int) or isinstance(page_start, bool):
        raise ReadFailure("malformed_page", "page startAddress is not an integer")
    if page_start != start:
        # A page covering different addresses means overlap/gap: duplicate or
        # missing addresses, never part of an ordered evidence chain.
        raise ReadFailure(
            "address_mismatch",
            f"expected page start {start}, controller sent {page_start}",
        )
    if not isinstance(page_revision, int) or isinstance(page_revision, bool):
        raise ReadFailure("malformed_page", "page revision is not an integer")
    if revision is not None and page_revision != revision:
        raise ReadFailure(
            "revision_changed",
            f"expected revision {revision}, page carries {page_revision}",
        )
    if not isinstance(values, list):
        raise ReadFailure("malformed_page", "page values are not a list")
    if len(values) != count:
        # Short page or missing registers: the window would have holes.
        raise ReadFailure(
            "short_page", f"expected {count} registers, got {len(values)}"
        )
    for index, value in enumerate(values):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ReadFailure(
                "malformed_value",
                f"value at address {start + index} is not an integer",
            )
        if not 0 <= value <= 0xFFFF:
            raise ReadFailure(
                "value_out_of_range",
                f"value at address {start + index} is not a 16-bit "
                f"unsigned integer: {value}",
            )

    return values, page_revision


def _one_round(base_url, start_address, count, deadline):
    """Read all pages under one revision; return (revision, values, sha256)."""
    revision = None
    collected = []
    offset = 0
    while offset < count:
        page_start = start_address + offset
        page_count = min(PAGE_SIZE, count - offset)
        values, revision = _fetch_page(
            base_url, page_start, page_count, revision, deadline
        )
        collected.extend(values)
        offset += page_count

    packed = b"".join(value.to_bytes(2, byteorder="big") for value in collected)
    digest = hashlib.sha256(packed).hexdigest()
    return revision, collected, digest


def read_stable_snapshot(base_url, start_address, count, deadline_seconds):
    """Read a full snapshot, discarding every unstable round.

    Returns ``(revision, values, sha256, attempts)`` where ``attempts`` counts
    the rounds consumed (1 means the first round was stable).
    """
    reasons = []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        deadline = time.monotonic() + deadline_seconds
        try:
            revision, collected, digest = _one_round(
                base_url, start_address, count, deadline
            )
            return revision, collected, digest, attempt
        except ReadFailure as failure:
            reasons.append(failure.descriptor())
    raise SnapshotUnstable(reasons)
