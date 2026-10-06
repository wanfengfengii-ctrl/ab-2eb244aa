"""Crash-safe snapshot storage.

Each snapshot is one JSON file written atomically (temp file + fsync +
``os.replace``).  A snapshot moves through three states::

    pending -> complete      (immutable, replayable evidence)
    pending -> conflict      (terminal failure; no evidence payload)

A ``pending`` file left by a crashed process is reclaimable: a retried request
with the same parameters adopts it and finishes the read.
"""

import json
import os
import re
import tempfile
import threading
from datetime import datetime, timezone

ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}\Z")

STATE_PENDING = "pending"
STATE_COMPLETE = "complete"
STATE_CONFLICT = "conflict"


class ParameterConflict(Exception):
    """A snapshot id already exists with different parameters."""


class InvalidSnapshotId(Exception):
    pass


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SnapshotStore:
    def __init__(self, directory):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()

    @staticmethod
    def validate_id(snapshot_id):
        if not isinstance(snapshot_id, str) or not ID_PATTERN.match(snapshot_id):
            raise InvalidSnapshotId(
                "snapshotId must be 1..128 chars of [A-Za-z0-9_-]"
            )

    def _path(self, snapshot_id):
        return os.path.join(self.directory, f"snapshot-{snapshot_id}.json")

    def exists(self, snapshot_id):
        return os.path.exists(self._path(snapshot_id))

    def load(self, snapshot_id):
        with open(self._path(snapshot_id), "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _atomic_write(self, snapshot_id, record):
        path = self._path(snapshot_id)
        directory = os.path.dirname(path)
        fd, tmp = tempfile.mkstemp(
            prefix=f".tmp-{snapshot_id}-", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(record, fh, indent=2, sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def create_or_get(self, snapshot_id, device_id, start_address, count):
        """Return ``(record, created)`` for a snapshot request.

        Same id + same parameters returns the existing record.  A pending
        record is returned so the caller can adopt and finish it.  Different
        parameters raise :class:`ParameterConflict`.
        """
        self.validate_id(snapshot_id)
        with self._lock:
            if self.exists(snapshot_id):
                record = self.load(snapshot_id)
                params = (
                    record["deviceId"],
                    record["startAddress"],
                    record["registerCount"],
                )
                if params != (device_id, start_address, count):
                    raise ParameterConflict(record)
                return record, False

            record = {
                "snapshotId": snapshot_id,
                "deviceId": device_id,
                "startAddress": start_address,
                "registerCount": count,
                "state": STATE_PENDING,
                "revision": None,
                "sha256": None,
                "values": None,
                "attempts": 0,
                "reason": None,
                "attemptReasons": [],
                "createdAt": now_iso(),
                "updatedAt": now_iso(),
            }
            self._atomic_write(snapshot_id, record)
            return record, True

    def commit_conflict(self, record, reason):
        record["state"] = STATE_CONFLICT
        record["reason"] = reason
        record["revision"] = None
        record["sha256"] = None
        record["values"] = None
        record["updatedAt"] = now_iso()
        self._atomic_write(record["snapshotId"], record)
        return record

    def commit_complete(self, record, revision, values, digest):
        record["state"] = STATE_COMPLETE
        record["revision"] = revision
        record["values"] = values
        record["sha256"] = digest
        record["reason"] = None
        record["updatedAt"] = now_iso()
        self._atomic_write(record["snapshotId"], record)
        return record
