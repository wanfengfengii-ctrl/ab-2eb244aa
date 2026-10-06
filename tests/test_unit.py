"""Unit tests: revision-consistent reader, crash-safe store, validation.

The simulator runs in-process on an ephemeral port; no Docker required.
"""

import hashlib
import os
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import simulator.app as sim  # noqa: E402
from common.protocol import (  # noqa: E402
    MAX_REGISTERS,
    validate_params,
)
from gateway import reader  # noqa: E402
from gateway.reader import (  # noqa: E402
    ReadFailure,
    SnapshotUnstable,
    read_stable_snapshot,
)
from gateway.storage import (  # noqa: E402
    InvalidSnapshotId,
    ParameterConflict,
    SnapshotStore,
)


class ReaderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), sim.Handler)
        cls.port = cls.server.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(
            target=cls.server.serve_forever, daemon=True
        )
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        sim.STATE.reset(1)
        self.addCleanup(lambda: setattr(reader, "PAGE_TIMEOUT", 3.0))

    def _expected(self, revision, start, count):
        values = [sim.register_value(revision, start + i) for i in range(count)]
        packed = b"".join(v.to_bytes(2, "big") for v in values)
        return values, hashlib.sha256(packed).hexdigest()

    def test_single_page_stable(self):
        values, digest = self._expected(1, 1, 64)
        revision, got, got_digest, attempts = read_stable_snapshot(
            self.base_url, 1, 64, 10
        )
        self.assertEqual((revision, attempts), (1, 1))
        self.assertEqual(got, values)
        self.assertEqual(got_digest, digest)

    def test_multi_page_boundary(self):
        values, digest = self._expected(1, 100, 65)
        revision, got, got_digest, attempts = read_stable_snapshot(
            self.base_url, 100, 65, 10
        )
        self.assertEqual((revision, attempts), (1, 1))
        self.assertEqual(got, values)
        self.assertEqual(got_digest, digest)
        self.assertEqual(len(got), 65)

    def test_max_window(self):
        revision, got, digest, attempts = read_stable_snapshot(
            self.base_url, 1, MAX_REGISTERS, 30
        )
        self.assertEqual(revision, 1)
        self.assertEqual(len(got), MAX_REGISTERS)
        self.assertEqual(got, self._expected(1, 1, MAX_REGISTERS)[0])

    def test_revision_bump_then_retry_succeeds(self):
        sim.STATE.set_mode("bump-after", {"afterReads": 1})
        revision, got, digest, attempts = read_stable_snapshot(
            self.base_url, 1, 100, 10
        )
        # Round 1 reads page 1 at revision 1, revision bumps; round 2 reads
        # the whole window consistently at revision 2.
        self.assertEqual(attempts, 2)
        self.assertEqual(revision, 2)
        expected_values, expected_digest = self._expected(2, 1, 100)
        self.assertEqual(got, expected_values)
        self.assertEqual(digest, expected_digest)

    def test_persistent_flap_is_unstable(self):
        sim.STATE.set_mode("flap")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 100, 10)
        reasons = ctx.exception.reasons
        self.assertEqual(len(reasons), 3)
        self.assertTrue(
            all(r.startswith("revision_changed") for r in reasons), reasons
        )

    def test_short_page_is_unstable(self):
        sim.STATE.set_mode("short-page")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 100, 10)
        self.assertTrue(
            all(r.startswith("short_page") for r in ctx.exception.reasons)
        )

    def test_duplicate_addresses_is_unstable(self):
        sim.STATE.set_mode("duplicate-addresses")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 100, 10)
        self.assertTrue(
            all(
                r.startswith("address_mismatch")
                for r in ctx.exception.reasons
            ),
            ctx.exception.reasons,
        )

    def test_out_of_range_value_is_unstable(self):
        sim.STATE.set_mode("out-of-range")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 10, 10)
        self.assertTrue(
            all(r.startswith("value_out_of_range") for r in ctx.exception.reasons)
        )

    def test_page_error_is_unstable(self):
        sim.STATE.set_mode("page-error")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 10, 10)
        self.assertTrue(
            all(r.startswith("page_error") for r in ctx.exception.reasons)
        )

    def test_timeout_is_unstable(self):
        reader.PAGE_TIMEOUT = 0.3
        sim.STATE.set_mode("timeout", {"delaySeconds": 5})
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 10, 10)
        self.assertTrue(
            all(r.startswith("transport_error") for r in ctx.exception.reasons)
        )

    def test_dropped_connection_is_unstable(self):
        sim.STATE.set_mode("drop")
        with self.assertRaises(SnapshotUnstable) as ctx:
            read_stable_snapshot(self.base_url, 1, 10, 10)
        self.assertTrue(
            all(r.startswith("transport_error") for r in ctx.exception.reasons)
        )

    def test_failure_descriptors_are_stable_strings(self):
        failure = ReadFailure("revision_changed", "boom")
        self.assertEqual(failure.descriptor(), "revision_changed: boom")


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = SnapshotStore(self.tmp)

    def test_create_is_idempotent_for_same_params(self):
        first, created = self.store.create_or_get("snap-1", "buoy-a", 1, 10)
        second, created_again = self.store.create_or_get("snap-1", "buoy-a", 1, 10)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first["snapshotId"], second["snapshotId"])

    def test_different_params_conflict(self):
        self.store.create_or_get("snap-1", "buoy-a", 1, 10)
        with self.assertRaises(ParameterConflict):
            self.store.create_or_get("snap-1", "buoy-a", 1, 11)
        with self.assertRaises(ParameterConflict):
            self.store.create_or_get("snap-1", "buoy-b", 1, 10)
        with self.assertRaises(ParameterConflict):
            self.store.create_or_get("snap-1", "buoy-a", 2, 10)

    def test_invalid_id(self):
        for bad in ("", "id with space", "../escape", "x" * 129, 123):
            with self.assertRaises(InvalidSnapshotId):
                SnapshotStore.validate_id(bad)

    def test_complete_then_reload(self):
        record, _ = self.store.create_or_get("snap-2", "buoy", 5, 3)
        self.store.commit_complete(record, 7, [1, 2, 3], "a" * 64)
        reloaded = self.store.load("snap-2")
        self.assertEqual(reloaded["state"], "complete")
        self.assertEqual(reloaded["revision"], 7)
        self.assertEqual(reloaded["sha256"], "a" * 64)

    def test_conflict_keeps_no_evidence(self):
        record, _ = self.store.create_or_get("snap-3", "buoy", 1, 10)
        record["attempts"] = 3
        record["attemptReasons"] = ["revision_changed: x"] * 3
        self.store.commit_conflict(record, "revision unstable or transport failing")
        reloaded = self.store.load("snap-3")
        self.assertEqual(reloaded["state"], "conflict")
        self.assertIsNone(reloaded["values"])
        self.assertIsNone(reloaded["sha256"])
        self.assertIsNone(reloaded["revision"])
        self.assertEqual(reloaded["attempts"], 3)

    def test_pending_adoption_by_second_store_instance(self):
        store_a = SnapshotStore(self.tmp)
        store_a.create_or_get("orphan", "buoy", 1, 20)
        store_b = SnapshotStore(self.tmp)
        adopted, created = store_b.create_or_get("orphan", "buoy", 1, 20)
        self.assertFalse(created)
        self.assertEqual(adopted["state"], "pending")
        store_b.commit_complete(adopted, 9, [4] * 20, "b" * 64)
        self.assertEqual(store_a.load("orphan")["revision"], 9)

    def test_writes_are_atomic(self):
        record, _ = self.store.create_or_get("snap-4", "buoy", 1, 5)
        self.store.commit_complete(record, 1, [0], "c" * 64)
        leftovers = [
            name
            for name in os.listdir(self.tmp)
            if name.startswith(".tmp")
        ]
        self.assertEqual(leftovers, [])


class ValidationTestCase(unittest.TestCase):
    def test_window_limits(self):
        self.assertEqual(validate_params(1, MAX_REGISTERS), (None, None))
        self.assertIsNotNone(validate_params(1, MAX_REGISTERS + 1)[0])
        self.assertIsNotNone(validate_params(1, 0)[0])
        self.assertEqual(validate_params(1, 1), (None, None))
        self.assertEqual(validate_params(65536, 1), (None, None))
        self.assertIsNotNone(validate_params(65536, 2)[0])
        self.assertIsNotNone(validate_params(0, 1)[0])

    def test_types(self):
        self.assertIsNotNone(validate_params(True, 1)[0])
        self.assertIsNotNone(validate_params(1, True)[0])
        self.assertIsNotNone(validate_params("1", 1)[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
