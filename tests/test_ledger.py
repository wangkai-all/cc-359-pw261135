"""Ledger invariant tests. Stdlib unittest; no network, no fixtures."""

from __future__ import annotations

import io
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from refract.ledger import Conflict, Gone, Invalid, Ledger  # noqa: E402


class Clock:
    def __init__(self, start: float = 1_800_000_000.0):
        self.t = start
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            return self.t

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.t += seconds


class LedgerCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.ledger = Ledger(self.tmp.name, clock=self.clock, lease_ttl=10)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def deliver(self, source, receipt, body=b"payload", **kwargs):
        kwargs.setdefault("retain_seconds", 600)
        return self.ledger.receive(source, receipt, io.BytesIO(body), **kwargs)

    def package(self, digest):
        return next(p for p in self.ledger.snapshot()["packages"] if p["digest"] == digest)


class DuplicateDeliveryTests(LedgerCase):
    def test_identical_bytes_share_one_blob_and_keep_separate_provenance(self):
        body = b"identical bytes" * 100
        a = self.deliver("atlas", "A-1", body, filename="alpha.bin", attestation="atlas sig")
        b = self.deliver("harbor", "H-1", body, filename="beta.bin", attestation="harbor sig")
        self.assertFalse(a["duplicate_bytes"])
        self.assertTrue(b["duplicate_bytes"])
        self.assertEqual(a["package_digest"], b["package_digest"])
        pkg = self.package(a["package_digest"])
        self.assertEqual(pkg["size"], len(body))
        self.assertEqual(len(pkg["deliveries"]), 2)
        self.assertEqual(pkg["reference_count"], 2)
        receipts = {d["source_id"]: (d["receipt_no"], d["attestation"], d["filename"]) for d in pkg["deliveries"]}
        self.assertEqual(receipts["atlas"], ("A-1", "atlas sig", "alpha.bin"))
        self.assertEqual(receipts["harbor"], ("H-1", "harbor sig", "beta.bin"))
        blobs = list((self.ledger.data_dir / "blobs").rglob("[0-9a-f]" * 1))
        physical = [p for p in (self.ledger.data_dir / "blobs").rglob("*") if p.is_file()]
        self.assertEqual(len(physical), 1)

    def test_same_receipt_twice_is_conflict_and_keeps_one_delivery(self):
        self.deliver("atlas", "A-1")
        with self.assertRaises(Conflict):
            self.deliver("atlas", "A-1", b"different bytes")
        snap = self.ledger.snapshot()
        self.assertEqual(snap["kpis"]["deliveries_total"], 1)
        self.assertEqual(snap["kpis"]["physical_blobs"], 1)

    def test_same_receipt_number_for_two_sources_is_allowed(self):
        a = self.deliver("atlas", "SHARED-9")
        b = self.deliver("harbor", "SHARED-9")
        self.assertNotEqual(a["delivery"]["id"], b["delivery"]["id"])

    def test_digest_mismatch_is_rejected_and_leaves_nothing_behind(self):
        with self.assertRaises(Invalid):
            self.deliver("atlas", "A-1", expected_sha256="0" * 64)
        self.assertEqual(self.ledger.snapshot()["kpis"]["physical_blobs"], 0)
        self.assertEqual(
            [p for p in (self.ledger.data_dir / "blobs" / ".tmp").glob("*")],
            [],
        )


class RecallTests(LedgerCase):
    def setUp(self):
        super().setUp()
        body = b"shared" * 10
        self.a = self.deliver("atlas", "A-1", body)
        self.b = self.deliver("harbor", "H-1", body)
        self.digest = self.a["package_digest"]

    def test_recall_closes_only_that_source(self):
        self.ledger.recall(self.b["delivery"]["id"], "upstream takedown")
        with self.assertRaises(Gone) as ctx:
            self.ledger.acquire_lease(self.b["delivery"]["id"])
        self.assertEqual(ctx.exception.code, "delivery_recalled")
        lease = self.ledger.acquire_lease(self.a["delivery"]["id"], client="reader")
        chunk = self.ledger.stream_chunk(lease["lease_id"], 0, 24)
        self.assertEqual(chunk["data"], b"shared" * 4)

    def test_recall_fences_a_live_lease_but_not_the_shared_blob(self):
        lease = self.ledger.acquire_lease(self.b["delivery"]["id"])
        self.ledger.stream_chunk(lease["lease_id"], 0, 10)
        self.ledger.recall(self.b["delivery"]["id"], "takedown")
        with self.assertRaises(Gone):
            self.ledger.stream_chunk(lease["lease_id"], 10, 10)
        pkg = self.package(self.digest)
        lease_view = next(l for l in pkg["leases"] if l["id"] == lease["lease_id"])
        self.assertEqual(lease_view["state"], "fenced")
        self.assertTrue(pkg["physical"], "recall must not delete bytes another source still references")
        self.assertEqual(pkg["reference_count"], 1)
        self.assertEqual([d["source_id"] for d in pkg["valid_references"]], ["atlas"])

    def test_recall_does_not_block_cleanup_after_other_sources_let_go(self):
        self.ledger.recall(self.b["delivery"]["id"], "takedown")
        self.ledger.recall(self.a["delivery"]["id"], "window closed")
        out = self.ledger.cleanup()
        self.assertEqual([p["digest"] for p in out["purged"]], [self.digest])
        self.assertFalse(self.ledger.blobs.exists(self.digest))
        pkg = self.package(self.digest)
        self.assertTrue(pkg["tombstone"])
        self.assertEqual(pkg["cleanup"]["disposition"], "purged")
        # Provenance survives the bytes.
        self.assertEqual(pkg["deliveries"][1]["recall_reason"], "takedown")


class CleanupGateTests(LedgerCase):
    def test_valid_reference_blocks_purge(self):
        a = self.deliver("atlas", "A-1")
        out = self.ledger.cleanup()
        self.assertEqual(out["purged"], [])
        self.assertEqual(len(out["blocked"]), 1)
        self.assertEqual(out["blocked"][0]["valid_references"], 1)
        pkg = self.package(a["package_digest"])
        self.assertEqual(pkg["cleanup"]["disposition"], "blocked")
        self.assertTrue(any(b["kind"] == "reference" for b in pkg["cleanup"]["blockers"]))

    def test_expired_reference_permits_purge(self):
        a = self.deliver("atlas", "A-1", retain_seconds=30)
        self.clock.advance(31)
        out = self.ledger.cleanup()
        self.assertEqual(len(out["purged"]), 1)
        self.assertFalse(self.ledger.blobs.exists(a["package_digest"]))

    def test_active_download_blocks_purge(self):
        a = self.deliver("atlas", "A-1", retain_seconds=5)
        lease = self.ledger.acquire_lease(a["delivery"]["id"], client="reader")  # ttl 10
        self.ledger.stream_chunk(lease["lease_id"], 0, 1)
        self.clock.advance(6)  # retention gone, lease still fresh
        out = self.ledger.cleanup()
        self.assertEqual(out["purged"], [])
        self.assertEqual(out["blocked"][0]["active_leases"], 1)
        self.assertTrue(self.ledger.blobs.exists(a["package_digest"]))

    def test_slow_reader_that_keeps_reading_keeps_the_blob_alive(self):
        a = self.deliver("atlas", "A-1", b"x" * 100, retain_seconds=5)
        lease = self.ledger.acquire_lease(a["delivery"]["id"], client="slow")
        self.clock.advance(4)
        self.ledger.stream_chunk(lease["lease_id"], 0, 10)  # renews lease
        self.clock.advance(4)
        self.assertEqual(self.ledger.cleanup()["purged"], [])
        self.ledger.stream_chunk(lease["lease_id"], 10, 10)  # renews again
        self.clock.advance(4)
        self.assertEqual(self.ledger.cleanup()["purged"], [])

    def test_stalled_reader_loses_the_lease_then_cleanup_purges_and_reads_fail_closed(self):
        a = self.deliver("atlas", "A-1", b"y" * 200, retain_seconds=5)
        lease = self.ledger.acquire_lease(a["delivery"]["id"], client="stalled")
        self.ledger.stream_chunk(lease["lease_id"], 0, 10)
        self.clock.advance(20)  # > ttl and > retention
        self.ledger.enforce_scheduled()
        self.assertEqual(len(self.ledger.cleanup()["purged"]), 1)
        with self.assertRaises(Gone):
            self.ledger.stream_chunk(lease["lease_id"], 10, 10)

    def test_cleanup_is_atomic_against_a_concurrent_reader(self):
        """A reader either renews first (cleanup waits) or fails after the purge."""
        a = self.deliver("atlas", "A-1", b"z" * 4000, retain_seconds=60)
        lease = self.ledger.acquire_lease(a["delivery"]["id"], client="racer")
        self.ledger.stream_chunk(lease["lease_id"], 0, 100)

        results = {"chunks": 0, "gone": 0}
        reading = threading.Event()
        reading.set()

        def reader():
            offset = 100
            while offset < 4000 and reading.is_set():
                try:
                    out = self.ledger.stream_chunk(lease["lease_id"], offset, 100)
                    offset = out["next"]
                    results["chunks"] += 1
                except Gone:
                    results["gone"] += 1
                    return
                self.clock.advance(0.5)

        def cleaner():
            for _ in range(60):
                self.clock.advance(0.5)
                self.ledger.cleanup()

        threads = [threading.Thread(target=reader), threading.Thread(target=cleaner)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertGreater(results["chunks"], 0)
        # A chunk is never served from a purged package: either the reader
        # finished while cleanup waited, or it got a 410 and cleanup purged.
        purged = not self.ledger.blobs.exists(a["package_digest"])
        if purged:
            self.assertTrue(self.package(a["package_digest"])["tombstone"])
            self.assertGreater(results["gone"], 0)

    def test_retention_ended_by_operator_frees_cleanup(self):
        a = self.deliver("atlas", "A-1")
        self.ledger.end_retention(a["delivery"]["id"])
        self.assertEqual(len(self.ledger.cleanup()["purged"]), 1)

    def test_dry_run_reports_without_deleting(self):
        a = self.deliver("atlas", "A-1", retain_seconds=1)
        self.clock.advance(2)
        out = self.ledger.cleanup(dry_run=True)
        self.assertEqual(len(out["purged"]), 1)
        self.assertTrue(self.ledger.blobs.exists(a["package_digest"]))

    def test_receiving_again_after_purge_resurrects_the_bytes(self):
        a = self.deliver("atlas", "A-1", b"resurrect me", retain_seconds=1)
        self.clock.advance(2)
        self.assertEqual(len(self.ledger.cleanup()["purged"]), 1)
        self.assertFalse(self.ledger.blobs.exists(a["package_digest"]))
        again = self.deliver("orchard", "O-1", b"resurrect me")
        self.assertTrue(again["resurrected"])
        self.assertTrue(self.ledger.blobs.exists(again["package_digest"]))
        pkg = self.package(again["package_digest"])
        self.assertFalse(pkg["tombstone"])
        self.assertEqual(len(pkg["deliveries"]), 2)  # tombstoned delivery is still visible history


class RestartTests(LedgerCase):
    def test_restart_abandons_leases_and_unblocks_cleanup(self):
        a = self.deliver("atlas", "A-1", retain_seconds=5)
        lease = self.ledger.acquire_lease(a["delivery"]["id"], client="browser")
        self.ledger.stream_chunk(lease["lease_id"], 0, 1)
        self.assertEqual(self.ledger.cleanup()["purged"], [])
        self.ledger.rotate_epoch()
        recovery = self.ledger.recover()
        self.assertEqual(recovery["abandoned_leases"], [lease["lease_id"]])
        view = self.ledger.lease_view_by_id(lease["lease_id"])
        self.assertEqual(view["state"], "abandoned")
        self.clock.advance(6)
        self.assertEqual(len(self.ledger.cleanup()["purged"]), 1)

    def test_restart_does_not_break_another_source_reference(self):
        body = b"shared across a restart"
        self.deliver("atlas", "A-1", body)
        self.deliver("harbor", "H-1", body)
        self.ledger.rotate_epoch()
        self.ledger.recover()
        snap = self.ledger.snapshot()
        self.assertEqual(snap["kpis"]["valid_references"], 2)
        self.assertEqual(snap["kpis"]["physical_blobs"], 1)

    def test_recovery_tombstones_a_blob_deleted_behind_the_ledger(self):
        a = self.deliver("atlas", "A-1")
        self.ledger.blobs.remove(a["package_digest"])
        recovery = self.ledger.recover()
        self.assertEqual(recovery["missing_blobs"], [a["package_digest"]])
        pkg = self.package(a["package_digest"])
        self.assertTrue(pkg["tombstone"])
        self.assertEqual(pkg["cleanup"]["disposition"], "purged")

    def test_scheduled_recall_fires_and_fences(self):
        a = self.deliver("atlas", "A-1", recall_after_seconds=10)
        self.clock.advance(11)
        out = self.ledger.enforce_scheduled()
        self.assertEqual(out["recalled"], [a["delivery"]["id"]])
        with self.assertRaises(Gone):
            self.ledger.acquire_lease(a["delivery"]["id"])


class UploadValidationTests(LedgerCase):
    def test_unknown_source(self):
        from refract.ledger import NotFound
        with self.assertRaises(NotFound):
            self.deliver("nope", "x")

    def test_filename_is_sanitized(self):
        out = self.deliver("atlas", "A-1", filename="../../etc/passwd\r\n")
        self.assertEqual(out["delivery"]["filename"], "passwd")

    def test_unsafe_media_type_falls_back_to_octet_stream(self):
        out = self.deliver("atlas", "A-1", media_type='text/html"\r\nX-Evil: 1')
        self.assertEqual(out["delivery"]["declared_media_type"], "application/octet-stream")
        out = self.deliver("atlas", "A-2", media_type="text/plain; charset=utf-8")
        self.assertEqual(out["delivery"]["declared_media_type"], "text/plain")

    def test_upload_limit(self):
        from unittest import mock
        import refract.ledger as mod
        with mock.patch.object(mod, "MAX_UPLOAD_BYTES", 10):
            with self.assertRaises(Invalid):
                self.deliver("atlas", "A-1", b"x" * 11)

    def test_offset_outside_package(self):
        a = self.deliver("atlas", "A-1", b"12345")
        with self.assertRaises(Invalid):
            self.ledger.acquire_lease(a["delivery"]["id"], offset=6)
        with self.assertRaises(Invalid):
            self.ledger.acquire_lease(a["delivery"]["id"], offset=-1)


if __name__ == "__main__":
    unittest.main()
