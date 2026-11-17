"""Core custody ledger: deliveries, references, leases and physical cleanup.

Invariants
==========

1. Bytes are content-addressed. Two sources delivering identical content share
   one physical blob; they never share a delivery row.
2. A delivery owns its provenance (receipt, attestation, observed digest),
   retention window and recall state. Nothing copies provenance onto the
   package or merges it across deliveries.
3. Access is granted per delivery (per source). A recalled source's grant is
   refused, and any live lease issued under it is fenced immediately. A lease
   under another source delivering the same bytes keeps working untouched.
4. Bytes are purged only when *all* of these hold inside one ``BEGIN
   IMMEDIATE`` transaction:
      - no delivery for the digest is still a valid reference;
      - no lease for the digest is still active;
      - the file is present.
   The unlink happens inside that transaction, so a chunk read can never
   observe a half-purged package: either its lease was renewed first (cleanup
   then waits) or cleanup committed first (the read sees a tombstone and
   returns 410).
5. A live download renews its lease on every chunk, so a slow download keeps
   the bytes alive and cleanup waits. A stalled download loses its lease on
   TTL; cleanup may then purge, and the next chunk request fails cleanly with
   ``410`` rather than delivering a truncated file.
6. Retention expiry does not retroactively invalidate a lease that was granted
   while the delivery was valid; the lease ends, cleanup waits, then bytes may
   go. Recall is different: it stops distribution immediately and fences every
   live lease under that source.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

from .blobstore import BlobStore
from .db import connect, init_db

DEFAULT_SOURCES = [
    ("atlas", "Atlas Registry", "Synthetic registry mirror · signed receipts", 1),
    ("harbor", "Harbor Exchange", "Synthetic partner exchange · batch deliveries", 2),
    ("orchard", "Orchard Signal", "Synthetic research feed · short retention", 3),
]

MAX_UPLOAD_BYTES = 128 * 1024 * 1024
LEASE_TTL = 30.0
MAX_CHUNK = 4 * 1024 * 1024


# --------------------------------------------------------------------------
# errors


class LedgerError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, *, code: str | None = None, status: int | None = None):
        super().__init__(message)
        if code:
            self.code = code
        if status:
            self.status = status


class NotFound(LedgerError):
    status, code = 404, "not_found"


class Conflict(LedgerError):
    status, code = 409, "conflict"


class Gone(LedgerError):
    status, code = 410, "gone"


class Invalid(LedgerError):
    status, code = 422, "invalid"


# --------------------------------------------------------------------------
# helpers


@contextmanager
def txn(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _clean_text(value: str | None, *, limit: int, field: str) -> str:
    value = (value or "").strip()
    if len(value) > limit:
        raise Invalid(f"{field} is longer than {limit} characters")
    return value


def _clean_filename(value: str | None) -> str:
    value = _clean_text(value, limit=255, field="filename")
    value = value.replace("\\", "/").split("/")[-1]
    value = re.sub(r"[\x00-\x1f\x7f\"]", "", value).strip()
    return value or "package.bin"


def _clean_media_type(value: str | None) -> str:
    """Keep media types to a safe header token; anything else is binary."""
    value = (value or "").split(";")[0].strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9!#$&^_.+\-]*\/[a-z0-9][a-z0-9!#$&^_.+\-]*", value):
        return "application/octet-stream"
    return value


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


# --------------------------------------------------------------------------
# ledger


class Ledger:
    def __init__(
        self,
        data_dir: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        lease_ttl: float = LEASE_TTL,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "ledger.db"
        self.blobs = BlobStore(self.data_dir / "blobs")
        self.clock = clock
        self.lease_ttl = float(lease_ttl)
        self.clock_offset = 0.0
        self.epoch = uuid.uuid4().hex
        self.started_at = self.clock()

        conn = connect(self.db_path)
        try:
            init_db(conn)
            self._load_clock(conn)
            self.seed_sources(conn)
        finally:
            conn.close()
        self.last_recovery = self.recover()

    def _load_clock(self, conn: sqlite3.Connection) -> None:
        """Persist the demo clock offset so it advances across a restart."""
        row = conn.execute("SELECT value FROM meta WHERE key='clock_offset'").fetchone()
        if row:
            self.clock_offset = float(row["value"])

    def _save_clock(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('clock_offset', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"{self.clock_offset:.3f}",),
        )

    def now(self) -> float:
        """Ledger time: wall clock plus the demo offset, clamped to be monotone."""
        base = self.clock() + self.clock_offset
        if base < getattr(self, "_last_now", 0.0):
            base = self._last_now
        self._last_now = base
        return base

    def advance_clock(self, seconds: float) -> float:
        """Move the demo clock forward (persisted; never moves backwards)."""
        if seconds < 0:
            raise Invalid("clock can only move forward")
        with self._conn() as conn:
            with txn(conn):
                self.clock_offset += float(seconds)
                self._save_clock(conn)
                self._log(
                    conn,
                    "clock",
                    "demo",
                    "clock_advanced",
                    f"+{seconds:g}s · ledger time now {iso(self.now())}",
                )
        return self.now()

    # -- plumbing --------------------------------------------------------

    @contextmanager
    def _conn(self):
        conn = connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def seed_sources(self, conn: sqlite3.Connection) -> None:
        count = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        if count:
            return
        with txn(conn):
            for sid, name, note, slot in DEFAULT_SOURCES:
                conn.execute(
                    "INSERT INTO sources(id, display_name, note, color_slot, created_at) "
                    "VALUES (?,?,?,?,?)",
                    (sid, name, note, slot, self.started_at),
                )

    def _log(self, conn, entity: str, entity_id: str, action: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO events(at, entity, entity_id, action, detail) VALUES (?,?,?,?,?)",
            (self.now(), entity, entity_id, action, detail),
        )

    def _blob_event(self, conn, digest: str, action: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO blob_events(digest, at, action, detail) VALUES (?,?,?,?)",
            (digest, self.now(), action, detail),
        )

    def rotate_epoch(self) -> str:
        self.epoch = uuid.uuid4().hex
        return self.epoch

    # -- recovery --------------------------------------------------------

    def recover(self) -> dict:
        """Reconcile after a (re)start.

        Leases do not survive a server process: anything still active under a
        previous epoch is abandoned and must stop gating cleanup. Physical
        files that vanished underneath the ledger become tombstones rather
        than phantom references, and files with no package row (an upload that
        died between ``os.replace`` and commit) are removed.
        """
        now = self.now()
        abandoned: list[str] = []
        missing: list[str] = []
        orphans: list[str] = []
        with self._conn() as conn:
            with txn(conn):
                rows = conn.execute(
                    "SELECT * FROM leases WHERE state='active' AND epoch != ?",
                    (self.epoch,),
                ).fetchall()
                for row in rows:
                    reason = "server restarted before the download completed"
                    conn.execute(
                        "UPDATE leases SET state='abandoned', streaming=0, finished_at=?, "
                        "fence_reason=? WHERE id=?",
                        (now, reason, row["id"]),
                    )
                    self._log(
                        conn,
                        "lease",
                        row["id"],
                        "lease_abandoned",
                        f"{row['bytes_sent']} bytes read before restart · {row['client'] or 'unnamed client'}",
                    )
                    abandoned.append(row["id"])
                self._log(
                    conn,
                    "server",
                    self.epoch,
                    "server_started",
                    f"epoch {self.epoch[:10]} · recovered {len(abandoned)} abandoned lease(s)",
                )
            rows = conn.execute("SELECT * FROM packages WHERE tombstone=0").fetchall()
            for row in rows:
                if not self.blobs.exists(row["digest"]):
                    with txn(conn):
                        conn.execute(
                            "UPDATE packages SET tombstone=1, deleted_at=? WHERE digest=?",
                            (now, row["digest"]),
                        )
                        self._blob_event(conn, row["digest"], "missing", "bytes absent at recovery")
                        self._log(
                            conn,
                            "package",
                            row["digest"],
                            "blob_missing",
                            "bytes were absent at start-up; package tombstoned",
                        )
                    missing.append(row["digest"])
            known = {row[0] for row in conn.execute("SELECT digest FROM packages")}
            for path in self.blobs.iter_blobs():
                if path.name in known:
                    continue
                # Only sweep orphans that are old enough not to belong to an
                # upload still in flight in this process.
                try:
                    age = now - path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age < 120:
                    continue
                path.unlink(missing_ok=True)
                orphans.append(path.name)
        scrubbed = self.blobs.scrub_temp()
        return {
            "abandoned_leases": abandoned,
            "missing_blobs": missing,
            "orphan_blobs_removed": orphans,
            "temp_files_scrubbed": scrubbed,
        }

    # -- upload ----------------------------------------------------------

    def receive(
        self,
        source_id: str,
        receipt_no: str,
        stream,
        *,
        retain_seconds: float,
        recall_after_seconds: float | None = None,
        filename: str = "",
        media_type: str = "",
        attestation: str = "",
        expected_sha256: str = "",
    ) -> dict:
        """Store an uploaded byte stream and record one source delivery.

        The upload is hashed into a temp file first; only then does a single
        ledger transaction decide between a new blob, a shared duplicate, or
        the resurrection of a previously purged digest.
        """
        now = self.now()
        source_id = _clean_text(source_id, limit=64, field="source_id")
        receipt_no = _clean_text(receipt_no, limit=128, field="receipt_no")
        if not source_id:
            raise Invalid("source_id is required")
        if not receipt_no:
            raise Invalid("receipt_no is required")
        try:
            retain_seconds = float(retain_seconds)
        except (TypeError, ValueError):
            raise Invalid("retain_seconds must be a number") from None
        if not (0 < retain_seconds <= 10 * 365 * 86400):
            raise Invalid("retain_seconds must be between 0 and 10 years")
        recall_at = None
        recall_after = None
        if recall_after_seconds not in (None, ""):
            try:
                recall_after = float(recall_after_seconds)
            except (TypeError, ValueError):
                raise Invalid("recall_after_seconds must be a number") from None
            if recall_after <= 0:
                raise Invalid("recall_after_seconds must be positive")
            recall_at = now + recall_after
        filename = _clean_filename(filename)
        media_type = _clean_media_type(media_type)
        attestation = _clean_text(attestation, limit=1000, field="attestation")
        expected_sha256 = (expected_sha256 or "").strip().lower()

        temp = self.blobs.temp_path()
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(temp, "wb") as out:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_UPLOAD_BYTES:
                        raise Invalid(
                            f"upload exceeds {MAX_UPLOAD_BYTES} bytes",
                            status=413,
                            code="too_large",
                        )
                    hasher.update(chunk)
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
            digest = hasher.hexdigest()
            if expected_sha256 and expected_sha256 != digest:
                raise Invalid(
                    "declared sha256 does not match the received bytes",
                    status=422,
                    code="digest_mismatch",
                )

            with self._conn() as conn:
                src = conn.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
                if not src:
                    raise NotFound(f"unknown source {source_id!r}")

                with txn(conn):
                    if conn.execute(
                        "SELECT 1 FROM deliveries WHERE source_id=? AND receipt_no=?",
                        (source_id, receipt_no),
                    ).fetchone():
                        raise Conflict(
                            f"receipt {receipt_no!r} was already recorded for source {source_id!r}",
                            code="duplicate_receipt",
                        )
                    pkg = conn.execute("SELECT * FROM packages WHERE digest=?", (digest,)).fetchone()
                    duplicate = resurrected = False
                    dest = self.blobs.path_for(digest)

                    def install_temp() -> None:
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(temp, dest)

                    if pkg is None:
                        install_temp()
                        conn.execute(
                            "INSERT INTO packages(digest, size, media_type, blob_path, created_at, "
                            "deleted_at, tombstone) VALUES (?,?,?,?,?,NULL,0)",
                            (digest, size, media_type, str(dest.relative_to(self.data_dir)), now),
                        )
                        self._blob_event(
                            conn, digest, "stored", f"{size} bytes received from {source_id}"
                        )
                    elif not self.blobs.exists(digest):
                        install_temp()
                        conn.execute(
                            "UPDATE packages SET size=?, media_type=?, deleted_at=NULL, tombstone=0 "
                            "WHERE digest=?",
                            (size, media_type, digest),
                        )
                        resurrected = True
                        self._blob_event(
                            conn,
                            digest,
                            "resurrected",
                            f"retained bytes reinstated from {source_id} after purge",
                        )
                    else:
                        duplicate = True
                        conn.execute(
                            "UPDATE packages SET deleted_at=NULL, tombstone=0 WHERE digest=?",
                            (digest,),
                        )
                        self._blob_event(
                            conn,
                            digest,
                            "reused",
                            f"identical bytes from {source_id} reuse the existing blob",
                        )

                    delivery_id = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO deliveries(id, digest, source_id, receipt_no, filename, "
                        "declared_media_type, attestation, received_at, retain_until, recall_at, "
                        "verification, observed_sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            delivery_id,
                            digest,
                            source_id,
                            receipt_no,
                            filename,
                            media_type,
                            attestation,
                            now,
                            now + retain_seconds,
                            recall_at,
                            "sha256",
                            digest,
                        ),
                    )
                    self._log(
                        conn,
                        "delivery",
                        delivery_id,
                        "received",
                        f"{src['display_name']} · receipt {receipt_no} · {size} bytes · "
                        f"retain {retain_seconds:g}s"
                        + (" · shared blob" if duplicate else "")
                        + (" · resurrected" if resurrected else ""),
                    )
                    if recall_at is not None:
                        self._log(
                            conn,
                            "delivery",
                            delivery_id,
                            "recall_scheduled",
                            f"{src['display_name']} requested recall in {recall_after:g}s",
                        )
                    row = conn.execute(
                        "SELECT d.*, s.display_name AS source_name, s.color_slot AS color_slot "
                        "FROM deliveries d JOIN sources s ON s.id=d.source_id WHERE d.id=?",
                        (delivery_id,),
                    ).fetchone()
                return {
                    "delivery": self.delivery_view(row, self.now()),
                    "package_digest": digest,
                    "duplicate_bytes": duplicate,
                    "resurrected": resurrected,
                    "physical_blob_created": not duplicate and not resurrected,
                }
        finally:
            temp.unlink(missing_ok=True)

    # -- recall / retention ---------------------------------------------

    def recall(self, delivery_id: str, reason: str = "") -> dict:
        now = self.now()
        reason = _clean_text(reason, limit=500, field="reason") or "source requested recall"
        with self._conn() as conn:
            with txn(conn):
                row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
                if not row:
                    raise NotFound("delivery not found")
                if row["recalled_at"] is None:
                    conn.execute(
                        "UPDATE deliveries SET recalled_at=?, recall_reason=? WHERE id=?",
                        (now, reason, delivery_id),
                    )
                    cur = conn.execute(
                        "UPDATE leases SET state='fenced', streaming=0, "
                        "fence_reason=?, last_seen=? "
                        "WHERE delivery_id=? AND state='active'",
                        (f"source recalled: {reason}", now, delivery_id),
                    )
                    self._log(
                        conn,
                        "delivery",
                        delivery_id,
                        "recalled",
                        reason
                        + (f" · fenced {cur.rowcount} live lease(s)" if cur.rowcount else ""),
                    )
                row = conn.execute(
                    "SELECT d.*, s.display_name AS source_name, s.color_slot AS color_slot "
                    "FROM deliveries d JOIN sources s ON s.id=d.source_id WHERE d.id=?",
                    (delivery_id,),
                ).fetchone()
                return self.delivery_view(row, now)

    def end_retention(self, delivery_id: str, note: str = "") -> dict:
        """Operator override: end a delivery's retention window now."""
        now = self.now()
        with self._conn() as conn:
            with txn(conn):
                row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
                if not row:
                    raise NotFound("delivery not found")
                changed = conn.execute(
                    "UPDATE deliveries SET retain_until=? WHERE id=? AND retain_until > ?",
                    (now, delivery_id, now),
                ).rowcount
                if changed:
                    self._log(
                        conn,
                        "delivery",
                        delivery_id,
                        "retention_ended",
                        note or "retention ended early by operator",
                    )
                row = conn.execute(
                    "SELECT d.*, s.display_name AS source_name, s.color_slot AS color_slot "
                    "FROM deliveries d JOIN sources s ON s.id=d.source_id WHERE d.id=?",
                    (delivery_id,),
                ).fetchone()
                return self.delivery_view(row, now)

    def enforce_scheduled(self, now: float | None = None) -> dict:
        """Honour scheduled recalls, observe retention ends and stale leases."""
        now = self.now() if now is None else now
        out = {"recalled": [], "expired": [], "leases_expired": []}
        with self._conn() as conn:
            due = conn.execute(
                "SELECT id, source_id FROM deliveries "
                "WHERE recalled_at IS NULL AND recall_at IS NOT NULL AND recall_at <= ?",
                (now,),
            ).fetchall()
            for d in due:
                with txn(conn):
                    changed = conn.execute(
                        "UPDATE deliveries SET recalled_at=?, recall_reason=? "
                        "WHERE id=? AND recalled_at IS NULL",
                        (now, "scheduled recall time reached", d["id"]),
                    ).rowcount
                    if not changed:
                        continue
                    cur = conn.execute(
                        "UPDATE leases SET state='fenced', "
                        "fence_reason='scheduled recall time reached', streaming=0, last_seen=? "
                        "WHERE delivery_id=? AND state='active'",
                        (now, d["id"]),
                    )
                    self._log(
                        conn,
                        "delivery",
                        d["id"],
                        "recalled",
                        f"scheduled recall time reached · fenced {cur.rowcount} live lease(s)",
                    )
                out["recalled"].append(d["id"])

            stale_deliv = conn.execute(
                "SELECT d.id, d.source_id FROM deliveries d "
                "WHERE d.expired_at IS NULL AND d.retain_until <= ?",
                (now,),
            ).fetchall()
            for d in stale_deliv:
                with txn(conn):
                    changed = conn.execute(
                        "UPDATE deliveries SET expired_at=? WHERE id=? AND expired_at IS NULL",
                        (now, d["id"]),
                    ).rowcount
                    if not changed:
                        continue
                    self._log(
                        conn,
                        "delivery",
                        d["id"],
                        "retention_elapsed",
                        f"{d['source_id']} retention window ended",
                    )
                out["expired"].append(d["id"])

            stale_leases = conn.execute(
                "SELECT id, digest, bytes_sent, delivery_id FROM leases "
                "WHERE state='active' AND expires_at <= ?",
                (now,),
            ).fetchall()
            for lease in stale_leases:
                with txn(conn):
                    changed = conn.execute(
                        "UPDATE leases SET state='expired', streaming=0, finished_at=?, "
                        "fence_reason='lease expired without a heartbeat' "
                        "WHERE id=? AND state='active'",
                        (now, lease["id"]),
                    ).rowcount
                    if not changed:
                        continue
                    self._log(
                        conn,
                        "lease",
                        lease["id"],
                        "lease_expired",
                        f"no heartbeat before deadline · {lease['bytes_sent']} bytes sent",
                    )
                out["leases_expired"].append(lease["id"])
        return out

    # -- views -----------------------------------------------------------

    @staticmethod
    def delivery_view(row, now: float) -> dict:
        if row["recalled_at"] is not None:
            status = "recalled"
        elif now >= row["retain_until"]:
            status = "expired"
        else:
            status = "valid"
        keys = row.keys()
        return {
            "id": row["id"],
            "digest": row["digest"],
            "source_id": row["source_id"],
            "source_name": row["source_name"] if "source_name" in keys else row["source_id"],
            "color_slot": row["color_slot"] if "color_slot" in keys else 0,
            "receipt_no": row["receipt_no"],
            "filename": row["filename"],
            "declared_media_type": row["declared_media_type"],
            "attestation": row["attestation"],
            "received_at": row["received_at"],
            "retain_until": row["retain_until"],
            "recall_at": row["recall_at"],
            "recalled_at": row["recalled_at"],
            "recall_reason": row["recall_reason"],
            "expired_at": row["expired_at"],
            "status": status,
            "valid": status == "valid",
            "verification": row["verification"],
            "observed_sha256": row["observed_sha256"],
        }

    @staticmethod
    def lease_view(row, now: float) -> dict:
        if row["state"] == "active" and row["expires_at"] <= now:
            state = "expired"
        else:
            state = row["state"]
        keys = row.keys()
        return {
            "id": row["id"],
            "digest": row["digest"],
            "delivery_id": row["delivery_id"],
            "source_id": row["source_id"] if "source_id" in keys else None,
            "source_name": row["source_name"] if "source_name" in keys else None,
            "receipt_no": row["receipt_no"] if "receipt_no" in keys else None,
            "client": row["client"],
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "offset": row["offset"],
            "bytes_sent": row["bytes_sent"],
            "state": state,
            "streaming": bool(row["streaming"]),
            "fence_reason": row["fence_reason"],
            "epoch": row["epoch"],
            "last_seen": row["last_seen"],
            "finished_at": row["finished_at"],
        }

    def _package_view(self, conn, pkg, now: float) -> dict:
        deliveries = conn.execute(
            "SELECT d.*, s.display_name AS source_name, s.color_slot AS color_slot "
            "FROM deliveries d JOIN sources s ON s.id=d.source_id "
            "WHERE d.digest=? ORDER BY d.received_at ASC",
            (pkg["digest"],),
        ).fetchall()
        leases = conn.execute(
            "SELECT l.*, d.source_id AS source_id, d.receipt_no AS receipt_no, "
            "s.display_name AS source_name "
            "FROM leases l JOIN deliveries d ON d.id=l.delivery_id "
            "JOIN sources s ON s.id=d.source_id "
            "WHERE l.digest=? ORDER BY l.created_at DESC LIMIT 12",
            (pkg["digest"],),
        ).fetchall()
        delivery_views = [self.delivery_view(d, now) for d in deliveries]
        valid = [d for d in delivery_views if d["valid"]]
        lease_views = [self.lease_view(l, now) for l in leases]
        active = [l for l in lease_views if l["state"] == "active"]
        physical = (not pkg["tombstone"]) and self.blobs.exists(pkg["digest"])
        missing = (not pkg["tombstone"]) and not physical

        blockers = []
        for d in valid:
            blockers.append(
                {
                    "kind": "reference",
                    "delivery_id": d["id"],
                    "source_id": d["source_id"],
                    "source_name": d["source_name"],
                    "detail": f"{d['source_name']} holds a valid reference until {iso(d['retain_until'])}",
                    "until": d["retain_until"],
                }
            )
        for l in active:
            blockers.append(
                {
                    "kind": "download",
                    "lease_id": l["id"],
                    "delivery_id": l["delivery_id"],
                    "source_id": l["source_id"],
                    "source_name": l["source_name"],
                    "detail": (
                        f"{'streaming' if l['streaming'] else 'lease open'} for "
                        f"{l['client'] or 'unnamed client'} · {l['bytes_sent']} bytes sent · "
                        f"lease ends {iso(l['expires_at'])}"
                    ),
                    "bytes_sent": l["bytes_sent"],
                    "streaming": l["streaming"],
                    "expires_at": l["expires_at"],
                }
            )
        if missing:
            blockers.append(
                {
                    "kind": "missing",
                    "detail": "bytes are missing from the blob store; physical state is inconsistent",
                }
            )

        ready = (not pkg["tombstone"]) and physical and not valid and not active and not missing
        if pkg["tombstone"]:
            disposition = "purged"
        elif missing:
            disposition = "inconsistent"
        elif ready:
            disposition = "cleanup_ready"
        else:
            disposition = "blocked"

        filenames = []
        for d in delivery_views:
            if d["filename"] and d["filename"] not in filenames:
                filenames.append(d["filename"])

        return {
            "digest": pkg["digest"],
            "short_digest": pkg["digest"][:12],
            "size": pkg["size"],
            "media_type": pkg["media_type"],
            "created_at": pkg["created_at"],
            "deleted_at": pkg["deleted_at"],
            "tombstone": bool(pkg["tombstone"]),
            "physical": physical,
            "filenames": filenames,
            "deliveries": delivery_views,
            "valid_references": valid,
            "reference_count": len(valid),
            "leases": lease_views,
            "active_leases": active,
            "cleanup": {
                "ready": ready,
                "disposition": disposition,
                "blockers": blockers,
                "reason": _blocker_sentence(blockers, disposition),
            },
        }

    def snapshot(self, *, event_limit: int = 60) -> dict:
        self.enforce_scheduled()
        now = self.now()
        with self._conn() as conn:
            sources = conn.execute("SELECT * FROM sources ORDER BY color_slot, id").fetchall()
            packages = conn.execute("SELECT * FROM packages ORDER BY created_at DESC").fetchall()
            package_views = [self._package_view(conn, p, now) for p in packages]
            events = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (event_limit,)
            ).fetchall()
            source_views = []
            for s in sources:
                source_views.append(
                    {
                        "id": s["id"],
                        "display_name": s["display_name"],
                        "note": s["note"],
                        "color_slot": s["color_slot"],
                        "deliveries": conn.execute(
                            "SELECT COUNT(*) FROM deliveries WHERE source_id=?", (s["id"],)
                        ).fetchone()[0],
                        "valid_references": conn.execute(
                            "SELECT COUNT(*) FROM deliveries WHERE source_id=? AND recalled_at IS NULL "
                            "AND retain_until > ?",
                            (s["id"], now),
                        ).fetchone()[0],
                    }
                )
            kpis = {
                "physical_blobs": sum(1 for p in package_views if p["physical"]),
                "physical_bytes": sum(p["size"] for p in package_views if p["physical"]),
                "valid_references": sum(p["reference_count"] for p in package_views),
                "blocked_cleanups": sum(
                    1 for p in package_views if p["cleanup"]["disposition"] == "blocked"
                ),
                "cleanup_ready": sum(
                    1 for p in package_views if p["cleanup"]["disposition"] == "cleanup_ready"
                ),
                "purged_packages": sum(1 for p in package_views if p["tombstone"]),
                "active_leases": sum(len(p["active_leases"]) for p in package_views),
                "deliveries_total": sum(len(p["deliveries"]) for p in package_views),
            }
            return {
                "now": now,
                "clock_offset": self.clock_offset,
                "server": {
                    "epoch": self.epoch,
                    "epoch_short": self.epoch[:10],
                    "started_at": self.started_at,
                    "lease_ttl": self.lease_ttl,
                },
                "sources": source_views,
                "packages": package_views,
                "events": [
                    {
                        "id": e["id"],
                        "at": e["at"],
                        "entity": e["entity"],
                        "entity_id": e["entity_id"],
                        "action": e["action"],
                        "detail": e["detail"],
                    }
                    for e in reversed(events)
                ],
                "kpis": kpis,
            }

    # -- cleanup ---------------------------------------------------------

    def cleanup(self, *, dry_run: bool = False) -> dict:
        self.enforce_scheduled()
        now = self.now()
        purged, blocked = [], []
        with self._conn() as conn:
            with txn(conn):
                rows = conn.execute("SELECT * FROM packages WHERE tombstone=0").fetchall()
                for pkg in rows:
                    digest = pkg["digest"]
                    valid = conn.execute(
                        "SELECT d.id, d.source_id, d.retain_until, s.display_name AS source_name "
                        "FROM deliveries d JOIN sources s ON s.id=d.source_id "
                        "WHERE d.digest=? AND d.recalled_at IS NULL AND d.retain_until > ?",
                        (digest, now),
                    ).fetchall()
                    active = conn.execute(
                        "SELECT l.*, d.source_id, s.display_name AS source_name "
                        "FROM leases l JOIN deliveries d ON d.id=l.delivery_id "
                        "JOIN sources s ON s.id=d.source_id "
                        "WHERE l.digest=? AND l.state='active' AND l.expires_at > ?",
                        (digest, now),
                    ).fetchall()
                    if valid or active:
                        blockers = [
                            {
                                "kind": "reference",
                                "source_id": v["source_id"],
                                "source_name": v["source_name"],
                                "detail": f"{v['source_name']} valid reference until {iso(v['retain_until'])}",
                            }
                            for v in valid
                        ] + [
                            {
                                "kind": "download",
                                "lease_id": a["id"],
                                "source_id": a["source_id"],
                                "source_name": a["source_name"],
                                "detail": (
                                    f"{'streaming' if a['streaming'] else 'lease open'} for "
                                    f"{a['client'] or 'unnamed client'} · {a['bytes_sent']} bytes sent"
                                ),
                            }
                            for a in active
                        ]
                        blocked.append(
                            {
                                "digest": digest,
                                "short_digest": digest[:12],
                                "valid_references": len(valid),
                                "active_leases": len(active),
                                "blockers": blockers,
                            }
                        )
                        continue
                    if not self.blobs.exists(digest):
                        conn.execute(
                            "UPDATE packages SET tombstone=1, deleted_at=? WHERE digest=?",
                            (now, digest),
                        )
                        self._blob_event(conn, digest, "missing", "bytes absent during cleanup")
                        self._log(
                            conn, "package", digest, "blob_missing", "no bytes to purge; tombstoned"
                        )
                        continue
                    if dry_run:
                        purged.append(
                            {"digest": digest, "short_digest": digest[:12], "bytes": pkg["size"]}
                        )
                        continue
                    self.blobs.remove(digest)
                    conn.execute(
                        "UPDATE packages SET tombstone=1, deleted_at=? WHERE digest=?",
                        (now, digest),
                    )
                    self._blob_event(
                        conn,
                        digest,
                        "purged",
                        f"no valid references and no active readers · {pkg['size']} bytes",
                    )
                    self._log(
                        conn,
                        "package",
                        digest,
                        "purged",
                        f"{pkg['size']} bytes removed · no valid references · no active readers",
                    )
                    purged.append(
                        {"digest": digest, "short_digest": digest[:12], "bytes": pkg["size"]}
                    )
        return {
            "at": now,
            "dry_run": dry_run,
            "purged": purged,
            "blocked": blocked,
            "purged_bytes": sum(p["bytes"] for p in purged),
        }

    # -- leases ----------------------------------------------------------

    def acquire_lease(
        self,
        delivery_id: str,
        *,
        client: str = "",
        offset: int = 0,
        ttl: float | None = None,
    ) -> dict:
        """Grant a source-scoped read lease after checking current eligibility."""
        self.enforce_scheduled()
        now = self.now()
        client = _clean_text(client, limit=120, field="client")
        try:
            offset = int(offset)
        except (TypeError, ValueError):
            raise Invalid("offset must be an integer") from None
        ttl = float(ttl or self.lease_ttl)
        if not (1.0 <= ttl <= 3600.0):
            raise Invalid("ttl must be between 1 and 3600 seconds")
        with self._conn() as conn:
            with txn(conn):
                row = conn.execute(
                    "SELECT d.*, p.size AS pkg_size, p.tombstone, p.media_type AS pkg_media_type, "
                    "s.display_name AS source_name, s.color_slot AS color_slot "
                    "FROM deliveries d JOIN packages p ON p.digest=d.digest "
                    "JOIN sources s ON s.id=d.source_id WHERE d.id=?",
                    (delivery_id,),
                ).fetchone()
                if not row:
                    raise NotFound("delivery not found")
                if row["recalled_at"] is not None:
                    raise Gone(
                        f"{row['source_name']} recalled this delivery; access under that source is closed",
                        code="delivery_recalled",
                    )
                if now >= row["retain_until"]:
                    raise Gone(
                        f"{row['source_name']} retention window ended at {iso(row['retain_until'])}",
                        code="retention_ended",
                    )
                if row["tombstone"] or not self.blobs.exists(row["digest"]):
                    raise Gone("physical bytes are no longer present", code="blob_purged")
                if offset < 0 or offset > row["pkg_size"]:
                    raise Invalid("offset is outside the package")
                lease_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO leases(id, digest, delivery_id, client, created_at, expires_at, "
                    "offset, bytes_sent, state, streaming, epoch, last_seen) "
                    "VALUES (?,?,?,?,?,?,?,0,'active',0,'',?)",
                    (lease_id, row["digest"], delivery_id, client, now, now + ttl, offset, now),
                )
                self._log(
                    conn,
                    "lease",
                    lease_id,
                    "lease_opened",
                    f"{row['source_name']} · {client or 'unnamed client'} · "
                    f"offset {offset} · ttl {ttl:g}s",
                )
                return {
                    "lease_id": lease_id,
                    "delivery_id": delivery_id,
                    "digest": row["digest"],
                    "source_id": row["source_id"],
                    "source_name": row["source_name"],
                    "receipt_no": row["receipt_no"],
                    "filename": row["filename"],
                    "media_type": row["pkg_media_type"],
                    "size": row["pkg_size"],
                    "offset": offset,
                    "sha256": row["observed_sha256"],
                    "expires_at": now + ttl,
                }

    def stream_chunk(self, lease_id: str, offset: int, limit: int = MAX_CHUNK) -> dict:
        """Serve one verified chunk and renew the lease in the same step.

        Ordering is the safety mechanism: validation, renewal and the disk read
        all happen inside one ``BEGIN IMMEDIATE`` transaction. Cleanup takes the
        same write lock, so a blob cannot disappear between the renewed lease
        and the bytes leaving the store. A stalled reader's lease expires, the
        purge commits, and its next chunk request gets ``410``.
        """
        self.enforce_scheduled()
        now = self.now()
        with self._conn() as conn:
            with txn(conn):
                row = conn.execute(
                    "SELECT l.*, d.source_id, d.receipt_no, d.filename, d.recalled_at, "
                    "d.retain_until, d.observed_sha256, p.size AS pkg_size, p.tombstone, "
                    "s.display_name AS source_name "
                    "FROM leases l JOIN deliveries d ON d.id=l.delivery_id "
                    "JOIN packages p ON p.digest=l.digest "
                    "JOIN sources s ON s.id=d.source_id WHERE l.id=?",
                    (lease_id,),
                ).fetchone()
                if not row:
                    raise NotFound("lease not found")
                if row["state"] != "active":
                    raise Gone(
                        f"lease is {row['state']}: {row['fence_reason'] or 'not active'}",
                        code="lease_inactive",
                    )
                if row["expires_at"] <= now:
                    conn.execute(
                        "UPDATE leases SET state='expired', streaming=0, finished_at=?, "
                        "fence_reason='lease expired before the next chunk' WHERE id=?",
                        (now, lease_id),
                    )
                    self._log(
                        conn,
                        "lease",
                        lease_id,
                        "lease_expired",
                        f"stalled at {row['bytes_sent']} of {row['pkg_size']} bytes",
                    )
                    raise Gone(
                        "lease expired before the next chunk; the download must be re-granted",
                        code="lease_expired",
                    )
                if row["recalled_at"] is not None:
                    conn.execute(
                        "UPDATE leases SET state='fenced', streaming=0, finished_at=?, "
                        "fence_reason='source recalled the delivery' WHERE id=?",
                        (now, lease_id),
                    )
                    raise Gone("source recalled this delivery", code="delivery_recalled")
                if row["tombstone"] or not self.blobs.exists(row["digest"]):
                    raise Gone("physical bytes are no longer present", code="blob_purged")
                if offset < 0 or offset > row["pkg_size"]:
                    raise Invalid("offset is outside the package")
                # Renew first; cleanup waits on the active lease. The read below
                # happens before this transaction commits, so it cannot race a purge.
                conn.execute(
                    "UPDATE leases SET expires_at=?, last_seen=?, streaming=1, epoch=?, "
                    "offset=?, bytes_sent=? WHERE id=?",
                    (
                        now + self.lease_ttl,
                        now,
                        self.epoch,
                        offset,
                        max(row["bytes_sent"], offset),
                        lease_id,
                    ),
                )
                path = self.blobs.path_for(row["digest"])
                try:
                    with open(path, "rb") as fh:
                        fh.seek(offset)
                        data = fh.read(limit)
                except FileNotFoundError:
                    conn.execute(
                        "UPDATE packages SET tombstone=1, deleted_at=? WHERE digest=?",
                        (now, row["digest"]),
                    )
                    self._blob_event(conn, row["digest"], "missing", "vanished during a chunk read")
                    raise Gone(
                        "physical bytes are no longer present", code="blob_purged"
                    ) from None
                size = row["pkg_size"]
                meta = {
                    "digest": row["digest"],
                    "source_id": row["source_id"],
                    "source_name": row["source_name"],
                    "receipt_no": row["receipt_no"],
                    "filename": row["filename"],
                    "sha256": row["observed_sha256"],
                    "size": size,
                }
                next_offset = offset + len(data)
                conn.execute(
                    "UPDATE leases SET bytes_sent=? WHERE id=? AND state='active' "
                    "AND epoch=? AND bytes_sent < ?",
                    (next_offset, lease_id, self.epoch, next_offset),
                )
        return {
            **meta,
            "offset": offset,
            "next": next_offset,
            "data": data,
            "eof": next_offset >= meta["size"],
        }

    def heartbeat(self, lease_id: str, bytes_sent: int) -> None:
        """Renew a live lease. Raises Gone if it was fenced, expired or purged."""
        now = self.now()
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE leases SET last_seen=?, expires_at=?, bytes_sent=? "
                "WHERE id=? AND state='active' AND streaming=1 AND epoch=?",
                (now, now + self.lease_ttl, int(bytes_sent), lease_id, self.epoch),
            )
            if cur.rowcount:
                return
            row = conn.execute(
                "SELECT state, fence_reason FROM leases WHERE id=?", (lease_id,)
            ).fetchone()
            if not row:
                raise Gone("lease disappeared", code="lease_gone")
            raise Gone(
                f"lease {row['state']}: {row['fence_reason'] or 'no longer active'}",
                code="lease_fenced",
            )

    def finish_stream(
        self, lease_id: str, *, state: str, bytes_sent: int, detail: str = ""
    ) -> None:
        if state not in ("released", "aborted"):
            raise Invalid("finish state must be released or aborted")
        now = self.now()
        with self._conn() as conn:
            with txn(conn):
                row = conn.execute("SELECT * FROM leases WHERE id=?", (lease_id,)).fetchone()
                if not row:
                    return
                if row["state"] == "active":
                    conn.execute(
                        "UPDATE leases SET state=?, streaming=0, bytes_sent=?, finished_at=?, "
                        "last_seen=? WHERE id=?",
                        (state, int(bytes_sent), now, now, lease_id),
                    )
                    self._log(
                        conn,
                        "lease",
                        lease_id,
                        "stream_released" if state == "released" else "stream_aborted",
                        f"{bytes_sent} bytes sent" + (f" · {detail}" if detail else ""),
                    )
                elif state == "aborted" and row["state"] in ("fenced", "expired"):
                    conn.execute(
                        "UPDATE leases SET streaming=0, finished_at=? WHERE id=?",
                        (now, lease_id),
                    )

    def lease_view_by_id(self, lease_id: str) -> dict:
        now = self.now()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT l.*, d.source_id, d.receipt_no, s.display_name AS source_name "
                "FROM leases l JOIN deliveries d ON d.id=l.delivery_id "
                "JOIN sources s ON s.id=d.source_id WHERE l.id=?",
                (lease_id,),
            ).fetchone()
            if not row:
                raise NotFound("lease not found")
            return self.lease_view(row, now)


def _blocker_sentence(blockers: list, disposition: str) -> str:
    if disposition == "purged":
        return "Bytes are purged. The package remains as a tombstone for provenance."
    if disposition == "inconsistent":
        return "Bytes are missing from disk; the ledger can neither serve nor clean this package."
    if disposition == "cleanup_ready":
        return "No valid references and no active readers. Bytes are eligible for physical cleanup."
    refs = [b for b in blockers if b["kind"] == "reference"]
    downloads = [b for b in blockers if b["kind"] == "download"]
    parts = []
    if refs:
        names = ", ".join(b["source_name"] for b in refs)
        parts.append(f"{len(refs)} valid reference{'s' if len(refs) != 1 else ''} ({names})")
    if downloads:
        parts.append(f"{len(downloads)} active download{'s' if len(downloads) != 1 else ''}")
    return "Cleanup blocked by " + " and ".join(parts) + "."
