"""SQLite schema and connection helpers.

The ledger deliberately separates concerns:

* ``packages``      — one row per physical byte-store (content-addressed blob).
* ``deliveries``    — one row per source handing us a package. Provenance,
                      declared metadata, retention and recall state live here,
                      never on the blob. Two sources delivering identical bytes
                      create two deliveries and exactly one package.
* ``leases``        — short, fenced download grants. A lease is what keeps a
                      blob's bytes physically alive while a reader is streaming.
* ``blob_events``   — append-only audit trail for physical disposition.
* ``events``        — append-only operational log shown to operators.
* ``meta``          — ledger clock offset, server epoch, cleanup bookkeeping.

A blob is physically deleted only when *no* delivery that references it is
still eligible (each delivery carries its own expiry/recall gate) AND no live
lease covers it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id            TEXT PRIMARY KEY,
    display_name  TEXT NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    color_slot    INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS packages (
    digest        TEXT PRIMARY KEY,          -- sha256 of the bytes
    size          INTEGER NOT NULL,
    media_type    TEXT NOT NULL,
    blob_path     TEXT NOT NULL,             -- relative to the data dir
    created_at    REAL NOT NULL,
    deleted_at    REAL,                      -- set when bytes are purged
    tombstone     INTEGER NOT NULL DEFAULT 0 -- 1 once bytes are gone for good
);

CREATE TABLE IF NOT EXISTS deliveries (
    id                   TEXT PRIMARY KEY,
    digest               TEXT NOT NULL REFERENCES packages(digest),
    source_id            TEXT NOT NULL REFERENCES sources(id),
    receipt_no           TEXT NOT NULL,
    filename             TEXT NOT NULL DEFAULT '',
    declared_media_type  TEXT NOT NULL DEFAULT '',
    attestation          TEXT NOT NULL DEFAULT '',
    received_at          REAL NOT NULL,
    retain_until         REAL NOT NULL,      -- source's requested retention
    recall_at            REAL,               -- source's requested recall time
    recalled_at          REAL,               -- when recall was honoured
    recall_reason        TEXT NOT NULL DEFAULT '',
    expired_at           REAL,               -- when retention was observed ended
    verification         TEXT NOT NULL DEFAULT 'sha256',
    observed_sha256      TEXT NOT NULL,
    UNIQUE (source_id, receipt_no)
);

CREATE INDEX IF NOT EXISTS deliveries_by_digest ON deliveries(digest);
CREATE INDEX IF NOT EXISTS deliveries_by_source ON deliveries(source_id);

CREATE TABLE IF NOT EXISTS leases (
    id            TEXT PRIMARY KEY,
    digest        TEXT NOT NULL REFERENCES packages(digest),
    delivery_id   TEXT NOT NULL REFERENCES deliveries(id),
    client        TEXT NOT NULL DEFAULT '',
    created_at    REAL NOT NULL,
    expires_at    REAL NOT NULL,
    offset        INTEGER NOT NULL DEFAULT 0,     -- resume point for this grant
    bytes_sent    INTEGER NOT NULL DEFAULT 0,
    state         TEXT NOT NULL DEFAULT 'active', -- active|fenced|released|aborted|expired|abandoned
    streaming     INTEGER NOT NULL DEFAULT 0,     -- 1 while a stream owns the lease
    fence_reason  TEXT NOT NULL DEFAULT '',
    epoch         TEXT NOT NULL DEFAULT '',
    last_seen     REAL NOT NULL,
    finished_at   REAL
);

CREATE INDEX IF NOT EXISTS leases_by_digest ON leases(digest);
CREATE INDEX IF NOT EXISTS leases_by_state ON leases(state);
CREATE INDEX IF NOT EXISTS leases_by_delivery ON leases(delivery_id);

CREATE TABLE IF NOT EXISTS blob_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    digest     TEXT NOT NULL,
    at         REAL NOT NULL,
    action     TEXT NOT NULL,                -- stored|reused|resurrected|purged|missing
    detail     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         REAL NOT NULL,
    entity     TEXT NOT NULL,
    entity_id  TEXT NOT NULL,
    action     TEXT NOT NULL,
    detail     TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS events_by_id ON events(id);
"""

MIGRATIONS = {
    "sources.color_slot": "color_slot INTEGER NOT NULL DEFAULT 0",
    "deliveries.filename": "filename TEXT NOT NULL DEFAULT ''",
    "deliveries.declared_media_type": "declared_media_type TEXT NOT NULL DEFAULT ''",
    "deliveries.attestation": "attestation TEXT NOT NULL DEFAULT ''",
    "deliveries.expired_at": "expired_at REAL",
    "leases.streaming": "streaming INTEGER NOT NULL DEFAULT 0",
    "leases.fence_reason": "fence_reason TEXT NOT NULL DEFAULT ''",
    "leases.epoch": "epoch TEXT NOT NULL DEFAULT ''",
    "leases.finished_at": "finished_at REAL",
    "leases.offset": "offset INTEGER NOT NULL DEFAULT 0",
}

MIGRATION_TABLES = {
    "sources": ["color_slot"],
    "deliveries": ["filename", "declared_media_type", "attestation", "expired_at"],
    "leases": ["streaming", "fence_reason", "epoch", "finished_at", "offset"],
}

DEFAULTS = {
    "color_slot": "0",
    "filename": "''",
    "declared_media_type": "''",
    "attestation": "''",
    "expired_at": "NULL",
    "streaming": "0",
    "fence_reason": "''",
    "epoch": "''",
    "finished_at": "NULL",
    "offset": "0",
}


def _apply_migrations(conn: sqlite3.Connection) -> None:
    for table, columns in MIGRATION_TABLES.items():
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for column in columns:
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {MIGRATIONS[f'{table}.{column}']}")
                conn.execute(
                    f"UPDATE {table} SET {column} = {DEFAULTS[column]} WHERE {column} IS NULL"
                )


def connect(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _apply_migrations(conn)
