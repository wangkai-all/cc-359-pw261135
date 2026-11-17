# Refract — custody ledger for duplicated deliveries

Refract is a local, self-contained demonstration of one problem: **different
sources can deliver byte-identical packages, but their retention rules, recall
rights and provenance are not identical.** Physical storage may be shared;
eligibility and source evidence may not.

Everything runs on this machine with synthetic fixtures. No network calls, no
third-party services, no real data.

```bash
python3 -m refract.server --data-dir data        # http://127.0.0.1:8765
python3 -m unittest discover -s tests -v        # 25 invariant tests
```

Requires Python 3.11+ and nothing else. The dashboard is plain HTML/CSS/JS
served by the same process.

## The model

| Layer | Table | Meaning |
|---|---|---|
| Physical bytes | `packages` | One content-addressed blob per sha256. Canonical, no provenance. |
| Delivery | `deliveries` | One row per source receipt: attestation, observed digest, retention window, recall state. Never merged. |
| Reader | `leases` | A short, fenced grant scoped to **one delivery**. Renewed on every chunk. |
| Audit | `events`, `blob_events` | Append-only operational history, including after bytes are gone. |

`deliveries` references `packages`; no provenance column exists on the package
row. That asymmetry is deliberate: two deliveries of the same bytes share
exactly one blob and exactly zero evidence.

## Invariants, and where they are enforced

1. **Shared bytes, separate evidence.** `Ledger.receive` hashes the upload
   first, then inserts the package only if the digest is new. The second
   delivery reuses the file and records its own receipt, filename and
   attestation (`refract/ledger.py`).
2. **Recall is per source.** `Ledger.recall` marks that delivery recalled and
   fences only leases issued under it. A reader holding the same bytes via
   another source is untouched. A new lease for the recalled delivery gets
   `410 delivery_recalled`.
3. **Cleanup needs every gate open.** `Ledger.cleanup` purges a blob only when,
   inside one `BEGIN IMMEDIATE` transaction, it finds zero valid deliveries and
   zero active leases, then unlinks the file before committing. Anything else
   is reported as a blocker: which source still holds a reference, which reader
   is still downloading.
4. **Slow downloads are safe by construction.** Each chunk request validates
   the lease, renews it, and reads the bytes inside the same write transaction
   a purge would need. A slow but live reader keeps its lease; a stalled one
   loses it at TTL, cleanup may then purge, and the stalled reader's next
   request fails `410` instead of yielding a truncated file.
5. **Restart is a first-class event.** A new process gets a new epoch. Recovery
   abandons leases from older epochs (so they stop blocking cleanup), tombstones
   packages whose files vanished, and sweeps dead temp files and stale orphans.
   Provenance and retention are unchanged.
6. **Bytes can come back.** Receiving an already-purged digest resurrects the
   file and adds a fresh delivery; prior tombstones remain visible as history.

## What the page shows

- Five ledger totals: physical blobs, bytes on disk, valid references, blocked
  cleanups, cleanup-ready packages.
- One card per physical file: delivery list (source, receipt, retention, recall
  state), a cleanup gate explaining exactly what is blocking it, and the live
  download leases with progress.
- Per-source actions: download through that source, recall, end retention.
- A guided demo that runs the four hard cases end to end and asserts each
  outcome in the page (green = expected, red = a failed invariant):
  1. the same fixture delivered by Atlas and Harbor lands as one blob and two
     deliveries;
  2. recalling Harbor refuses Harbor's readers while Atlas keeps serving the
     same bytes and cleanup stays blocked by Atlas;
  3. a slow Atlas download has its retention ended mid-stream — cleanup is
     blocked by the reader, then the server restarts, the lease is abandoned,
     and the reader fails closed;
  4. cleanup purges the blob, and re-delivering the same fixture resurrects it
     with a new receipt.

## API sketch

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/uploads?source=&receipt=&retain=&recall_after=&sha256=` | Raw body upload, hashed server-side and compared with the declared digest. |
| `POST` | `/api/seed-fixtures` | Receive a bundled synthetic fixture through the same path. |
| `POST` | `/api/deliveries/{id}/recall` | Honour a source recall; fences that source's leases. |
| `POST` | `/api/deliveries/{id}/end-retention` | Demo shortcut: move that window's end to now. |
| `POST` | `/api/leases` | Acquire a source-scoped lease. |
| `GET` | `/api/leases/{id}/chunk?offset=&limit=` | Verified chunk; renews the lease or returns `410`. |
| `POST` | `/api/leases/{id}/finish` | `released` or `aborted`. |
| `POST` | `/api/cleanup?dry_run=1` | Attempt physical cleanup; reports purged and blocked. |
| `POST` | `/api/simulate/restart` | Rotate the epoch and run recovery. |
| `POST` | `/api/clock/advance` | Move the persisted demo clock (for retention expiry). |
| `GET` | `/api/state` | Full ledger snapshot for the dashboard. |

## Fixtures

`packages/` holds four deterministic synthetic files (field notes, a sample
manifest, a telemetry window, and a 3 MiB binary capture) generated for this
demo. They contain no real observations.

## Layout

```
refract/
  db.py          schema and migrations
  blobstore.py   content-addressed files, atomic install, temp sweeping
  ledger.py      deliveries, recall, leases, cleanup, recovery
  server.py      stdlib HTTP + streaming endpoints
  static/        dashboard (no build step)
packages/        synthetic fixtures
tests/           invariant tests
```
