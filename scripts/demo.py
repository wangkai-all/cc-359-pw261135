"""End-to-end demo from an empty data directory, over real HTTP.

Boots the server in-process on an ephemeral port, then:

  1. uploads the same synthetic fixture twice (Atlas, then Harbor),
  2. reads it back through each source's own lease,
  3. recalls Harbor and proves Atlas still reads the same bytes,
  4. ends Atlas retention, proves a live reader blocks cleanup,
  5. simulates a restart, proves the reader fails closed and cleanup purges,
  6. re-delivers the bytes and proves the blob is resurrected.

Run with:  python3 scripts/demo.py --fresh --data-dir /tmp/refract-demo
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from refract.server import App, make_server  # noqa: E402

FIXTURE = ROOT / "packages" / "telemetry-window.json"


def call(base: str, method: str, path: str, *, body=None, headers=None):
    headers = dict(headers or {})
    data = body
    if isinstance(body, dict):
        data = json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        try:
            parsed = json.loads(payload or b"{}")
        except json.JSONDecodeError:
            parsed = {"raw": payload.decode(errors="replace")}
        return exc.code, parsed


def upload(base: str, source: str, receipt: str, payload: bytes, retain: float):
    status, out = call(
        base,
        "POST",
        f"/api/uploads?source={source}&receipt={receipt}&retain={retain:g}"
        f"&filename={FIXTURE.name}&sha256={hashlib.sha256(payload).hexdigest()}",
        body=payload,
        headers={"Content-Type": "application/json"},
    )
    assert status == 201, out
    return out


def read_all(base: str, delivery_id: str, chunk: int = 2048):
    status, lease = call(base, "POST", "/api/leases", body={"delivery_id": delivery_id, "client": "demo"})
    assert status == 201, lease
    parts, offset = [], 0
    while True:
        request = urllib.request.Request(
            base + f"/api/leases/{lease['lease_id']}/chunk?offset={offset}&limit={chunk}"
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read()
                parts.append(data)
                offset = int(response.headers["X-Refract-Next"])
                total = int(response.headers["X-Refract-Total"])
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read() or b"{}")
            call(base, "POST", f"/api/leases/{lease['lease_id']}/finish", body={"state": "aborted", "bytes_sent": offset})
            return offset, total, None, payload.get("error", {})
        if offset >= total:
            call(base, "POST", f"/api/leases/{lease['lease_id']}/finish", body={"state": "released", "bytes_sent": offset})
            blob = b"".join(parts)
            return offset, total, hashlib.sha256(blob).hexdigest(), None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--fresh", action="store_true", help="delete the data dir first")
    parser.add_argument("--lease-ttl", type=float, default=5.0)
    args = parser.parse_args()

    data_dir = Path(args.data_dir) if args.data_dir else Path(tempfile.mkdtemp(prefix="refract-demo-"))
    if args.fresh and data_dir.exists():
        shutil.rmtree(data_dir)
    if data_dir.exists() and any(data_dir.iterdir()):
        print(f"data dir is not empty: {data_dir}", file=sys.stderr)
        return 2

    app = App(data_dir, lease_ttl=args.lease_ttl)
    httpd = make_server("127.0.0.1", 0, app)
    base = f"http://127.0.0.1:{httpd.server_port}"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"empty data dir: {data_dir}")
    print(f"server:         {base}\n")

    payload = FIXTURE.read_bytes()
    expect = hashlib.sha256(payload).hexdigest()

    a = upload(base, "atlas", "demo-A", payload, retain=3600)
    b = upload(base, "harbor", "demo-H", payload, retain=3600)
    blob_files = [p for p in (data_dir / "blobs").rglob("*") if p.is_file()]
    print("1. duplicate delivery")
    print(f"   atlas   new blob? {a['physical_blob_created']}   harbor shared? {b['duplicate_bytes']}")
    print(f"   physical files on disk: {len(blob_files)}   deliveries: 2\n")
    assert len(blob_files) == 1 and b["duplicate_bytes"]

    n, total, digest, err = read_all(base, a["delivery"]["id"])
    assert err is None and digest == expect, (err, digest)
    print("2. read via atlas")
    print(f"   {n}/{total} bytes, sha256 matches: {digest == expect}\n")

    call(base, "POST", f"/api/deliveries/{b['delivery']['id']}/recall", body={"reason": "demo recall"})
    blocked = call(base, "POST", "/api/leases", body={"delivery_id": b["delivery"]["id"]})[0]
    n2, _, digest2, err2 = read_all(base, a["delivery"]["id"])
    state = call(base, "GET", "/api/state")[1]
    pkg = next(p for p in state["packages"] if p["digest"] == expect)
    print("3. recall only harbor")
    print(f"   harbor lease status: {blocked}   atlas read ok: {digest2 == expect}")
    print(f"   cleanup disposition: {pkg['cleanup']['disposition']} · {pkg['cleanup']['reason']}\n")
    assert blocked == 410 and digest2 == expect and pkg["cleanup"]["disposition"] == "blocked"

    call(base, "POST", f"/api/deliveries/{a['delivery']['id']}/end-retention", body={})
    state = call(base, "GET", "/api/state")[1]
    pkg = next(p for p in state["packages"] if p["digest"] == expect)
    print("4. retention ended, nothing recalled on atlas's side is left")
    print(f"   disposition: {pkg['cleanup']['disposition']} · blockers: "
          f"{[b['kind'] for b in pkg['cleanup']['blockers']]}\n")
    assert pkg["cleanup"]["disposition"] in ("cleanup_ready", "blocked")

    # Open a slow reader on a fresh delivery, then restart while it is live.
    c = upload(base, "orchard", "demo-O", payload, retain=3600)
    status, lease = call(base, "POST", "/api/leases", body={"delivery_id": c["delivery"]["id"], "client": "slow"})
    assert status == 201, lease
    request = urllib.request.Request(base + f"/api/leases/{lease['lease_id']}/chunk?offset=0&limit=1024")
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()
    state = call(base, "GET", "/api/state")[1]
    pkg = next(p for p in state["packages"] if p["digest"] == expect)
    print("5. slow reader active")
    print(f"   blockers: {[b['kind'] for b in pkg['cleanup']['blockers']]}")
    cleanup = call(base, "POST", "/api/cleanup?dry_run=1")[1]
    print(f"   dry-run purge candidates: {len(cleanup['purged'])}   (live reader must block)\n")
    assert any(b["kind"] == "download" for b in pkg["cleanup"]["blockers"])
    assert not cleanup["purged"]

    restart = call(base, "POST", "/api/simulate/restart")[1]
    print("6. restart during the download")
    print(f"   abandoned leases: {len(restart['recovery']['abandoned_leases'])}")
    code, err = call(base, "GET", f"/api/leases/{lease['lease_id']}/chunk?offset=1024&limit=1024")
    print(f"   reader's next chunk: {code} {err.get('error', {}).get('code')}\n")
    assert code == 410

    call(base, "POST", f"/api/deliveries/{c['delivery']['id']}/recall", body={"reason": "demo recall"})
    out = call(base, "POST", "/api/cleanup")[1]
    print("7. cleanup")
    print(f"   purged {len(out['purged'])} package(s) · {out['purged_bytes']} bytes")
    blob_on_disk = any(
        p.is_file() and p.parent.name != ".tmp" for p in (data_dir / "blobs").rglob("*")
    )
    print(f"   blob on disk: {blob_on_disk}\n")
    assert not blob_on_disk
    assert len(out["purged"]) == 1

    d = upload(base, "atlas", "demo-A2", payload, retain=3600)
    print("8. re-delivery after purge")
    print(f"   resurrected: {d['resurrected']}   new receipt: {d['delivery']['receipt_no']}")
    state = call(base, "GET", "/api/state")[1]
    pkg = next(p for p in state["packages"] if p["digest"] == expect)
    print(f"   physical: {pkg['physical']}   deliveries kept as history: {len(pkg['deliveries'])}")
    assert d["resurrected"] and pkg["physical"]

    httpd.shutdown()
    print("\nall demo invariants held")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
