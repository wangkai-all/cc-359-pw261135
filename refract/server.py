"""Local HTTP front end for the Refract custody ledger.

Stdlib only: ``python3 -m refract.server --data-dir data`` then open the URL.
All packages are local synthetic fixtures; nothing leaves the machine.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sys
import time
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .ledger import Invalid, Ledger, LedgerError, NotFound

STATIC_DIR = Path(__file__).with_name("static")
FIXTURE_DIR = Path(__file__).resolve().parent.parent / "packages"

MAX_JSON_BYTES = 64 * 1024
MAX_CHUNK = 4 * 1024 * 1024


class App:
    def __init__(self, data_dir: str | Path, *, lease_ttl: float = 30.0):
        self.ledger = Ledger(data_dir, lease_ttl=lease_ttl)
        self.data_dir = Path(data_dir)

    def state(self) -> dict:
        snap = self.ledger.snapshot()
        snap["fixtures"] = sorted(p.name for p in FIXTURE_DIR.glob("*") if p.is_file())
        snap["lease_ttl"] = self.ledger.lease_ttl
        return snap


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    app: App
    server_version = "Refract/0.1"
    protocol_version = "HTTP/1.1"

    # -- response helpers ------------------------------------------------

    def send_json(self, obj, status: int = 200) -> None:
        body = _json_bytes(obj)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, status: int, code: str, message: str) -> None:
        self.send_json({"error": {"code": code, "message": message}}, status)

    def send_static(self, path: Path, content_type: str | None = None) -> None:
        if not path.is_file():
            self.send_error_json(404, "not_found", "file not found")
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header(
            "Content-Type",
            content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream",
        )
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_JSON_BYTES:
            raise Invalid("request body too large", status=413, code="too_large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            raise Invalid("body must be valid JSON", code="bad_json") from None
        if not isinstance(value, dict):
            raise Invalid("body must be a JSON object", code="bad_json")
        return value

    def drain_body(self) -> None:
        """Consume any request body so keep-alive framing stays intact."""
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)

    # -- routing ---------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path in ("/", "/index.html"):
                self.send_static(STATIC_DIR / "index.html", "text/html; charset=utf-8")
            elif path == "/styles.css":
                self.send_static(STATIC_DIR / "styles.css", "text/css; charset=utf-8")
            elif path == "/app.js":
                self.send_static(STATIC_DIR / "app.js", "text/javascript; charset=utf-8")
            elif path == "/favicon.svg":
                self.send_static(STATIC_DIR / "favicon.svg", "image/svg+xml")
            elif path == "/api/state":
                self.send_json(self.app.state())
            elif path == "/api/health":
                self.send_json({"ok": True, "epoch": self.app.ledger.epoch})
            elif path == "/api/fixture":
                name = (query.get("name") or [""])[0]
                self.send_static(FIXTURE_DIR / Path(name).name)
            elif path == "/api/leases/status":
                lease_id = (query.get("id") or [""])[0]
                self.send_json(self.app.ledger.lease_view_by_id(lease_id))
            elif re.fullmatch(r"/api/leases/[0-9a-f]{32}/chunk", path):
                lease_id = path.split("/")[3]
                try:
                    offset = int((query.get("offset") or ["0"])[0])
                    limit = int((query.get("limit") or ["262144"])[0])
                except ValueError:
                    raise Invalid("offset and limit must be integers") from None
                limit = max(1, min(limit, MAX_CHUNK))
                self.send_chunk(lease_id, offset, limit)
            else:
                self.send_error_json(404, "not_found", "unknown path")
        except LedgerError as exc:
            self.send_error_json(exc.status, exc.code, str(exc))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # pragma: no cover - defensive
            traceback.print_exc()
            self.send_error_json(500, "internal", str(exc))

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path == "/api/uploads":
                self.handle_upload(query)
            elif re.fullmatch(r"/api/deliveries/[0-9a-f]{32}/recall", path):
                body = self.read_json()
                self.send_json(self.app.ledger.recall(path.split("/")[3], body.get("reason", "")))
            elif re.fullmatch(r"/api/deliveries/[0-9a-f]{32}/end-retention", path):
                body = self.read_json()
                self.send_json(self.app.ledger.end_retention(path.split("/")[3], body.get("note", "")))
            elif path == "/api/cleanup":
                self.drain_body()
                dry = (query.get("dry_run") or ["0"])[0] in ("1", "true", "yes")
                self.send_json(self.app.ledger.cleanup(dry_run=dry))
            elif path == "/api/leases":
                body = self.read_json()
                out = self.app.ledger.acquire_lease(
                    body.get("delivery_id", ""),
                    client=body.get("client", ""),
                    offset=int(body.get("offset", 0) or 0),
                    ttl=body.get("ttl"),
                )
                self.send_json(out, 201)
            elif re.fullmatch(r"/api/leases/[0-9a-f]{32}/heartbeat", path):
                body = self.read_json()
                self.app.ledger.heartbeat(path.split("/")[3], int(body.get("bytes_sent", 0)))
                self.send_json({"ok": True})
            elif re.fullmatch(r"/api/leases/[0-9a-f]{32}/finish", path):
                body = self.read_json()
                self.app.ledger.finish_stream(
                    path.split("/")[3],
                    state=body.get("state", "released"),
                    bytes_sent=int(body.get("bytes_sent", 0)),
                    detail=body.get("detail", ""),
                )
                self.send_json({"ok": True})
            elif path == "/api/maintenance":
                self.drain_body()
                out = self.app.ledger.enforce_scheduled()
                self.send_json(out)
            elif path == "/api/simulate/restart":
                self.drain_body()
                self.simulate_restart()
            elif path == "/api/clock/advance":
                body = self.read_json()
                try:
                    seconds = float(body.get("seconds", 0))
                except (TypeError, ValueError):
                    raise Invalid("seconds must be a number") from None
                now = self.app.ledger.advance_clock(seconds)
                self.send_json({"now": now, "clock_offset": self.app.ledger.clock_offset})
            elif path == "/api/seed-fixtures":
                self.handle_seed()
            else:
                self.send_error_json(404, "not_found", "unknown path")
        except LedgerError as exc:
            self.send_error_json(exc.status, exc.code, str(exc))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # pragma: no cover - defensive
            traceback.print_exc()
            self.send_error_json(500, "internal", str(exc))

    # -- uploads ---------------------------------------------------------

    def handle_upload(self, query: dict) -> None:
        header_len = self.headers.get("Content-Length")
        if header_len is None:
            raise Invalid("Content-Length is required", code="length_required")
        try:
            length = int(header_len)
        except ValueError:
            raise Invalid("Content-Length must be an integer") from None
        if length > 128 * 1024 * 1024:
            raise Invalid("upload exceeds 128 MiB", status=413, code="too_large")
        if query.get("chunked") == ["1"]:
            raise Invalid("send a single Content-Length body", code="unsupported_transfer")
        source = (query.get("source") or [""])[0]
        receipt = (query.get("receipt") or [""])[0]
        retain = (query.get("retain") or [""])[0]
        if not retain:
            raise Invalid("retain is required (seconds)")
        out = self.app.ledger.receive(
            source,
            receipt,
            LimitedReader(self.rfile, length),
            retain_seconds=float(retain),
            recall_after_seconds=(query.get("recall_after") or [None])[0],
            filename=(query.get("filename") or [""])[0],
            media_type=self.headers.get("Content-Type") or (query.get("media_type") or [""])[0],
            attestation=(query.get("attestation") or [""])[0],
            expected_sha256=(query.get("sha256") or [""])[0],
        )
        self.send_json(out, 201)

    def handle_seed(self) -> None:
        """Load a bundled synthetic fixture through the normal receive path."""
        body = self.read_json()
        source = body.get("source", "atlas")
        receipt = body.get("receipt") or f"seed-{uuid.uuid4().hex[:10]}"
        retain = float(body.get("retain_seconds", 3600))
        name = body.get("fixture", "")
        if not name:
            raise Invalid("fixture is required")
        path = FIXTURE_DIR / Path(name).name
        if not path.is_file():
            raise NotFound(f"unknown fixture {name!r}")
        with open(path, "rb") as fh:
            out = self.app.ledger.receive(
                source,
                receipt,
                fh,
                retain_seconds=retain,
                recall_after_seconds=body.get("recall_after_seconds"),
                filename=path.name,
                media_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                attestation=body.get("attestation", ""),
            )
        self.send_json(out, 201)

    # -- streaming -------------------------------------------------------

    def send_chunk(self, lease_id: str, offset: int, limit: int) -> None:
        """Return up to ``limit`` bytes and renew the lease.

        The lease is fenced by recall or lost on TTL; either way this endpoint
        returns 410 and the client stops. Nothing here unlinks the blob, and
        cleanup cannot purge a blob that still has an active lease.
        """
        grant = self.app.ledger.stream_chunk(lease_id, offset, limit)
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(grant["data"])))
        self.send_header("X-Refract-Offset", str(grant["offset"]))
        self.send_header("X-Refract-Next", str(grant["next"]))
        self.send_header("X-Refract-Total", str(grant["size"]))
        self.send_header("X-Refract-Sha256", grant["sha256"])
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(grant["data"])

    # -- helpers ---------------------------------------------------------

    def simulate_restart(self) -> None:
        """Simulate a new server process against the same data directory.

        A real restart builds a fresh Ledger (new epoch) and runs recovery;
        this rotation exercises exactly that path without killing the process.
        """
        self.app.ledger.rotate_epoch()
        recovery = self.app.ledger.recover()
        self.send_json(
            {
                "epoch": self.app.ledger.epoch,
                "epoch_short": self.app.ledger.epoch[:10],
                "recovery": recovery,
            }
        )

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("%s %s\n" % (time.strftime("%H:%M:%S", time.localtime()), fmt % args))


class LimitedReader:
    """File-like adapter that reads exactly ``length`` bytes from a socket."""

    def __init__(self, rfile, length: int):
        self.rfile = rfile
        self.remaining = length

    def read(self, n: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if n is None or n < 0:
            n = self.remaining
        n = min(n, self.remaining)
        data = self.rfile.read(n)
        self.remaining -= len(data)
        return data


def make_server(host: str, port: int, app: App) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"app": app})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    return httpd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refract local custody ledger")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--lease-ttl", type=float, default=30.0)
    args = parser.parse_args(argv)

    app = App(args.data_dir, lease_ttl=args.lease_ttl)
    rec = app.ledger.last_recovery
    httpd = make_server(args.host, args.port, app)
    print(f"Refract listening on http://{args.host}:{httpd.server_port}")
    print(f"  data dir: {Path(args.data_dir).resolve()}")
    print(f"  epoch:    {app.ledger.epoch[:10]}")
    print(f"  recovery: {rec}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
