"""Blob store: content-addressed bytes on disk, atomic and crash-safe.

Writes go to a temporary sibling and are ``os.replace``-d into place, so a
process that dies mid-upload leaves no half-visible blob. Readers re-verify
the digest streamed *from disk* rather than trusting the name.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

CHUNK = 1024 * 1024


class BlobStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp = self.root / ".tmp"
        self.tmp.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def valid_digest(digest: str) -> bool:
        return len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)

    def path_for(self, digest: str) -> Path:
        # Two-level fanout keeps directories small; digest is validated hex.
        if not self.valid_digest(digest):
            raise ValueError("invalid digest")
        return self.root / digest[:2] / digest[2:4] / digest

    def temp_path(self) -> Path:
        fd, name = tempfile.mkstemp(prefix="upload-", suffix=".part", dir=self.tmp)
        os.close(fd)
        return Path(name)

    def install(self, temp: Path, digest: str) -> tuple[Path, bool]:
        """Move ``temp`` into its content address. Returns (dest, created).

        If the destination already exists the freshly received bytes are
        discarded: they are, by definition, identical. The caller still
        records a *separate delivery* for the new source.
        """
        dest = self.path_for(digest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            temp.unlink(missing_ok=True)
            return dest, False
        os.replace(temp, dest)
        return dest, True

    def exists(self, digest: str) -> bool:
        try:
            return self.path_for(digest).is_file()
        except ValueError:
            return False

    def size(self, digest: str) -> int:
        return self.path_for(digest).stat().st_size

    def remove(self, digest: str) -> None:
        self.path_for(digest).unlink(missing_ok=True)

    def hash_file(self, path: Path) -> tuple[str, int]:
        h = hashlib.sha256()
        n = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK)
                if not chunk:
                    break
                n += len(chunk)
                h.update(chunk)
        return h.hexdigest(), n

    def fsync_to_disk(self, path: Path) -> None:
        with open(path, "rb") as fh:
            os.fsync(fh.fileno())

    def iter_blobs(self):
        """Yield every installed blob file (fanout layout only)."""
        for first in sorted(self.root.iterdir()):
            if not first.is_dir() or first.name.startswith("."):
                continue
            for second in sorted(first.iterdir()):
                if not second.is_dir():
                    continue
                for blob in sorted(second.iterdir()):
                    if blob.is_file():
                        yield blob

    def scrub_temp(self) -> int:
        n = 0
        for part in self.tmp.glob("*.part"):
            part.unlink(missing_ok=True)
            n += 1
        return n
