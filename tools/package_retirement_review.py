#!/usr/bin/env python3
"""Build a source-only, reproducible review package from a pinned Git base.

Run only on a clean Git checkout. This never accesses runtime data, credentials,
Dhan, Telegram, systemd, or the running bots.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
from pathlib import Path
import re
import subprocess
import zipfile


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL)


def build(base: str, output: Path) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("Exact 40-character baseline SHA required")
    root = Path(git("rev-parse", "--show-toplevel").decode().strip()).resolve()
    head = git("rev-parse", "HEAD").decode().strip()
    if git("status", "--porcelain"):
        raise ValueError("Dirty tree: review ZIP would not match GitHub")
    git("cat-file", "-e", base + "^{commit}")
    changes = git("diff", "--no-renames", "--name-status", "-z", base, head).split(b"\x00")
    manifest, sums, deleted = [], [], []
    files = {}
    index = 0
    while index < len(changes):
        if not changes[index]:
            break
        status = changes[index].decode("ascii")
        path = changes[index + 1].decode("utf8")
        index += 2
        rel = Path(path)
        if (rel.is_absolute() or ".." in rel.parts or not path or
                any(part in (".env", "data", "secrets") for part in rel.parts) or
                any(path.endswith(s) for s in (".db", ".sqlite", ".session", ".pem", ".key"))):
            raise ValueError("Unsafe review path detected")
        manifest.append(f"{status}\t{path}")
        if status == "D":
            deleted.append(path)
        elif status in ("A", "M", "T"):
            target = root / rel
            if not target.is_file() or target.is_symlink():
                raise ValueError(f"Missing or linked changed source: {path}")
            content = target.read_bytes()
            files[path] = content
            sums.append(f"{sha256(content).hexdigest()}  {path}")
        else:
            raise ValueError(f"Unexpected diff status: {status}")
    patch = git("diff", "--no-ext-diff", "--no-renames", "--binary", base, head)
    info = {
        "BASE_SHA.txt": (base + "\n").encode(),
        "HEAD_SHA.txt": (head + "\n").encode(),
        "CHANGES.tsv": ("status\tpath\n" + "\n".join(manifest) + "\n").encode(),
        "DELETED_FILES.txt": ("\n".join(deleted) + "\n").encode(),
        "SHA256SUMS.txt": ("\n".join(sums) + "\n").encode(),
        "UNIFIED.patch": patch,
        "REVIEW_ONLY.txt": (
            "Source-only review; NOT a production backup or deployment installer.\n"
            "No runtime data, Dhan credentials, SQLite, token, or systemd state.\n"
            "Do not merge or deploy without independent review and owner approval.\n"
        ).encode(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=8) as z:
        for path, data in sorted(files.items()):
            z.writestr("changed-files/" + path, data)
        for path, data in sorted(info.items()):
            z.writestr(path, data)
    return {"base": base, "head": head, "files": len(files),
            "deletions": len(deleted), "zip": str(output)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(build(args.base, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
