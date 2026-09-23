#!/usr/bin/env python3
"""Download the pinned public datasets used by the typo training pipeline.

The script intentionally downloads only the three annotated CSCD-NS splits;
the 2M synthetic corpus and unlabeled NLPCC test file are outside this plan.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


REPOSITORIES = {
    "ecspell": {
        "url": "https://github.com/aopolin-lv/ECSpell.git",
        "revision": "1bc423c8eb93fc63efa4722d7b27830cdb8ef443",
    },
    "nlpcc2023": {
        "url": "https://github.com/Arvid-pku/NLPCC2023_Shared_Task8.git",
        "revision": "84e65a1a92973128f29c6867ec008ba08386c089",
    },
}

CSCD_FILES = {
    "train.tsv": {
        "google_drive_id": "1Fm0at3KLNjMFnrB3PO8K0OzxfK4uAj-T",
        "sha256": "d5e3276a556dd890c5d26d82a15e9f7f61fee9d83cea899cb765cc45a2decdbf",
    },
    "dev.tsv": {
        "google_drive_id": "1HFCgGQcrvitTOo6D5RQHSXCDYs9mAX0X",
        "sha256": "68e6525cef92bbcc55b19e30803b210b39c41f042ac4a33dd76e2e05243e4cae",
    },
    "test.tsv": {
        "google_drive_id": "1oDf1iZBod9rvk7T3MNU-ILqTE9X5UhGb",
        "sha256": "81f6b719570472ab6a5ccb82f014ec58bf91cc244db9b70135fd5b824d7583da",
    },
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clone_pinned(target: Path, *, url: str, revision: str) -> None:
    if not target.exists():
        subprocess.run(
            ["git", "clone", "--filter=blob:none", url, str(target)], check=True
        )
    subprocess.run(["git", "-C", str(target), "fetch", "origin", revision], check=True)
    subprocess.run(
        ["git", "-C", str(target), "checkout", "--detach", revision], check=True
    )
    actual = subprocess.check_output(
        ["git", "-C", str(target), "rev-parse", "HEAD"], text=True
    ).strip()
    if actual != revision:
        raise RuntimeError(f"{target.name} revision mismatch: {actual}")


def download_cscd(target: Path) -> None:
    try:
        import gdown
    except ImportError as exc:
        raise RuntimeError(
            "缺少 gdown；请先安装 requirements-typo-training.txt"
        ) from exc
    target.mkdir(parents=True, exist_ok=True)
    for name, metadata in CSCD_FILES.items():
        path = target / name
        if not path.exists() or sha256(path) != metadata["sha256"]:
            gdown.download(
                id=metadata["google_drive_id"], output=str(path), quiet=False
            )
        actual = sha256(path)
        if actual != metadata["sha256"]:
            raise RuntimeError(f"CSCD-NS {name} checksum mismatch: {actual}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()

    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    for name, metadata in REPOSITORIES.items():
        clone_pinned(root / name, **metadata)
    download_cscd(root / "cscd-ns")

    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and ".git" not in path.parts:
            files[str(path.relative_to(root))] = {
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
    manifest = {
        "schema_version": 1,
        "repositories": REPOSITORIES,
        "cscd_source": "https://github.com/nghuyong/cscd-ns",
        "cscd_files": CSCD_FILES,
        "files": files,
    }
    (root / "download-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output_dir": str(root), "files": len(files)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
