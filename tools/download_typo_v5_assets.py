#!/usr/bin/env python3
"""Fetch pinned Noto Sans SC font and OFL license into a new asset directory."""

import argparse
import hashlib
import json
import tempfile
import urllib.request
from pathlib import Path

ASSETS = {
    "NotoSansSC-wght.ttf": (
        "https://raw.githubusercontent.com/google/fonts/main/ofl/notosanssc/NotoSansSC%5Bwght%5D.ttf",
        "a3041811a78c361b1de50f953c805e0244951c21c5bd412f7232ef0d899af0da",
    ),
    "OFL-LICENSE.txt": (
        "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/LICENSE",
        "6a73f9541c2de74158c0e7cf6b0a58ef774f5a780bf191f2d7ec9cc53efe2bf2",
    ),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("输出目录已存在；不覆盖字体资产")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.output.parent, prefix="v5-assets-") as temporary:
        temporary = Path(temporary)
        for name, (url, expected) in ASSETS.items():
            digest = hashlib.sha256()
            with urllib.request.urlopen(url, timeout=60) as response, (temporary / name).open("wb") as stream:
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    stream.write(block)
                    digest.update(block)
            if digest.hexdigest() != expected:
                raise RuntimeError(f"资源哈希不匹配: {name}")
        manifest = {
            "font": "NotoSansSC-wght.ttf", "font_sha256": ASSETS["NotoSansSC-wght.ttf"][1],
            "license": "SIL Open Font License 1.1", "license_file": "OFL-LICENSE.txt",
            "license_sha256": ASSETS["OFL-LICENSE.txt"][1],
            "sources": {name: url for name, (url, _) in ASSETS.items()},
            "pypinyin_version": "0.55.0",
        }
        (temporary / "asset-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.rename(args.output)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
