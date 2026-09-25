"""Fetch the author-published CEC3 mirror and verify pinned HF-revision content."""

from hashlib import sha256
from pathlib import Path
import argparse
import json


MODEL_ID = "tiannlp/ChineseErrorCorrector3-4B"
HF_REVISION = "e6d757fa285d66b5bd7faa97f93d085dbb51aee4"
EXPECTED_SHA256 = {
    "model-00001-of-00002.safetensors": "28acad4933be8d1d44a8ff3dcc1f2acce3ed9043a36bf9feb32c10af2fe97dc6",
    "model-00002-of-00002.safetensors": "df435b82d09456f24bb55f6e3b1be091a1af1bc970799ac07aea23686af6dd7a",
    "model.safetensors.index.json": "e36bba5af4706cfd22ecf7eedb5fd8f4d2559b92f2414b932651b9ed6495921a",
    "config.json": "4ffa4ed0bdd7c425a2aefb33533044fcfcb9f30e665ec4158c06db22560ca2f7",
    "tokenizer.json": "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
    "tokenizer_config.json": "cd5c1909da4950a610a9e3a6cb6763cff0381d91c6f0838855db8d5ae83d6c9f",
    "chat_template.jinja": "a55ee1b1660128b7098723e0abcd92caa0788061051c62d51cbe87d9cf1974d8",
    "vocab.json": "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910",
    "merges.txt": "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5",
    "added_tokens.json": "c0284b582e14987fbd3d5a2cb2bd139084371ed9acbae488829a1c900833c680",
    "generation_config.json": "787409d8b23543ca6a24affbca01de9251ec48e5b0f268911ca92daabde329bf",
}


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from modelscope.hub.file_download import model_file_download

    args.output.mkdir(parents=True, exist_ok=True)
    for name, expected in EXPECTED_SHA256.items():
        target = args.output / name
        if not target.is_file() or sha256_file(target) != expected:
            downloaded = Path(model_file_download(
                MODEL_ID, name, revision="master", local_dir=str(args.output)
            ))
            if downloaded.resolve() != target.resolve():
                raise RuntimeError(f"Unexpected download path: {downloaded}")
        if sha256_file(target) != expected:
            raise RuntimeError(f"{name} differs from pinned {HF_REVISION} content")
        print(f"verified {name}", flush=True)
    manifest = {
        "model": "twnlp/ChineseErrorCorrector3-4B",
        "revision": HF_REVISION,
        "engine": "transformers-qwen3-cec3",
        "source": "https://www.modelscope.cn/models/tiannlp/ChineseErrorCorrector3-4B",
        "canonical_source": "https://huggingface.co/twnlp/ChineseErrorCorrector3-4B",
        "license": "Apache-2.0",
        "model_sha256": sha256((
            EXPECTED_SHA256["model-00001-of-00002.safetensors"] + "\n"
            + EXPECTED_SHA256["model-00002-of-00002.safetensors"] + "\n"
        ).encode()).hexdigest(),
        "model_sha256_method": "SHA256 of newline-terminated shard SHA256 values in index order",
        "files_sha256": EXPECTED_SHA256,
    }
    manifest_path = args.output / "model-manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
