#!/usr/bin/env python3
"""Normalize, de-duplicate and freeze public Chinese typo datasets as JSONL."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable


HAN = re.compile(r"[\u4e00-\u9fff]")


@dataclass
class Record:
    source: str
    source_split: str
    split: str
    row: int
    text: str
    target: str

    @property
    def source_id(self) -> str:
        return f"{self.source}:{self.source_split}:{self.row:06d}"


def normalize_text(value: str) -> str:
    return "".join(value.split())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_three_column(
    path: Path,
    *,
    source: str,
    source_split: str,
    split: str,
) -> Iterable[Record]:
    with path.open(encoding="utf-8-sig") as stream:
        for row, raw in enumerate(stream, 1):
            parts = raw.rstrip("\r\n").split("\t")
            if len(parts) != 3:
                yield Record(source, source_split, split, row, "", "")
                continue
            _, text, target = parts
            yield Record(source, source_split, split, row, text.strip(), target.strip())


def parse_two_column(
    path: Path,
    *,
    source: str,
    source_split: str,
    split: str,
) -> Iterable[Record]:
    with path.open(encoding="utf-8-sig") as stream:
        for row, raw in enumerate(stream, 1):
            parts = raw.rstrip("\r\n").split("\t")
            if len(parts) != 2:
                yield Record(source, source_split, split, row, "", "")
                continue
            text, target = parts
            yield Record(source, source_split, split, row, text.strip(), target.strip())


def edits_for(record: Record) -> list[dict]:
    return [
        {
            "start": index,
            "end": index + 1,
            "original": old,
            "replacement": new,
        }
        for index, (old, new) in enumerate(zip(record.text, record.target))
        if old != new
    ]


def validate(record: Record) -> str | None:
    if not record.text or not record.target:
        return "malformed_or_empty"
    if len(record.text) != len(record.target):
        return "length_change"
    edits = edits_for(record)
    if any(
        not HAN.fullmatch(edit["original"])
        or not HAN.fullmatch(edit["replacement"])
        for edit in edits
    ):
        return "non_han_replacement"
    return None


def simhash(value: str) -> int:
    features = [value[index:index + 3] for index in range(max(1, len(value) - 2))]
    weights = [0] * 64
    for feature in features:
        number = int.from_bytes(
            hashlib.blake2b(feature.encode(), digest_size=8).digest(), "big"
        )
        for bit in range(64):
            weights[bit] += 1 if number & (1 << bit) else -1
    result = 0
    for bit, weight in enumerate(weights):
        if weight >= 0:
            result |= 1 << bit
    return result


class CrossSplitDeduper:
    """Keep higher-priority frozen records and reject exact/near duplicates."""

    def __init__(self) -> None:
        self.exact: dict[str, str] = {}
        self.items: list[tuple[str, int, str]] = []
        self.buckets: dict[tuple[int, int], list[int]] = defaultdict(list)

    def duplicate_reason(self, record: Record) -> str | None:
        values = {normalize_text(record.text), normalize_text(record.target)}
        values.discard("")
        if any(value in self.exact for value in values):
            return "cross_split_exact_duplicate"
        candidate_ids: set[int] = set()
        signatures = [(value, simhash(value)) for value in values]
        for _, signature in signatures:
            for band in range(4):
                candidate_ids.update(
                    self.buckets.get((band, (signature >> (band * 16)) & 0xFFFF), [])
                )
        for candidate_id in candidate_ids:
            old_value, old_signature, _ = self.items[candidate_id]
            for value, signature in signatures:
                if abs(len(value) - len(old_value)) > max(2, int(len(value) * 0.04)):
                    continue
                if (signature ^ old_signature).bit_count() > 3:
                    continue
                if SequenceMatcher(None, value, old_value, autojunk=False).ratio() >= 0.96:
                    return "cross_split_near_duplicate"
        return None

    def add(self, record: Record) -> None:
        for value in {normalize_text(record.text), normalize_text(record.target)}:
            if not value:
                continue
            self.exact[value] = record.split
            signature = simhash(value)
            item_id = len(self.items)
            self.items.append((value, signature, record.split))
            for band in range(4):
                self.buckets[(band, (signature >> (band * 16)) & 0xFFFF)].append(
                    item_id
                )


def record_payload(record: Record) -> dict:
    edits = edits_for(record)
    normalized = normalize_text(record.text)
    return {
        "project_id": record.source_id,
        "group_id": hashlib.sha256(
            (normalize_text(record.text) + "\0" + normalize_text(record.target)).encode()
        ).hexdigest(),
        "split": record.split,
        "source": record.source,
        "source_split": record.source_split,
        "source_row": record.row,
        "text": record.text,
        "target": record.target,
        "expected": edits,
        "is_correct": not edits,
        "normalized_text_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
    }


def collect_records(args: argparse.Namespace) -> tuple[list[Record], dict[str, dict]]:
    inputs = {
        "cscd_train": args.cscd_dir / "train.tsv",
        "cscd_dev": args.cscd_dir / "dev.tsv",
        "cscd_test": args.cscd_dir / "test.tsv",
        "ecspell_law_train": args.ecspell_dir / "law.train",
        "ecspell_law_test": args.ecspell_dir / "law.test",
        "ecspell_odw_train": args.ecspell_dir / "odw.train",
        "ecspell_odw_test": args.ecspell_dir / "odw.test",
        "nlpcc_external": args.nlpcc_file,
    }
    missing = [str(path) for path in inputs.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("缺少数据文件：" + "、".join(missing))

    records: list[Record] = []
    records.extend(parse_three_column(inputs["cscd_test"], source="cscd-ns", source_split="test", split="test"))
    records.extend(parse_two_column(inputs["nlpcc_external"], source="nlpcc2023", source_split="dev", split="test_nlpcc"))
    records.extend(parse_three_column(inputs["ecspell_law_test"], source="ecspell-law", source_split="test", split="test_ecspell_law"))
    records.extend(parse_three_column(inputs["ecspell_odw_test"], source="ecspell-odw", source_split="test", split="test_ecspell_odw"))
    records.extend(parse_three_column(inputs["cscd_dev"], source="cscd-ns", source_split="dev", split="dev"))
    records.extend(parse_three_column(inputs["cscd_train"], source="cscd-ns", source_split="train", split="train"))
    records.extend(parse_three_column(inputs["ecspell_law_train"], source="ecspell-law", source_split="train", split="train"))
    records.extend(parse_three_column(inputs["ecspell_odw_train"], source="ecspell-odw", source_split="train", split="train"))
    metadata = {
        name: {
            "path": str(path.resolve()),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for name, path in inputs.items()
    }
    return records, metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cscd-dir", type=Path, required=True)
    parser.add_argument("--ecspell-dir", type=Path, required=True)
    parser.add_argument("--nlpcc-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    records, input_metadata = collect_records(args)
    rejected = Counter()
    accepted: list[Record] = []
    deduper = CrossSplitDeduper()
    seen_within_split: set[tuple[str, str, str]] = set()
    for record in records:
        reason = validate(record)
        if reason:
            rejected[(record.source, record.source_split, reason)] += 1
            continue
        within_key = (
            record.split,
            normalize_text(record.text),
            normalize_text(record.target),
        )
        if within_key in seen_within_split:
            rejected[(record.source, record.source_split, "within_split_duplicate")] += 1
            continue
        reason = deduper.duplicate_reason(record)
        if reason:
            rejected[(record.source, record.source_split, reason)] += 1
            continue
        seen_within_split.add(within_key)
        deduper.add(record)
        accepted.append(record)

    accepted.sort(key=lambda item: (item.split, item.source, item.source_split, item.row))
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = output_dir / "public_typo_dataset.jsonl"
    with dataset_path.open("w", encoding="utf-8") as stream:
        for record in accepted:
            stream.write(json.dumps(record_payload(record), ensure_ascii=False) + "\n")

    counts = defaultdict(lambda: {"records": 0, "correct": 0, "errors": 0, "characters": 0, "clean_characters": 0})
    for record in accepted:
        key = f"{record.split}:{record.source}"
        item = counts[key]
        edits = edits_for(record)
        item["records"] += 1
        item["correct"] += int(not edits)
        item["errors"] += len(edits)
        item["characters"] += len(record.text)
        item["clean_characters"] += len(record.text) if not edits else 0
    report = {
        "schema_version": 1,
        "dataset": str(dataset_path),
        "dataset_sha256": sha256_file(dataset_path),
        "inputs": input_metadata,
        "counts": dict(sorted(counts.items())),
        "rejected": [
            {"source": key[0], "source_split": key[1], "reason": key[2], "count": count}
            for key, count in sorted(rejected.items())
        ],
        "policies": {
            "only_equal_length_han_substitutions": True,
            "test_precedes_dev_precedes_train": True,
            "near_duplicate_similarity": 0.96,
            "near_duplicate_simhash_hamming": 3,
            "correct_test_characters_only_use_originally_correct_rows": True,
        },
        "licenses": {
            "cscd-ns": "See upstream dataset terms: https://github.com/nghuyong/cscd-ns",
            "ecspell": "MIT repository license; retain upstream attribution",
            "nlpcc2023": "MIT repository license; retain upstream attribution",
        },
    }
    report_path = output_dir / "cleaning-report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
