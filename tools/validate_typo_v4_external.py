#!/usr/bin/env python3
"""Check provenance and sample-size prerequisites for the de-identified bid benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from tools.evaluate_duplicate_typo_candidates import load_records


def validate(path: Path):
    rows = load_records(path)
    if {row["split"] for row in rows} != {"external"}:
        raise ValueError("标书外部集必须全部标注 split=external")
    errors = correct_characters = 0
    projects = set()
    fingerprints = set()
    for row in rows:
        projects.add(row["project_id"])
        normalized = "".join(row["text"].split())
        fingerprint = hashlib.sha256(normalized.encode()).hexdigest()
        if fingerprint in fingerprints:
            raise ValueError("标书外部集有重复文本，不得重复计数")
        fingerprints.add(fingerprint)
        if (
            row.get("pdf_ocr_checked") is not True
            or not row.get("human_verified_by")
            or not row.get("pdf_sha256")
            or not row.get("ocr_sha256")
            or not isinstance(row.get("pdf_page"), int)
            or row["pdf_page"] < 1
        ):
            raise ValueError("每条标书样本必须记录人工 PDF/OCR 核对、审核人、文件哈希和页码")
        errors += len(row["expected"])
        if not row["expected"]:
            correct_characters += len(row["text"])
    return {
        "records": len(rows), "projects": len(projects), "true_edits": errors,
        "independent_correct_characters": correct_characters,
        "risk_sample_prerequisite_met": errors >= 50 and correct_characters >= 20_000,
        "note": "来源字段是人工核对声明；本工具不代替逐页比对原始 PDF 与 OCR",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("external_jsonl", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.external_jsonl), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
