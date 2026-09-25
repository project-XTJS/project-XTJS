#!/usr/bin/env python3
"""Draft public spelling-scope labels; human review remains mandatory for acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.evaluate_duplicate_typo_candidates import load_records
from tools.train_typo_detector_v5 import scope_labels


def prepare(rows, split: str, reviews: dict[tuple[str, int], str | dict] | None = None,
            review_source: str = "human"):
    if review_source not in ("human", "agent"):
        raise ValueError("invalid_review_source")
    reviews = reviews or {}
    counters = Counter()
    output = []
    used_reviews = set()
    for row in rows:
        if row["split"] != split:
            continue
        text, target = row["text"], row.get("target")
        if not isinstance(target, str):
            raise ValueError("public_row_missing_target")
        if len(text) != len(target):
            counters["unaligned_rows"] += 1
            continue
        labels = scope_labels(row, counters)
        annotations = []
        expected = []
        for start, (original, replacement) in enumerate(zip(text, target)):
            if original == replacement:
                continue
            draft = "spelling" if labels[start] == 1 else "legal_word" if labels[start] == 0 else "uncertain"
            decision = reviews.get((row["project_id"], start))
            reviewed = decision is not None
            if reviewed:
                used_reviews.add((row["project_id"], start))
            category = decision.get("category") if isinstance(decision, dict) else decision or draft
            rationale = decision.get("rationale") if isinstance(decision, dict) else None
            if isinstance(decision, dict) and decision.get("review_source") not in (None, review_source):
                raise ValueError("复核文件来源与 --review-source 不一致")
            if category not in ("spelling", "legal_word", "uncertain"):
                raise ValueError("invalid_review_category")
            if category == "spelling" and draft == "uncertain":
                raise ValueError("不能将未可靠对齐的改动提升为词内错字")
            if category == "spelling" and draft == "legal_word" and rationale != "dictionary_false_positive":
                raise ValueError("词典收录词提升为错字须逐项注明 dictionary_false_positive")
            annotations.append({
                "start": start, "end": start + 1, "original": original,
                "replacement": replacement, "category": category,
                "draft_category": draft, "reviewed": bool(reviewed),
                "review_source": review_source if reviewed else None,
                "review_rationale": rationale,
                "review_method": decision.get("method") if isinstance(decision, dict) else None,
            })
            counters[f"category_{category}"] += 1
            if not reviewed or review_source != "human":
                counters["pending_human_review"] += 1
            if reviewed and review_source == "agent":
                counters["agent_reviewed"] += 1
            if category == "spelling":
                expected.append({"start": start, "end": start + 1, "replacement": replacement})
        output.append({
            **row, "expected": expected, "scope_annotations": annotations,
            "originally_correct": text == target,
            "scope_review_status": (
                "human_confirmed" if all(item["reviewed"] and item["review_source"] == "human" for item in annotations)
                else "agent_reviewed" if all(item["reviewed"] for item in annotations)
                else "pending"
            ),
        })
    if set(reviews) != used_reviews:
        raise ValueError("人工复核文件含未匹配的项目或位置")
    return output, dict(counters)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--split", choices=("dev", "test"), default="dev")
    parser.add_argument("--reviews", type=Path, help="人工复核 JSONL: project_id,start,category")
    parser.add_argument("--review-source", choices=("human", "agent"),
                        help="复核来源；AI 复核不得标记为 human")
    parser.add_argument("--frozen-manifest", type=Path, help="处理 test 时必须提供已冻结模型和阈值清单")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("输出已存在；不允许覆盖冻结数据")
    if args.reviews and not args.review_source:
        parser.error("提供 --reviews 时必须显式指定 --review-source")
    if args.split == "test" and (not args.frozen_manifest or not args.frozen_manifest.is_file()):
        parser.error("test 集只允许在模型与门槛冻结后处理")
    if args.frozen_manifest:
        frozen = json.loads(args.frozen_manifest.read_text(encoding="utf-8"))
        if not all(frozen.get(key) is not None for key in (
            "detector_model_sha256", "detector_threshold", "glyph_threshold",
            "font_sha256", "pinyin_version", "pillow_version",
        )):
            parser.error("冻结清单不完整")
    reviews = {}
    if args.reviews:
        for line in args.reviews.read_text(encoding="utf-8").splitlines():
            if line.strip():
                item = json.loads(line)
                key = (item["project_id"], int(item["start"]))
                if key in reviews:
                    raise ValueError("duplicate_review_key")
                reviews[key] = item
    rows = load_records(args.dataset)
    prepared, counts = prepare(rows, args.split, reviews, review_source=args.review_source or "human")
    if not prepared:
        parser.error("所选 split 为空")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        for row in prepared:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    report = {
        "split": args.split, "rows": len(prepared), "counts": counts,
        "source_sha256": hashlib.sha256(args.dataset.read_bytes()).hexdigest(),
        "output_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "human_review_complete": counts.get("pending_human_review", 0) == 0,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
