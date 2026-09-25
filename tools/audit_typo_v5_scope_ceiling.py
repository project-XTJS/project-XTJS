#!/usr/bin/env python3
"""Measure model-independent DEV recall ceilings of the fixed v5 scope gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import jieba

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.service.typo_runtime.contract import protected_spans
from app.service.typo_runtime.v5 import HANZI, ShapeSoundGate


def audit(rows: list[dict], gate: ShapeSoundGate, glyph_threshold: float, example_limit: int = 12):
    if {row.get("split") for row in rows} != {"dev"}:
        raise ValueError("scope_ceiling_dev_only")
    if any(row.get("scope_review_status") not in ("agent_reviewed", "human_confirmed") for row in rows):
        raise ValueError("scope_review_incomplete")
    counts = Counter()
    examples: dict[str, list[dict]] = {}
    for row in rows:
        text, target = row["text"], row["target"]
        target_tokens = list(jieba.tokenize(target))
        known_source_positions = set()
        for token, begin, end in jieba.tokenize(text):
            if len(token) >= 2 and jieba.dt.FREQ.get(token, 0):
                known_source_positions.update(range(begin, end))
        spans = protected_spans(text)
        for edit in row["expected"]:
            counts["gold_spelling"] += 1
            start = int(edit["start"])
            token = next(((word, a, b) for word, a, b in target_tokens if a <= start < b), None)
            source_word = text[token[1]:token[2]] if token else None
            target_word = token[0] if token else None
            if any(a <= start < b for a, b in spans):
                reason = "protected"
            elif not token or not (2 <= len(target_word) <= 8 and all(HANZI.fullmatch(c) for c in target_word)
                                   and jieba.dt.FREQ.get(target_word, 0)):
                reason = "invalid_target_word"
            elif jieba.dt.FREQ.get(source_word, 0):
                reason = "source_in_jieba"
            elif start in known_source_positions:
                reason = "source_token_prefilter"
            else:
                similarity = gate.evidence(text[start], edit["replacement"])
                if similarity is None or (
                    similarity["similarity_type"] not in ("pinyin", "both")
                    and float(similarity.get("glyph_similarity") or 0) < glyph_threshold
                ):
                    reason = "shape_sound_rejected"
                else:
                    reason = "structurally_eligible"
            counts[reason] += 1
            if reason != "structurally_eligible" and len(examples.setdefault(reason, [])) < example_limit:
                examples[reason].append({
                    "project_id": row["project_id"], "start": start,
                    "original_word": source_word, "replacement_word": target_word,
                    "context": text[max(0, start - 12):min(len(text), start + 13)],
                })
    return {
        "counts": dict(counts),
        "max_possible_recall_before_models_and_duplicate_alignment":
            counts["structurally_eligible"] / counts["gold_spelling"] if counts["gold_spelling"] else 0,
        "examples": examples,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dev_scope", type=Path)
    parser.add_argument("--font", required=True, type=Path)
    parser.add_argument("--glyph-threshold", type=float, default=0.55)
    parser.add_argument("--example-limit", type=int, default=12)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("审计报告已存在，拒绝覆盖")
    jieba.initialize()
    gate = ShapeSoundGate(args.font, glyph_threshold=0)
    rows = [json.loads(line) for line in args.dev_scope.open(encoding="utf-8") if line.strip()]
    report = audit(rows, gate, args.glyph_threshold, args.example_limit)
    report.update({
        "scope_sha256": hashlib.sha256(args.dev_scope.read_bytes()).hexdigest(),
        "font_sha256": hashlib.sha256(args.font.read_bytes()).hexdigest(),
        "glyph_threshold": args.glyph_threshold,
        "scope_review_status": "agent_reviewed_not_human_confirmed",
        "model_predictions_used": False,
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("counts", "max_possible_recall_before_models_and_duplicate_alignment")},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
