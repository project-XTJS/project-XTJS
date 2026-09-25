#!/usr/bin/env python3
"""Search original-detector and glyph thresholds on complete, reviewed public DEV only."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.evaluate_duplicate_typo_candidates import load_records
from app.service.typo_runtime.v5 import VERSION

DETECTOR_THRESHOLDS = (0.5, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999)
GLYPH_THRESHOLDS = (0.55, 0.65, 0.75, 0.85, 0.9)


def _key(item):
    return int(item["start"]), int(item["end"]), str(item["replacement"])


def summarize(rows, detector_threshold: float, glyph_threshold: float):
    edits, positions, true_edits, true_positions = set(), set(), set(), set()
    clean_chars = clean_fp = 0
    missed = Counter()
    for index, row in enumerate(rows):
        if row["result"].get("rule_version") != VERSION or not row["result"].get("eval_trace"):
            raise ValueError("需要 v5 的完整开发集 trace")
        gold = {_key(item) for item in row["expected"]}
        true_edits.update((index, *item) for item in gold)
        true_positions.update((index, a, b) for a, b, _ in gold)
        raw = {}
        for item in row["result"].get("raw_candidates") or []:
            key = _key(item)
            previous = raw.get(key)
            if previous is None or float(item.get("detector_score") or -1) > float(previous.get("detector_score") or -1):
                raw[key] = item
        eligible = [item for item in row["result"].get("budget_candidates") or []
                    if float(item["detector_score"]) >= detector_threshold]
        by_chunk = {}
        for item in eligible:
            by_chunk.setdefault(int(item.get("chunk_offset") or 0), []).append(item)
        budget = set()
        for proposals in by_chunk.values():
            proposals.sort(key=lambda item: (-float(item["probability_ratio"]), -float(item["candidate_probability"]), int(item["start"])))
            budget.update(_key(item) for item in proposals[:8])
        compatible = {_key(item) for item in row["result"].get("calibration_candidates") or []
                      if item.get("similarity_type") in ("pinyin", "both")
                      or float(item.get("glyph_similarity") or -1) >= glyph_threshold}
        accepted = budget & compatible
        reasons = {(int(item["start"]), str(item["replacement"])): item["reason"]
                   for item in row["result"].get("calibration_reasons") or []}
        for truth in gold:
            if truth in accepted:
                continue
            if truth not in raw:
                missed["macbert_not_nominated"] += 1
            elif raw[truth].get("detector_score") is None or float(raw[truth]["detector_score"]) < detector_threshold:
                missed["detector_rejected"] += 1
            elif truth not in budget:
                missed["source_or_budget_rejected"] += 1
            else:
                missed[reasons.get((truth[0], truth[2]), "glyph_or_other_rejected")] += 1
        edits.update((index, *item) for item in accepted)
        positions.update((index, a, b) for a, b, _ in accepted)
        if row.get("originally_correct") is True:
            clean_chars += len(row["text"])
            clean_fp += len(accepted)
    tp_edit = len(edits & true_edits)
    return {
        "detector_threshold": detector_threshold, "glyph_threshold": glyph_threshold,
        "expected_edits": len(true_edits), "correct_characters": clean_chars,
        "confirmed_results": len(edits),
        "position_precision": len(positions & true_positions) / len(positions) if positions else 0.0,
        "modification_precision": tp_edit / len(edits) if edits else 0.0,
        "recall": tp_edit / len(true_edits) if true_edits else 0.0,
        "false_positives_per_10k": clean_fp * 10000 / clean_chars if clean_chars else 0.0,
        "missed_stage_counts": dict(missed),
    }


def choose(rows, expected_records: int, latency: dict | None = None):
    if len(rows) != expected_records or not rows or {row["split"] for row in rows} != {"dev"}:
        raise ValueError("开发集轨迹必须完整且仅含 dev")
    if any(row.get("scope_review_status") != "human_confirmed" for row in rows):
        raise ValueError("词内拼写/合法词/不确定分类尚未全部人工复核")
    if any(not isinstance(row.get("result"), dict) or not row["result"].get("eval_trace") for row in rows):
        raise ValueError("开发集轨迹缺少 v5 trace")
    if max(float(row["result"].get("trace_score_floor") if row["result"].get("trace_score_floor") is not None else 1) for row in rows) > min(DETECTOR_THRESHOLDS):
        raise ValueError("检测器 trace 门槛高于待搜索范围")
    if max(float(row["result"].get("glyph_threshold") if row["result"].get("glyph_threshold") is not None else 1) for row in rows) > min(GLYPH_THRESHOLDS):
        raise ValueError("字形 trace 门槛高于待搜索范围")
    grid = [summarize(rows, detector, glyph) for detector in DETECTOR_THRESHOLDS for glyph in GLYPH_THRESHOLDS]
    passing = [item for item in grid if item["position_precision"] >= 0.98
               and item["modification_precision"] >= 0.98 and item["false_positives_per_10k"] <= 1]
    passing.sort(key=lambda item: (-item["recall"], -item["detector_threshold"], -item["glyph_threshold"]))
    best = passing[0] if passing else None
    measurements = latency if isinstance(latency, list) else [latency] if latency else []
    measured = [item for item in passing if any(
        report.get("latency_gate_passed") is True
        and report.get("same_evidence") is True
        and report.get("cold_warm_matched") is True
        and report.get("detector_threshold") is not None
        and report.get("glyph_threshold") is not None
        and float(report["detector_threshold"]) == item["detector_threshold"]
        and float(report["glyph_threshold"]) == item["glyph_threshold"]
        for report in measurements
    )]
    chosen = measured[0] if measured else None
    return {
        "searched_combinations": len(grid), "best": best, "selected": chosen,
        "top_five": passing[:5], "dev_accuracy_passed": bool(best),
        "latency_passed": bool(chosen), "ready_for_frozen_test": bool(chosen),
        "test_split_used": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dev_trace", type=Path)
    parser.add_argument("--expected-dev-records", type=int, required=True)
    parser.add_argument("--latency-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--freeze", type=Path)
    args = parser.parse_args()
    if args.output.exists() or args.freeze and args.freeze.exists():
        parser.error("不允许覆盖现有报告或冻结清单")
    rows = load_records(args.dev_trace)
    latency = json.loads(args.latency_report.read_text(encoding="utf-8")) if args.latency_report else None
    report = choose(rows, args.expected_dev_records, latency)
    report["dev_trace_sha256"] = hashlib.sha256(args.dev_trace.read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.freeze:
        if not report["ready_for_frozen_test"]:
            parser.error("开发集准确率、人工复核或耗时门槛未通过，不能冻结")
        result = rows[0]["result"]
        identities = ["detector_model", "model", "reference_model", "font_sha256", "pinyin_version", "pillow_version"]
        for name in identities:
            if not result.get(name) or any(row["result"].get(name) != result[name] for row in rows):
                parser.error(f"模型或资源版本不完整/不一致: {name}")
        detector_hash = result["detector_model"].split("#", 1)[-1].split("+", 1)[0]
        frozen = {
            "pipeline_version": VERSION, "detector_model_sha256": detector_hash,
            "detector_threshold": report["selected"]["detector_threshold"],
            "glyph_threshold": report["selected"]["glyph_threshold"],
            "font_sha256": result["font_sha256"], "pinyin_version": result["pinyin_version"],
            "pillow_version": result["pillow_version"],
            "macbert_model": result["model"], "cec3_model": result["reference_model"],
            "dev_trace_sha256": report["dev_trace_sha256"],
        }
        args.freeze.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
