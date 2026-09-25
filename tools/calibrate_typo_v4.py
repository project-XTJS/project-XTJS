#!/usr/bin/env python3
"""Select v4 thresholds from frozen public DEV traces only; never reads test labels."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from tools.evaluate_duplicate_typo_candidates import load_records

SUPPORTED = (0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999)
UNSUPPORTED = (0.9, 0.95, 0.97, 0.98, 0.99, 0.995, 0.999, 0.9995, 0.9999, 1.01)


def key(item):
    return int(item["start"]), int(item["end"]), str(item["replacement"])


def summarize(records, supported, unsupported):
    position_tp = edit_tp = predicted = predicted_positions = clean_fp = expected = clean_chars = 0
    cec3_calls = 0
    stage = Counter()
    for row in records:
        text = row["text"]
        gold = {key(item) for item in row["expected"]}
        result = row["result"]
        if not result.get("eval_trace"):
            raise ValueError("需要 TYPO_V4_EVAL_TRACE=true 采集的完整开发集结果")
        if result.get("rule_version") != "duplicate-typo-macbert-cec3-v4":
            raise ValueError("不能用非 v4 结果校准")
        raw = {key(item) for item in result["raw_candidates"]}
        budget = {key(item) for item in result["budget_candidates"]}
        reason_priority = {"verifier_rejected": 0, "cec3_unsupported": 1, "position_invalid": 2,
                           "word_invalid": 3, "eligible_for_calibration": 4}
        reasons = {}
        for item in result["calibration_reasons"]:
            identity = (int(item["start"]), str(item["replacement"]))
            reason = str(item["reason"])
            if reason_priority.get(reason, -1) > reason_priority.get(reasons.get(identity), -1):
                reasons[identity] = reason
        if any(float(item["verifier_score"]) >= min(supported, unsupported) for item in result["budget_candidates"]):
            cec3_calls += 1
        accepted = {
            key(item) for item in result["calibration_candidates"]
            if float(item["verifier_score"]) >= (
                supported if item["verification_method"] == "macbert_verifier_cec3" else unsupported
            )
        }
        for truth in gold:
            if truth not in raw:
                stage["not_nominated"] += 1
            elif truth not in budget:
                stage["budget_skipped"] += 1
            elif truth in accepted:
                stage["confirmed_before_two_side_alignment"] += 1
            else:
                reason = reasons.get((truth[0], truth[2]), "verifier_rejected")
                if reason == "eligible_for_calibration":
                    reason = "verifier_rejected"
                stage[reason] += 1
        expected += len(gold)
        predicted += len(accepted)
        predicted_positions += len({(a, b) for a, b, _ in accepted})
        edit_tp += len(accepted & gold)
        position_tp += len({(a, b) for a, b, _ in accepted} & {(a, b) for a, b, _ in gold})
        if not gold:
            clean_chars += len(text)
            clean_fp += len({(a, b) for a, b, _ in accepted})
    return {
        "supported_threshold": supported, "unsupported_threshold": unsupported,
        "expected_edits": expected, "correct_characters": clean_chars,
        "confirmed_results": predicted, "position_precision": position_tp / predicted_positions if predicted_positions else 0.0,
        "modification_precision": edit_tp / predicted if predicted else 0.0,
        "recall": edit_tp / expected if expected else 0.0,
        "false_positives_per_10k": clean_fp * 10000 / clean_chars if clean_chars else 0.0,
        "estimated_cec3_calling_texts": cec3_calls,
        "missed_stage_counts": dict(stage),
        "two_side_alignment_evaluated": False,
    }


def choose(records, latency_report=None):
    latency_ok = False
    measured_thresholds = None
    if latency_report:
        v3, v4 = float(latency_report["v3_p95_seconds"]), float(latency_report["v4_p95_seconds"])
        measured_thresholds = (
            float(latency_report["supported_threshold"]),
            float(latency_report["unsupported_threshold"]),
        )
        latency_ok = (
            v3 > 0 and v4 <= 1.5 * v3 and latency_report.get("same_evidence") is True
            and latency_report.get("cold_warm_matched") is True
        )
    reports = [summarize(records, a, b) for a in SUPPORTED for b in UNSUPPORTED]
    passing = [r for r in reports if r["position_precision"] >= 0.98
               and r["modification_precision"] >= 0.98
               and r["false_positives_per_10k"] <= 1.0]
    passing.sort(key=lambda r: (-r["recall"], r["estimated_cec3_calling_texts"],
                                -r["supported_threshold"], -r["unsupported_threshold"]))
    accuracy_best = passing[0] if passing else None
    measured = next((r for r in passing if (r["supported_threshold"], r["unsupported_threshold"]) == measured_thresholds), None)
    chosen = measured if latency_report and latency_ok else accuracy_best if not latency_report else None
    return {
        "split": "dev", "searched_combinations": len(reports),
        "selected": chosen, "accuracy_best_before_latency": accuracy_best,
        "next_latency_candidates": [
            {"supported_threshold": r["supported_threshold"], "unsupported_threshold": r["unsupported_threshold"],
             "recall": r["recall"], "estimated_cec3_calling_texts": r["estimated_cec3_calling_texts"]}
            for r in passing[:5]
        ],
        "development_accuracy_gate_passed": bool(accuracy_best),
        "latency_gate_passed": latency_ok,
        "latency_thresholds_match_selected": measured is not None if latency_report else False,
        "ready_for_frozen_test": bool(measured) and latency_ok,
        "note": "单文本开发集不验证重复两侧对齐；最终测试和标书外部检查仍是独立门槛",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dev_trace", type=Path)
    parser.add_argument("--latency-report", type=Path)
    parser.add_argument("--output", type=Path, help="保存版本化开发集校准报告，拒绝覆盖已有文件")
    args = parser.parse_args()
    if args.output and args.output.exists():
        parser.error("校准报告已存在，拒绝覆盖")
    rows = load_records(args.dev_trace)
    if {row["split"] for row in rows} != {"dev"}:
        parser.error("校准只允许公开 dev 集，测试集必须冻结")
    latency = json.loads(args.latency_report.read_text()) if args.latency_report else None
    report = choose(rows, latency)
    report["provenance"] = {
        "records": len(rows),
        "trace_sha256": hashlib.sha256(args.dev_trace.read_bytes()).hexdigest(),
        "verifier_models": sorted({str(row["result"].get("verifier_model") or "") for row in rows}),
        "trace_score_floor": sorted({float(row["result"].get("trace_score_floor") or 0) for row in rows}),
    }
    body = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(body + "\n", encoding="utf-8")
    print(body)


if __name__ == "__main__":
    main()
