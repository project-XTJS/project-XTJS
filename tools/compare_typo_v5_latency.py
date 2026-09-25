#!/usr/bin/env python3
"""Compare isolated v3/v5 cold+warm p95 on identical evidence, sequentially."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.compare_typo_v4_latency import get_json, measure
from app.service.typo_runtime.contract import VERSION as V3_VERSION
from app.service.typo_runtime.v5 import VERSION as V5_VERSION


def compare(v3, v5):
    source = [item["text_sha256"] for item in v3["samples"]]
    target = [item["text_sha256"] for item in v5["samples"]]
    if not source or source != target:
        raise ValueError("v3/v5 证据或顺序不一致")
    old = max(v3["cold_p95_seconds"], v3["warm_p95_seconds"])
    new = max(v5["cold_p95_seconds"], v5["warm_p95_seconds"])
    return {
        "same_evidence": True, "cold_warm_matched": True,
        "evidence_count": len(source), "v3_p95_seconds": old, "v5_p95_seconds": new,
        "v3_cold_p95_seconds": v3["cold_p95_seconds"],
        "v3_warm_p95_seconds": v3["warm_p95_seconds"],
        "v5_cold_p95_seconds": v5["cold_p95_seconds"],
        "v5_warm_p95_seconds": v5["warm_p95_seconds"],
        "latency_gate_passed": old > 0 and new <= 1.5 * old,
        "detector_threshold": v5.get("detector_threshold"),
        "glyph_threshold": v5.get("glyph_threshold"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_jsonl", type=Path)
    parser.add_argument("--phase", choices=("v3-baseline", "v5-candidate"), required=True)
    parser.add_argument("--service-url", required=True)
    parser.add_argument("--online-health-url", required=True)
    parser.add_argument("--min-gpu-free-mib", type=int, required=True)
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.min_gpu_free_mib <= 0:
        parser.error("输出不可覆盖，显存余量门槛必须为正数")
    if args.phase == "v5-candidate" and not args.baseline_report:
        parser.error("v5 比较需要独立 v3 基线报告")
    rows = [json.loads(line) for line in args.evidence_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
    expected = V3_VERSION if args.phase == "v3-baseline" else V5_VERSION
    measured = measure(rows, args.service_url, expected, args.online_health_url, args.min_gpu_free_mib)
    if args.phase == "v5-candidate":
        health = get_json(args.service_url.rstrip("/") + "/health")
        measured["detector_threshold"] = health.get("detector_threshold")
        measured["glyph_threshold"] = health.get("glyph_threshold")
    report = measured if not args.baseline_report else compare(json.loads(args.baseline_report.read_text(encoding="utf-8")), measured)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
