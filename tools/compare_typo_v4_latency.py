#!/usr/bin/env python3
"""Two-phase v3/v4 cold/warm p95 replay; never co-reside both GPU services."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


def get_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def preflight(health_url, minimum_free):
    health = get_json(health_url.rstrip("/") + "/health")
    if health.get("state") not in ("ready", "unloaded") or health.get("error"):
        raise RuntimeError("在线服务状态异常，停止离线回放")
    result = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
        capture_output=True, text=True, check=True, timeout=5,
    )
    if min(int(value.strip()) for value in result.stdout.splitlines() if value.strip()) < minimum_free:
        raise RuntimeError("显存余量不足，停止离线回放")


def check(url, text):
    request = urllib.request.Request(
        url.rstrip("/") + "/check",
        data=json.dumps({"text": text}, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"},
    )
    start = time.perf_counter()
    with urllib.request.urlopen(request, timeout=900) as response:
        payload = json.loads(response.read())
    elapsed = time.perf_counter() - start
    if payload.get("status") != "completed":
        raise RuntimeError("回放检查未完成")
    return elapsed, payload


def p95(values):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def measure(rows, service_url, expected_version, health_url, minimum_free):
    service_health = get_json(service_url.rstrip("/") + "/health")
    if service_health.get("rule_version") != expected_version:
        raise ValueError("离线服务管线版本不匹配")
    samples = []
    for row in rows:
        text = row["text"]
        if not isinstance(text, str) or not 0 < len(text) <= 240:
            raise ValueError("耗时回放只接受 1~240 Unicode 字的相同证据块")
        preflight(health_url, minimum_free)
        cold, response = check(service_url, text)
        if response.get("cache_hit") is not False:
            raise RuntimeError("冷缓存不成立；请使用隔离服务的新缓存")
        preflight(health_url, minimum_free)
        warm, response = check(service_url, text)
        if response.get("cache_hit") is not True:
            raise RuntimeError("热缓存不成立")
        samples.append({"text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                        "cold_seconds": cold, "warm_seconds": warm})
    return {
        "rule_version": expected_version, "samples": samples,
        "cold_p95_seconds": p95([item["cold_seconds"] for item in samples]),
        "warm_p95_seconds": p95([item["warm_seconds"] for item in samples]),
        "supported_threshold": service_health.get("verifier_supported_threshold"),
        "unsupported_threshold": service_health.get("verifier_unsupported_threshold"),
    }


def compare(baseline, candidate):
    old = [item["text_sha256"] for item in baseline["samples"]]
    new = [item["text_sha256"] for item in candidate["samples"]]
    if old != new:
        raise ValueError("v3/v4 回放证据或顺序不一致")
    v3 = max(baseline["cold_p95_seconds"], baseline["warm_p95_seconds"])
    v4 = max(candidate["cold_p95_seconds"], candidate["warm_p95_seconds"])
    return {
        "evidence_count": len(old), "same_evidence": True, "cold_warm_matched": True,
        "v3_cold_p95_seconds": baseline["cold_p95_seconds"],
        "v3_warm_p95_seconds": baseline["warm_p95_seconds"],
        "v4_cold_p95_seconds": candidate["cold_p95_seconds"],
        "v4_warm_p95_seconds": candidate["warm_p95_seconds"],
        "v3_p95_seconds": v3, "v4_p95_seconds": v4,
        "supported_threshold": candidate["supported_threshold"],
        "unsupported_threshold": candidate["unsupported_threshold"],
        "latency_gate_passed": v4 <= 1.5 * v3,
        "v3_service_stopped_before_v4": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_jsonl", type=Path)
    parser.add_argument("--phase", choices=("v3-baseline", "v4-candidate"), required=True)
    parser.add_argument("--service-url", required=True)
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--online-health-url", required=True)
    parser.add_argument("--min-gpu-free-mib", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.min_gpu_free_mib <= 0:
        parser.error("显存余量门槛必须为正数")
    if args.output.exists():
        parser.error("输出已存在，拒绝覆盖")
    rows = [json.loads(line) for line in args.evidence_jsonl.read_text().splitlines() if line.strip()]
    if not rows:
        parser.error("证据列表为空")
    version = "duplicate-typo-macbert-cec3-v3" if args.phase == "v3-baseline" else "duplicate-typo-macbert-cec3-v4"
    if args.phase == "v4-candidate" and not args.baseline_report:
        parser.error("v4 阶段必须提供已完成、已卸载 v3 服务的基线报告")
    baseline = None
    if args.phase == "v4-candidate":
        baseline = json.loads(args.baseline_report.read_text())
        if baseline.get("rule_version") != "duplicate-typo-macbert-cec3-v3":
            raise ValueError("基线报告不是 v3")
        if not baseline.get("service_url"):
            raise ValueError("基线报告缺少 v3 隔离服务地址")
        try:
            get_json(str(baseline["service_url"]).rstrip("/") + "/health")
        except urllib.error.HTTPError as exc:
            raise RuntimeError("v3 隔离服务仍可访问但健康检查失败，不可开始 v4") from exc
        except urllib.error.URLError:
            pass
        else:
            raise RuntimeError("v3 隔离服务仍在运行；先卸载它再测试 v4")
    measured = measure(rows, args.service_url, version, args.online_health_url, args.min_gpu_free_mib)
    if args.phase == "v3-baseline":
        result = {**measured, "service_url": args.service_url}
    else:
        result = compare(baseline, measured)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "samples"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
