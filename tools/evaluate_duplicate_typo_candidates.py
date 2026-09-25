"""Evaluate confirmed duplicate-typo results against a grouped JSONL dataset.

Each JSONL row must contain ``project_id``, ``split``, ``text`` and ``expected``.
``expected`` is a list of Unicode code-point edits with ``start``, ``end`` and
``replacement``.  A row may contain a precomputed ``result``; otherwise this
tool calls the typo service's ``/check`` endpoint.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable


DEFAULT_GATE = {
    "position_precision": 0.98,
    "modification_precision": 0.98,
    "false_positives_per_10k": 1.0,
    "minimum_expected_edits": 200,
    "minimum_correct_characters": 100_000,
    "minimum_confirmed_results": 100,
}


def _edit_key(value: dict[str, Any]) -> tuple[int, int, str]:
    start, end = value.get("start"), value.get("end")
    replacement = value.get("replacement")
    if (
        not isinstance(start, int)
        or not isinstance(end, int)
        or not 0 <= start < end
        or not isinstance(replacement, str)
        or not replacement
    ):
        raise ValueError("错字标注必须包含有效的 start、end、replacement")
    return start, end, replacement


def load_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except ValueError as exc:
            raise ValueError(f"第 {line_number} 行不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise ValueError(f"第 {line_number} 行必须是 JSON 对象")
        text = value.get("text")
        if (
            not str(value.get("project_id") or "").strip()
            or not str(value.get("split") or "").strip()
            or not isinstance(text, str)
            or not text
            or not isinstance(value.get("expected"), list)
        ):
            raise ValueError(f"第 {line_number} 行缺少 project_id、split、text 或 expected")
        for expected in value["expected"]:
            start, end, _ = _edit_key(expected)
            if end > len(text):
                raise ValueError(f"第 {line_number} 行标注位置超过文本长度")
        records.append(value)
    if not records:
        raise ValueError("评测集为空")
    validate_group_splits(records)
    return records


def validate_group_splits(records: Iterable[dict[str, Any]]) -> None:
    projects: dict[str, str] = {}
    texts: dict[str, str] = {}
    for record in records:
        split = str(record["split"])
        project = str(record["project_id"])
        old_project_split = projects.setdefault(project, split)
        if old_project_split != split:
            raise ValueError(f"项目 {project} 跨数据集出现，存在内容泄漏")
        normalized = "".join(str(record["text"]).split())
        text_hash = hashlib.sha256(normalized.encode()).hexdigest()
        old_text_split = texts.setdefault(text_hash, split)
        if old_text_split != split:
            raise ValueError("相同重复文本跨数据集出现，存在内容泄漏")


def _chunks(text: str, limit: int = 240, overlap: int = 24):
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            marks = [match.end() for match in re.finditer(r"[。；！？\n]", text[start:end])]
            if marks and marks[-1] >= limit // 2:
                end = start + marks[-1]
        yield start, text[start:end]
        if end == len(text):
            break
        start = max(start + 1, end - overlap)


def _request_part(text: str, *, service_url: str, timeout: float, preflight: Callable[[], None] | None = None, throttle_seconds: float = 0) -> dict[str, Any]:
    if preflight:
        preflight()
    request = urllib.request.Request(
        service_url.rstrip("/") + "/check",
        data=json.dumps({"text": text}, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read())
    except (OSError, ValueError, urllib.error.HTTPError) as exc:
        raise RuntimeError("错别字服务调用失败") from exc
    if not isinstance(result, dict) or result.get("status") != "completed":
        raise RuntimeError("错别字服务未完成检查")
    if throttle_seconds:
        time.sleep(throttle_seconds)
    return result


def request_result(text: str, *, service_url: str, timeout: float, preflight: Callable[[], None] | None = None, throttle_seconds: float = 0) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    review_candidates: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    metadata: dict[str, Any] = {}
    counts = {key: 0 for key in ("candidate_count", "eligible_count", "hidden_count", "budget_skipped_count", "verifier_rejected_count", "cec3_unsupported_count", "word_invalid_count", "position_invalid_count", "detector_rejected_count", "source_valid_rejected_count", "similarity_rejected_count")}
    traces = {key: [] for key in ("raw_candidates", "budget_candidates", "calibration_candidates", "calibration_reasons")}
    for offset, part in _chunks(text):
        result = _request_part(part, service_url=service_url, timeout=timeout, preflight=preflight, throttle_seconds=throttle_seconds)
        metadata = result
        for key in counts:
            counts[key] += int(result.get(key) or 0)
        for key in traces:
            for raw in result.get(key) or []:
                candidate = dict(raw)
                candidate["chunk_offset"] = offset
                for position_key in ("start", "end", "word_start", "word_end", "context_start", "context_end"):
                    if isinstance(candidate.get(position_key), int):
                        candidate[position_key] += offset
                traces[key].append(candidate)
        for raw in result.get("issues") or []:
            issue = dict(raw)
            for key in ("start", "end", "word_start", "word_end", "context_start", "context_end"):
                if isinstance(issue.get(key), int):
                    issue[key] += offset
            key = (int(issue.get("start", -1)), int(issue.get("end", -1)), str(issue.get("replacement") or ""))
            if key in seen:
                continue
            seen.add(key)
            issues.append(issue)
        for raw in result.get("review_candidates") or []:
            candidate = dict(raw)
            for key in ("start", "end", "word_start", "word_end", "context_start", "context_end"):
                if isinstance(candidate.get(key), int):
                    candidate[key] += offset
            key = (
                int(candidate.get("start", -1)),
                int(candidate.get("end", -1)),
                str(candidate.get("replacement") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            review_candidates.append(candidate)
    hides_candidates = str(metadata.get("rule_version") or "").startswith(("duplicate-typo-macbert-cec3-v", "duplicate-typo-original-detector-v")) and not metadata.get("eval_trace")
    return {
        "status": "completed",
        "issues": issues,
        "review_candidates": review_candidates,
        # v3 deliberately hides rejected character suggestions; the visible
        # issues are not a valid proxy for raw MacBERT candidate recall.
        "candidate_metrics_available": not hides_candidates,
        "candidates": None if hides_candidates else traces["raw_candidates"] or issues + review_candidates,
        **traces,
        **counts,
        "rule_version": metadata.get("rule_version"),
        "word_rule_version": metadata.get("word_rule_version"),
        "verifier_model": metadata.get("verifier_model"),
        "detector_model": metadata.get("detector_model"),
        "model": metadata.get("model"),
        "reference_model": metadata.get("reference_model"),
        "font_sha256": metadata.get("font_sha256"),
        "pinyin_version": metadata.get("pinyin_version"),
        "pillow_version": metadata.get("pillow_version"),
        "glyph_threshold": metadata.get("glyph_threshold"),
        "detector_threshold": metadata.get("detector_threshold"),
        "eval_trace": bool(metadata.get("eval_trace")),
        "trace_score_floor": metadata.get("trace_score_floor"),
    }


def evaluate_records(
    records: Iterable[dict[str, Any]],
    *,
    split: str = "test",
    service_url: str | None = None,
    timeout: float = 30.0,
    preflight: Callable[[], None] | None = None,
    throttle_seconds: float = 0,
) -> dict[str, Any]:
    expected_positions: set[tuple[int, int, int]] = set()
    expected_edits: set[tuple[int, int, int, str]] = set()
    predicted_positions: set[tuple[int, int, int]] = set()
    predicted_edits: set[tuple[int, int, int, str]] = set()
    candidate_positions: set[tuple[int, int, int]] = set()
    candidate_edits: set[tuple[int, int, int, str]] = set()
    review_candidate_count = 0
    candidate_metrics_available = True
    candidate_count = eligible_count = hidden_count = 0
    correct_characters = 0
    correct_record_indices: set[int] = set()
    rule_versions: set[str] = set()
    scope_pending_review = 0
    selected = [record for record in records if str(record["split"]) == split]
    if not selected:
        raise ValueError(f"数据集中没有 split={split!r} 的记录")

    for record_index, record in enumerate(selected):
        text = str(record["text"])
        scope_pending_review += sum(not bool(item.get("reviewed")) for item in record.get("scope_annotations") or [])
        expected = [_edit_key(value) for value in record["expected"]]
        if not expected and record.get("originally_correct", True):
            correct_characters += len(text)
            correct_record_indices.add(record_index)
        for start, end, replacement in expected:
            expected_positions.add((record_index, start, end))
            expected_edits.add((record_index, start, end, replacement))

        result = record.get("result")
        if result is None:
            if not service_url:
                raise ValueError("记录没有 result，且未提供错别字服务地址")
            result = request_result(text, service_url=service_url, timeout=timeout, preflight=preflight, throttle_seconds=throttle_seconds)
        if not isinstance(result, dict) or not isinstance(result.get("issues"), list):
            raise ValueError("评测结果缺少 issues")
        if result.get("rule_version"):
            rule_versions.add(str(result["rule_version"]))
        candidate_count += int(result.get("candidate_count") or 0)
        eligible_count += int(result.get("eligible_count") or 0)
        hidden_count += int(result.get("hidden_count") or 0)
        for issue in result["issues"]:
            start, end, replacement = _edit_key(issue)
            if end > len(text):
                raise ValueError("预测位置超过文本长度")
            predicted_positions.add((record_index, start, end))
            predicted_edits.add((record_index, start, end, replacement))
        candidates = result.get("candidates")
        if result.get("candidate_metrics_available") is False:
            candidate_metrics_available = False
            candidates = []
        elif candidates is None:
            candidates = list(result.get("issues") or []) + list(
                result.get("review_candidates") or []
            )
        if not isinstance(candidates, list):
            raise ValueError("评测结果 candidates 格式无效")
        review_candidate_count += len(result.get("review_candidates") or [])
        for candidate in candidates:
            start, end, replacement = _edit_key(candidate)
            if end > len(text):
                raise ValueError("候选位置超过文本长度")
            candidate_positions.add((record_index, start, end))
            candidate_edits.add((record_index, start, end, replacement))

    position_true_positive = len(predicted_positions & expected_positions)
    modification_true_positive = len(predicted_edits & expected_edits)
    position_precision = position_true_positive / len(predicted_positions) if predicted_positions else 0.0
    modification_precision = modification_true_positive / len(predicted_edits) if predicted_edits else 0.0
    recall = modification_true_positive / len(expected_edits) if expected_edits else 0.0
    false_positive_positions = len(predicted_positions - expected_positions)
    correct_text_false_positives = sum(
        1 for record_index, _, _ in predicted_positions if record_index in correct_record_indices
    )
    false_positives_per_10k = (
        correct_text_false_positives * 10_000 / correct_characters if correct_characters else 0.0
    )
    candidate_position_tp = len(candidate_positions & expected_positions)
    candidate_modification_tp = len(candidate_edits & expected_edits)
    candidate_clean_false_positives = sum(
        1 for record_index, _, _ in candidate_positions if record_index in correct_record_indices
    )
    return {
        "split": split,
        "rule_versions": sorted(rule_versions),
        "scope_pending_review": scope_pending_review,
        "scope_human_review_complete": bool(selected) and all(
            "scope_annotations" in row and row.get("scope_review_status") == "human_confirmed"
            for row in selected
        ),
        "records": len(selected),
        "expected_edits": len(expected_edits),
        "correct_characters": correct_characters,
        "confirmed_results": len(predicted_edits),
        "position_true_positives": position_true_positive,
        "modification_true_positives": modification_true_positive,
        "false_positive_positions": false_positive_positions,
        "correct_text_false_positives": correct_text_false_positives,
        "position_precision": position_precision,
        "modification_precision": modification_precision,
        "recall": recall,
        "false_positives_per_10k": false_positives_per_10k,
        "review_candidate_count": review_candidate_count,
        "candidate_count": candidate_count,
        "eligible_count": eligible_count,
        "hidden_count": hidden_count,
        "candidate_metrics": {
            "predicted_edits": len(candidate_edits),
            "position_true_positives": candidate_position_tp,
            "modification_true_positives": candidate_modification_tp,
            "position_precision": candidate_position_tp / len(candidate_positions) if candidate_positions else 0.0,
            "modification_precision": candidate_modification_tp / len(candidate_edits) if candidate_edits else 0.0,
            "recall": candidate_modification_tp / len(expected_edits) if expected_edits else 0.0,
            "correct_text_false_positives": candidate_clean_false_positives,
            "false_positives_per_10k": candidate_clean_false_positives * 10_000 / correct_characters if correct_characters else 0.0,
        } if candidate_metrics_available else None,
    }


def assess_gate(metrics: dict[str, Any], *, baseline_recall: float | None = None, latency_report: dict[str, Any] | None = None) -> dict[str, Any]:
    checks = {
        "position_precision": metrics["position_precision"] >= DEFAULT_GATE["position_precision"],
        "modification_precision": metrics["modification_precision"] >= DEFAULT_GATE["modification_precision"],
        "false_positives_per_10k": metrics["false_positives_per_10k"] <= DEFAULT_GATE["false_positives_per_10k"],
        "minimum_expected_edits": metrics["expected_edits"] >= DEFAULT_GATE["minimum_expected_edits"],
        "minimum_correct_characters": metrics["correct_characters"] >= DEFAULT_GATE["minimum_correct_characters"],
        "minimum_confirmed_results": metrics["confirmed_results"] >= DEFAULT_GATE["minimum_confirmed_results"],
    }
    if "duplicate-typo-macbert-cec3-v4" in metrics.get("rule_versions", []):
        checks["recall_above_v3"] = baseline_recall is not None and metrics["recall"] > baseline_recall
    if "duplicate-typo-original-detector-v5" in metrics.get("rule_versions", []):
        checks["recall_above_v3_same_scope"] = baseline_recall is not None and metrics["recall"] > baseline_recall
        checks["scope_reviewed"] = metrics.get("scope_human_review_complete") is True
        checks["same_evidence_latency"] = bool(
            latency_report and latency_report.get("same_evidence") is True
            and latency_report.get("cold_warm_matched") is True
            and latency_report.get("latency_gate_passed") is True
        )
    return {"passed": all(checks.values()), "checks": checks, "thresholds": dict(DEFAULT_GATE)}


def online_preflight(health_url: str | None, minimum_free_mib: int, online_container: str | None = None) -> int:
    try:
        if online_container:
            response = subprocess.run(
                ["docker", "exec", online_container, "python", "-c",
                 "import json,urllib.request; print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:8090/health',timeout=3))))"],
                capture_output=True, text=True, check=True, timeout=8,
            )
            health = json.loads(response.stdout)
        else:
            with urllib.request.urlopen(health_url.rstrip("/") + "/health", timeout=5) as response:
                health = json.loads(response.read())
        if health.get("state") not in ("ready", "unloaded") or health.get("error"):
            raise RuntimeError("在线服务状态异常，停止离线评测")
        if int(health.get("pending") or 0):
            raise RuntimeError("线上模型正在处理请求，停止离线评测")
        free = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, check=True, timeout=5,
        )
        if min(int(value.strip()) for value in free.stdout.splitlines() if value.strip()) < minimum_free_mib:
            raise RuntimeError("显存余量不足，停止离线评测")
        return int((health.get("metrics") or {}).get("checks") or 0)
    except (OSError, ValueError, subprocess.SubprocessError, urllib.error.URLError) as exc:
        raise RuntimeError("无法确认在线健康或显存余量，停止离线评测") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--split", default="test")
    parser.add_argument(
        "--service-url",
        default=os.environ.get("TYPO_SERVICE_URL"),
        help="记录未包含预计算 result 时使用，例如 http://127.0.0.1:8090",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--enforce-gate", action="store_true")
    parser.add_argument("--save-results", type=Path, help="保存本次回放的逐条结果供离线校准，已有文件拒绝覆盖")
    parser.add_argument("--resume-results", action="store_true", help="仅续跑已有结果文件中尚未执行的记录")
    parser.add_argument("--max-new-records", type=int, help="本次最多新增多少条结果，供低峰吞吐试跑")
    parser.add_argument("--v3-baseline-report", type=Path, help="v4 最终验收须提供同集 v3 基线报告")
    parser.add_argument("--latency-report", type=Path, help="v5 最终验收须提供相同证据和冷热条件的 p95 比较")
    parser.add_argument("--frozen-manifest", type=Path, help="v5 test 集运行前固定模型、词典、字体与门槛")
    parser.add_argument("--by-source", action="store_true", help="按公开来源分层报告；需要预计算结果")
    parser.add_argument("--online-health-url", help="低峰离线回放前检查现网健康")
    parser.add_argument("--online-container", help="宿主机无法直连内部地址时，使用 docker exec 检查线上容器")
    parser.add_argument("--min-gpu-free-mib", type=int, default=0)
    parser.add_argument("--throttle-seconds", type=float, default=0.0)
    args = parser.parse_args()
    if args.resume_results and not args.save_results:
        parser.error("--resume-results 需要 --save-results")
    if args.max_new_records is not None and (args.max_new_records <= 0 or not args.save_results):
        parser.error("--max-new-records 必须为正数并与 --save-results 一起使用")
    if args.throttle_seconds < 0:
        parser.error("throttle-seconds 不能为负")
    if args.online_health_url and args.online_container:
        parser.error("在线健康地址与容器名只能选择一个")
    if (args.online_health_url or args.online_container) and args.min_gpu_free_mib <= 0:
        parser.error("检查线上健康时必须指定正数显存余量")
    preflight = None
    if args.online_health_url or args.online_container:
        initial_online_checks = None
        def preflight():
            nonlocal initial_online_checks
            current_checks = online_preflight(args.online_health_url, args.min_gpu_free_mib, args.online_container)
            if initial_online_checks is None:
                initial_online_checks = current_checks
            elif current_checks != initial_online_checks:
                raise RuntimeError("线上服务出现新请求，暂停离线评测")

    try:
        records = load_records(args.dataset)
        if args.save_results:
            existed = args.save_results.exists()
            if existed and not args.resume_results:
                raise ValueError("结果文件已存在，拒绝覆盖")
            selected = [row for row in records if str(row["split"]) == args.split]
            if not args.service_url and any("result" not in row for row in selected):
                raise ValueError("保存结果需要服务地址或预计算结果")
            completed = []
            if existed:
                completed = [json.loads(line) for line in args.save_results.read_text(encoding="utf-8").splitlines() if line.strip()]
                if len(completed) > len(selected) or any(
                    prior.get("project_id") != row.get("project_id")
                    or prior.get("text") != row.get("text")
                    or prior.get("expected") != row.get("expected")
                    or not isinstance(prior.get("result"), dict)
                    for prior, row in zip(completed, selected)
                ):
                    raise ValueError("已有结果与输入前缀不一致，不可续跑")
            with args.save_results.open("a" if existed else "x", encoding="utf-8") as stream:
                remaining = selected[len(completed):]
                if args.max_new_records is not None:
                    remaining = remaining[:args.max_new_records]
                for row in remaining:
                    current = dict(row)
                    current["result"] = row.get("result") or request_result(row["text"], service_url=args.service_url, timeout=args.timeout, preflight=preflight, throttle_seconds=args.throttle_seconds)
                    stream.write(json.dumps(current, ensure_ascii=False) + "\n")
                    stream.flush()
            records = load_records(args.save_results)
        metrics = evaluate_records(
            records,
            split=args.split,
            service_url=args.service_url,
            timeout=args.timeout,
            preflight=preflight,
            throttle_seconds=args.throttle_seconds,
        )
        baseline_recall = None
        if args.v3_baseline_report:
            baseline = json.loads(args.v3_baseline_report.read_text(encoding="utf-8"))
            baseline_recall = float((baseline.get("metrics") or baseline)["recall"])
        if "duplicate-typo-original-detector-v5" in metrics["rule_versions"] and args.split == "test":
            if not args.frozen_manifest or not args.frozen_manifest.is_file():
                raise ValueError("v5 test 集必须先提供冻结模型与门槛清单")
            if not args.save_results:
                raise ValueError("v5 test 必须保存不可覆盖的逐条结果，避免重复测试")
            frozen = json.loads(args.frozen_manifest.read_text(encoding="utf-8"))
            for row in records:
                result = row.get("result") or {}
                required = {
                    "model": frozen.get("macbert_model"),
                    "reference_model": frozen.get("cec3_model"),
                    "font_sha256": frozen.get("font_sha256"),
                    "pinyin_version": frozen.get("pinyin_version"),
                    "pillow_version": frozen.get("pillow_version"),
                    "detector_threshold": frozen.get("detector_threshold"),
                    "glyph_threshold": frozen.get("glyph_threshold"),
                }
                if any(expected is None or result.get(name) != expected for name, expected in required.items()):
                    raise ValueError("v5 test 服务版本或门槛与冻结清单不一致")
                detector_hash = str(result.get("detector_model") or "").split("#", 1)[-1].split("+", 1)[0]
                if detector_hash != frozen.get("detector_model_sha256"):
                    raise ValueError("v5 test 检测器权重与冻结清单不一致")
        latency = json.loads(args.latency_report.read_text(encoding="utf-8")) if args.latency_report else None
        report = {"metrics": metrics, "gate": assess_gate(metrics, baseline_recall=baseline_recall, latency_report=latency)}
        if args.save_results:
            report["collection"] = {
                "selected_total": len(selected),
                "completed": len(records),
                "partial": len(records) < len(selected),
            }
            if "duplicate-typo-original-detector-v5" in metrics["rule_versions"] and args.split == "test":
                report["gate"]["checks"]["complete_test_collection"] = len(records) == len(selected)
                report["gate"]["passed"] = all(report["gate"]["checks"].values())
        if args.by_source:
            selected = [row for row in records if str(row["split"]) == args.split]
            if any("result" not in row for row in selected):
                raise ValueError("按来源分层报告需要 --save-results 或预计算结果")
            report["by_source"] = {
                source: evaluate_records([row for row in selected if str(row.get("source") or "unknown") == source], split=args.split)
                for source in sorted({str(row.get("source") or "unknown") for row in selected})
            }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if args.enforce_gate and not report["gate"]["passed"]:
            raise SystemExit(2)
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
