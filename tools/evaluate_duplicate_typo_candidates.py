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
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable


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


def _request_part(text: str, *, service_url: str, timeout: float) -> dict[str, Any]:
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
    return result


def request_result(text: str, *, service_url: str, timeout: float) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    review_candidates: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    metadata: dict[str, Any] = {}
    for offset, part in _chunks(text):
        result = _request_part(part, service_url=service_url, timeout=timeout)
        metadata = result
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
    return {
        "status": "completed",
        "issues": issues,
        "review_candidates": review_candidates,
        "candidates": issues + review_candidates,
        "rule_version": metadata.get("rule_version"),
        "word_rule_version": metadata.get("word_rule_version"),
    }


def evaluate_records(
    records: Iterable[dict[str, Any]],
    *,
    split: str = "test",
    service_url: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    expected_positions: set[tuple[int, int, int]] = set()
    expected_edits: set[tuple[int, int, int, str]] = set()
    predicted_positions: set[tuple[int, int, int]] = set()
    predicted_edits: set[tuple[int, int, int, str]] = set()
    candidate_positions: set[tuple[int, int, int]] = set()
    candidate_edits: set[tuple[int, int, int, str]] = set()
    review_candidate_count = 0
    correct_characters = 0
    correct_record_indices: set[int] = set()
    selected = [record for record in records if str(record["split"]) == split]
    if not selected:
        raise ValueError(f"数据集中没有 split={split!r} 的记录")

    for record_index, record in enumerate(selected):
        text = str(record["text"])
        expected = [_edit_key(value) for value in record["expected"]]
        if not expected:
            correct_characters += len(text)
            correct_record_indices.add(record_index)
        for start, end, replacement in expected:
            expected_positions.add((record_index, start, end))
            expected_edits.add((record_index, start, end, replacement))

        result = record.get("result")
        if result is None:
            if not service_url:
                raise ValueError("记录没有 result，且未提供错别字服务地址")
            result = request_result(text, service_url=service_url, timeout=timeout)
        if not isinstance(result, dict) or not isinstance(result.get("issues"), list):
            raise ValueError("评测结果缺少 issues")
        for issue in result["issues"]:
            start, end, replacement = _edit_key(issue)
            if end > len(text):
                raise ValueError("预测位置超过文本长度")
            predicted_positions.add((record_index, start, end))
            predicted_edits.add((record_index, start, end, replacement))
        candidates = result.get("candidates")
        if candidates is None:
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
        "candidate_metrics": {
            "predicted_edits": len(candidate_edits),
            "position_true_positives": candidate_position_tp,
            "modification_true_positives": candidate_modification_tp,
            "position_precision": candidate_position_tp / len(candidate_positions) if candidate_positions else 0.0,
            "modification_precision": candidate_modification_tp / len(candidate_edits) if candidate_edits else 0.0,
            "recall": candidate_modification_tp / len(expected_edits) if expected_edits else 0.0,
            "correct_text_false_positives": candidate_clean_false_positives,
            "false_positives_per_10k": candidate_clean_false_positives * 10_000 / correct_characters if correct_characters else 0.0,
        },
    }


def assess_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "position_precision": metrics["position_precision"] >= DEFAULT_GATE["position_precision"],
        "modification_precision": metrics["modification_precision"] >= DEFAULT_GATE["modification_precision"],
        "false_positives_per_10k": metrics["false_positives_per_10k"] <= DEFAULT_GATE["false_positives_per_10k"],
        "minimum_expected_edits": metrics["expected_edits"] >= DEFAULT_GATE["minimum_expected_edits"],
        "minimum_correct_characters": metrics["correct_characters"] >= DEFAULT_GATE["minimum_correct_characters"],
        "minimum_confirmed_results": metrics["confirmed_results"] >= DEFAULT_GATE["minimum_confirmed_results"],
    }
    return {"passed": all(checks.values()), "checks": checks, "thresholds": dict(DEFAULT_GATE)}


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
    args = parser.parse_args()

    try:
        records = load_records(args.dataset)
        metrics = evaluate_records(
            records,
            split=args.split,
            service_url=args.service_url,
            timeout=args.timeout,
        )
        report = {"metrics": metrics, "gate": assess_gate(metrics)}
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if args.enforce_gate and not report["gate"]["passed"]:
            raise SystemExit(2)
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
