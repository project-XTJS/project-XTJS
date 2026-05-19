from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


# =========================
# 可修改参数
# =========================
# 默认项目文件夹。留空时运行脚本会在终端中提示输入。
DEFAULT_PROJECT_FOLDER = "项目优化/2026.5.19出口退税"
# 默认检查项。支持编号字符串，例如 ["1", "3", "6"]。留空时运行脚本会在终端中提示输入。
DEFAULT_CHECKS: list[str] = ["4"]
# 默认原 result JSON 路径。留空时自动在项目文件夹中识别。
DEFAULT_RESULT_JSON = ""
# 默认项目标识。留空时使用文件夹名。
DEFAULT_PROJECT_IDENTIFIER = ""


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.analysis import BidDocumentReviewService, DuplicateCheckService
from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)


def _configure_console_encoding() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8")
            except Exception:
                pass


CHECK_OPTIONS = (
    ("integrity_check", "完整性审查"),
    ("consistency_check", "一致性审查"),
    ("pricing_check", "开标一览表审查"),
    ("itemized_pricing_check", "分项报价表"),
    ("deviation_check", "偏离表审查"),
    ("verification_check", "签字盖章日期检查"),
    ("business_bid_duplicate_check", "商务标查重"),
    ("technical_bid_duplicate_check", "技术标查重"),
    ("personnel_reuse_check", "一人多用"),
    ("typo_check", "错别字检查"),
)

CHECK_LABELS = dict(CHECK_OPTIONS)
CHECK_INDEX = {str(index): code for index, (code, _) in enumerate(CHECK_OPTIONS, start=1)}
CHECK_ALIASES = {
    "integrity": "integrity_check",
    "integrity_check": "integrity_check",
    "完整性": "integrity_check",
    "完整性审查": "integrity_check",
    "consistency": "consistency_check",
    "consistency_check": "consistency_check",
    "一致性": "consistency_check",
    "一致性审查": "consistency_check",
    "pricing": "pricing_check",
    "pricing_check": "pricing_check",
    "开标": "pricing_check",
    "开标一览表": "pricing_check",
    "开标一览表检查": "pricing_check",
    "开标一览表审查": "pricing_check",
    "itemized": "itemized_pricing_check",
    "itemized_pricing": "itemized_pricing_check",
    "itemized_pricing_check": "itemized_pricing_check",
    "分项": "itemized_pricing_check",
    "分项报价表": "itemized_pricing_check",
    "分项报价表检查": "itemized_pricing_check",
    "分项报价表审查": "itemized_pricing_check",
    "deviation": "deviation_check",
    "deviation_check": "deviation_check",
    "偏离": "deviation_check",
    "偏离表": "deviation_check",
    "偏离表检查": "deviation_check",
    "商务标和技术标偏离表检查": "deviation_check",
    "偏离表审查": "deviation_check",
    "verification": "verification_check",
    "verification_check": "verification_check",
    "签章": "verification_check",
    "签字盖章": "verification_check",
    "签字盖章日期": "verification_check",
    "签字盖章日期检查": "verification_check",
    "business_duplicate": "business_bid_duplicate_check",
    "business_bid_duplicate": "business_bid_duplicate_check",
    "business_bid_duplicate_check": "business_bid_duplicate_check",
    "商务标查重": "business_bid_duplicate_check",
    "technical_duplicate": "technical_bid_duplicate_check",
    "technical_bid_duplicate": "technical_bid_duplicate_check",
    "technical_bid_duplicate_check": "technical_bid_duplicate_check",
    "技术标查重": "technical_bid_duplicate_check",
    "personnel": "personnel_reuse_check",
    "personnel_reuse": "personnel_reuse_check",
    "personnel_reuse_check": "personnel_reuse_check",
    "一人多用": "personnel_reuse_check",
    "typo": "typo_check",
    "typo_check": "typo_check",
    "错别字": "typo_check",
    "错别字检查": "typo_check",
}

STATUS_ORDER = {
    "pass": 0,
    "unclear": 1,
    "fail": 2,
    "missing": 3,
}

CHANGE_ORDER = {
    "improved": 0,
    "regressed": 1,
    "changed": 2,
    "unchanged": 3,
    "new_only": 4,
    "missing_in_new": 5,
}

REVIEW_CHECK_CODES = {
    "integrity_check",
    "consistency_check",
    "pricing_check",
    "itemized_pricing_check",
    "deviation_check",
    "verification_check",
}
EXTRA_CHECK_CODES = {
    "business_bid_duplicate_check",
    "technical_bid_duplicate_check",
    "personnel_reuse_check",
    "typo_check",
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按项目文件夹重跑选中的检查项，并与原 result JSON 进行终端对比。"
    )
    parser.add_argument(
        "--folder",
        help="项目文件夹路径；未提供时优先使用 DEFAULT_PROJECT_FOLDER，否则进入终端交互。",
    )
    parser.add_argument(
        "--checks",
        nargs="*",
        help="检查项编号、英文代码或中文名称；未提供时优先使用 DEFAULT_CHECKS，否则进入终端交互。",
    )
    parser.add_argument(
        "--result-json",
        help="原 result JSON 路径；未提供时优先使用 DEFAULT_RESULT_JSON，否则自动在项目文件夹中识别。",
    )
    parser.add_argument(
        "--output-dir",
        help="兼容旧参数，当前版本仅在终端显示结果，不会输出文件。",
    )
    parser.add_argument(
        "--project-identifier",
        help="项目标识；未提供时优先使用 DEFAULT_PROJECT_IDENTIFIER，否则默认使用文件夹名。",
    )
    return parser.parse_args(argv)


def _prompt_for_folder() -> str:
    return input("请输入项目文件夹路径或名称：").strip()


def _prompt_for_checks() -> list[str]:
    print("请选择要执行的检查项（可输入编号、英文代码或中文名称，多个用逗号分隔）：")
    for index, (code, label) in enumerate(CHECK_OPTIONS, start=1):
        print(f"  {index}. {label} ({code})")
    print("  all. 全部检查项")
    return [input("请输入选择：").strip()]


def _tokenize_check_inputs(values: list[str] | None) -> list[str]:
    tokens: list[str] = []
    for value in values or []:
        for part in re.split(r"[\s,，、;；]+", str(value or "").strip()):
            token = part.strip()
            if token:
                tokens.append(token)
    return tokens


def _normalize_selected_checks(values: list[str] | None) -> list[str]:
    tokens = _tokenize_check_inputs(values)
    if not tokens:
        raise ValueError("未提供任何检查项。")
    if any(token.lower() == "all" or token == "全部" for token in tokens):
        return [code for code, _ in CHECK_OPTIONS]

    selected: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        code = CHECK_INDEX.get(token)
        if code is None:
            code = CHECK_ALIASES.get(token.lower()) or CHECK_ALIASES.get(token)
        if code is None:
            raise ValueError(f"不支持的检查项选择：{token}")
        if code in seen:
            continue
        seen.add(code)
        selected.append(code)

    return [
        code
        for code, _ in CHECK_OPTIONS
        if code in seen
    ]


def _resolve_folder(raw_value: str) -> Path:
    if not raw_value:
        raise ValueError("项目文件夹不能为空。")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate = candidate.resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"项目文件夹不存在：{candidate}")
    if not candidate.is_dir():
        raise NotADirectoryError(f"项目文件夹不是目录：{candidate}")
    return candidate


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 根节点必须是对象：{path}")
    return payload


def _iter_result_containers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    containers: list[dict[str, Any]] = []

    def _append_if_dict(value: Any) -> None:
        if isinstance(value, dict):
            containers.append(value)

    _append_if_dict(payload)
    _append_if_dict(payload.get("data"))
    _append_if_dict(payload.get("result"))
    _append_if_dict(payload.get("results"))

    data = payload.get("data")
    if isinstance(data, dict):
        _append_if_dict(data.get("result"))
        _append_if_dict(data.get("results"))

    return containers


def _extract_business_review(payload: dict[str, Any]) -> dict[str, Any] | None:
    for container in _iter_result_containers(payload):
        review = container.get("business_bid_format_review")
        if isinstance(review, dict) and isinstance(review.get("bidders"), list):
            return review
        if (
            container.get("review_type") in {
                "business_bid_format_review",
                "unified_business_review",
                "selected_project_checks_review",
            }
            and isinstance(container.get("bidders"), list)
        ):
            return container
    return None


def _extract_named_result(payload: dict[str, Any], result_key: str) -> dict[str, Any] | None:
    for container in _iter_result_containers(payload):
        result = container.get(result_key)
        if isinstance(result, dict):
            return result
    return None


def _find_original_result_json(base_dir: Path, explicit_path: str | None) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        payload = _load_json(path)
        review = _extract_business_review(payload)
        if review is None:
            raise ValueError(f"指定的 result JSON 中未找到 business_bid_format_review：{path}")
        return path, payload, review

    candidates: list[tuple[int, Path, dict[str, Any], dict[str, Any]]] = []
    for path in sorted(base_dir.glob("*.json")) + sorted(base_dir.glob("*.JSON")):
        try:
            payload = _load_json(path)
        except Exception:
            continue
        review = _extract_business_review(payload)
        if review is None:
            continue
        score = 0
        name = path.stem.lower()
        if "result" in name:
            score += 3
        if "审查结果" in path.stem:
            score += 4
        if "review" in name:
            score += 2
        candidates.append((score, path, payload, review))

    if not candidates:
        raise FileNotFoundError(f"在 {base_dir} 下未找到可用于对比的 result JSON。")

    candidates.sort(key=lambda item: (-item[0], item[1].name))
    _, path, payload, review = candidates[0]
    return path, payload, review


def _selected_check_labels(selected_checks: list[str]) -> list[str]:
    return [f"{CHECK_LABELS.get(code, code)} ({code})" for code in selected_checks]


def _build_local_document_records(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    tender = dataset["tender"]
    tender_meta = tender["meta"]
    tender_content = tender["content"]
    records: list[dict[str, Any]] = []

    for index, bidder in enumerate(dataset["bidders"], start=1):
        business = bidder["business"]
        technical = bidder["technical"]
        business_meta = business["meta"]
        technical_meta = technical["meta"]
        relation_id = index

        records.append(
            {
                "relation_id": relation_id,
                "relation_role": DOCUMENT_TYPE_BUSINESS_BID,
                "identifier_id": business_meta.get("sha256") or f"{bidder['bidder_key']}_business_{index}",
                "document_type": DOCUMENT_TYPE_BUSINESS_BID,
                "file_name": business_meta.get("file_name"),
                "file_url": None,
                "content": business["content"],
                "bidder_key": bidder["bidder_key"],
                "tender_identifier_id": tender_meta.get("sha256") or "tender_local",
                "tender_document_type": DOCUMENT_TYPE_BUSINESS_BID,
                "tender_file_name": tender_meta.get("file_name"),
                "tender_file_url": None,
                "tender_content": tender_content,
            }
        )
        records.append(
            {
                "relation_id": relation_id,
                "relation_role": DOCUMENT_TYPE_TECHNICAL_BID,
                "identifier_id": technical_meta.get("sha256") or f"{bidder['bidder_key']}_technical_{index}",
                "document_type": DOCUMENT_TYPE_TECHNICAL_BID,
                "file_name": technical_meta.get("file_name"),
                "file_url": None,
                "content": technical["content"],
                "bidder_key": bidder["bidder_key"],
                "tender_identifier_id": tender_meta.get("sha256") or "tender_local",
                "tender_document_type": DOCUMENT_TYPE_TECHNICAL_BID,
                "tender_file_name": tender_meta.get("file_name"),
                "tender_file_url": None,
                "tender_content": tender_content,
            }
        )

    return records


def _run_selected_bidder_checks(
    service: UnifiedBusinessReviewService,
    *,
    tender_payload: dict[str, Any],
    tender_meta: dict[str, Any],
    bidder: dict[str, Any],
    selected_checks: list[str],
) -> dict[str, Any]:
    business_payload = bidder["business"]["content"]
    technical_document = bidder.get("technical") or {}
    technical_payload = technical_document.get("content")
    combined_payload = (
        service._merge_bid_documents(business_payload, technical_payload)
        if isinstance(technical_payload, dict)
        else business_payload
    )

    visible_checks: dict[str, Any] = {}
    integrity_check: dict[str, Any] | None = None

    if any(code in selected_checks for code in ("integrity_check", "consistency_check", "verification_check")):
        integrity_check = service._execute_check(
            check_code="integrity_check",
            check_name="商务标完整性审查",
            runner=lambda: service.integrity_checker.check_integrity(tender_payload, business_payload),
            normalizer=service._normalize_integrity,
        )
        if "integrity_check" in selected_checks:
            visible_checks["integrity_check"] = integrity_check

    if "consistency_check" in selected_checks:
        visible_checks["consistency_check"] = service._execute_consistency_check(
            tender_payload=tender_payload,
            business_payload=business_payload,
            integrity_check=integrity_check or {},
        )

    if "pricing_check" in selected_checks:
        visible_checks["pricing_check"] = service._execute_check(
            check_code="pricing_check",
            check_name="报价合理性审查",
            runner=lambda: {
                "self_check": service.reasonableness_checker.check_price_reasonableness(business_payload),
                "tender_limit_check": service.reasonableness_checker.check_bid_price_against_tender_limit(
                    tender_payload,
                    business_payload,
                ),
            },
            normalizer=service._normalize_pricing,
        )

    if "itemized_pricing_check" in selected_checks:
        visible_checks["itemized_pricing_check"] = service._execute_check(
            check_code="itemized_pricing_check",
            check_name="分项报价表审查",
            runner=lambda: service.itemized_checker.check_itemized_logic(
                business_payload,
                tender_text=tender_payload,
            ),
            normalizer=service._normalize_itemized,
        )

    if "deviation_check" in selected_checks:
        visible_checks["deviation_check"] = service._execute_check(
            check_code="deviation_check",
            check_name="偏离条款审查",
            runner=lambda: service.deviation_checker.check_technical_deviation(
                tender_payload,
                combined_payload,
            ),
            normalizer=service._normalize_deviation,
        )

    if "verification_check" in selected_checks:
        verification_check = service._execute_check(
            check_code="verification_check",
            check_name="签字盖章日期审查",
            runner=lambda: service.verification_checker.check_seal_and_date(tender_payload, business_payload),
            normalizer=service._normalize_verification,
        )
        if integrity_check is not None:
            verification_check = service._suppress_integrity_duplicates_in_verification(
                verification_check=verification_check,
                integrity_check=integrity_check,
            )
        visible_checks["verification_check"] = verification_check

    bidder_name = service._extract_bidder_name(visible_checks or {"verification_check": integrity_check or {}}, bidder["bidder_key"])
    issues = service._aggregate_bidder_issues(visible_checks)
    summary = service._summarize_bidder_checks(visible_checks)
    reading_guide = service._build_bidder_reading_guide(
        bidder_key=bidder["bidder_key"],
        bidder_name=bidder_name,
        summary=summary,
        checks=visible_checks,
        tender_meta=tender_meta,
        business_meta=bidder["business"]["meta"],
        technical_meta=(technical_document or {}).get("meta"),
    )

    documents = {
        "tender": tender_meta,
        "business": bidder["business"]["meta"],
    }
    if technical_document:
        documents["technical"] = technical_document["meta"]

    return {
        "bidder_key": bidder["bidder_key"],
        "bidder_name": bidder_name,
        "reading_guide": reading_guide,
        "documents": documents,
        "summary": summary,
        "checks": visible_checks,
        "issues": issues,
    }


def _build_selected_review(
    service: UnifiedBusinessReviewService,
    *,
    dataset_dir: Path,
    selected_checks: list[str],
    project_identifier: str | None = None,
    source_result_json: Path | None = None,
) -> dict[str, Any]:
    dataset = service._discover_dataset(dataset_dir)
    resolved_project_identifier = project_identifier or dataset["base_dir"].name

    bidders: list[dict[str, Any]] = []
    for bidder in dataset["bidders"]:
        bidders.append(
            _run_selected_bidder_checks(
                service,
                tender_payload=dataset["tender"]["content"],
                tender_meta=dataset["tender"]["meta"],
                bidder=bidder,
                selected_checks=selected_checks,
            )
        )

    reading_guide = service._build_review_reading_guide(
        tender_meta=dataset["tender"]["meta"],
        bidders=bidders,
    )

    return {
        "schema_version": service.RESULT_SCHEMA_VERSION,
        "review_type": "selected_project_checks_review",
        "generated_at": service._utc_now_iso(),
        "project_identifier_id": resolved_project_identifier,
        "selected_checks": selected_checks,
        "selected_check_labels": _selected_check_labels(selected_checks),
        "dataset": {
            "base_dir": str(dataset["base_dir"]),
            "source_result_json": str(source_result_json) if source_result_json else None,
            "tender": dataset["tender"]["meta"],
            "bidders": [
                {
                    "bidder_key": bidder["bidder_key"],
                    "business": bidder["business"]["meta"],
                    "technical": bidder["technical"]["meta"],
                }
                for bidder in dataset["bidders"]
            ],
            "file_count": 1 + len(dataset["bidders"]) * 2,
        },
        "reading_guide": reading_guide,
        "function_validation": service._summarize_function_validation(bidders),
        "summary": service._summarize_review(bidders),
        "bidders": bidders,
    }


def _build_personnel_reuse_result(review_result: dict[str, Any]) -> dict[str, Any]:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_personnel_count = 0
    total_reused_name_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        personnel_reuse_check = group.get("personnel_reuse_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_personnel_count = int(summary.get("personnel_count") or 0)
        group_reused_name_count = int(summary.get("reused_name_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "personnel_reuse_check": personnel_reuse_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "personnel_count": group_personnel_count,
                "reused_name_count": group_reused_name_count,
                "suspicious": bool(group_reused_name_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_personnel_count += group_personnel_count
        total_reused_name_count += group_reused_name_count

    config = review_result.get("config") or {}
    return {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "personnel_reuse_scope": config.get("personnel_reuse_scope"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "personnel_count": total_personnel_count,
            "reused_name_count": total_reused_name_count,
            "suspicious": bool(total_reused_name_count),
        },
    }


def _build_typo_check_result(review_result: dict[str, Any]) -> dict[str, Any]:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_typo_issue_count = 0
    total_shared_typo_issue_count = 0
    total_suspicious_typo_document_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        typo_check = group.get("typo_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_typo_issue_count = int(summary.get("typo_issue_count") or 0)
        group_shared_typo_issue_count = int(summary.get("shared_typo_issue_count") or 0)
        group_suspicious_document_count = int(summary.get("suspicious_typo_document_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "typo_check": typo_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "typo_issue_count": group_typo_issue_count,
                "shared_typo_issue_count": group_shared_typo_issue_count,
                "suspicious_typo_document_count": group_suspicious_document_count,
                "suspicious": bool(group_typo_issue_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_typo_issue_count += group_typo_issue_count
        total_shared_typo_issue_count += group_shared_typo_issue_count
        total_suspicious_typo_document_count += group_suspicious_document_count

    config = review_result.get("config") or {}
    return {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "typo_detection_engine": config.get("typo_detection_engine"),
            "typo_stopword_dictionary_enabled": config.get("typo_stopword_dictionary_enabled"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "typo_issue_count": total_typo_issue_count,
            "shared_typo_issue_count": total_shared_typo_issue_count,
            "suspicious_typo_document_count": total_suspicious_typo_document_count,
            "suspicious": bool(total_typo_issue_count),
        },
    }


def _build_selected_extra_results(
    *,
    dataset: dict[str, Any],
    selected_checks: list[str],
    project_identifier: str,
) -> dict[str, Any]:
    records = _build_local_document_records(dataset)
    duplicate_service = DuplicateCheckService()
    bid_review_service = BidDocumentReviewService()
    results: dict[str, Any] = {}

    if "business_bid_duplicate_check" in selected_checks:
        results["business_bid_duplicate_check"] = duplicate_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=records,
            document_types=[DOCUMENT_TYPE_BUSINESS_BID],
        )
    if "technical_bid_duplicate_check" in selected_checks:
        results["technical_bid_duplicate_check"] = duplicate_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=records,
            document_types=[DOCUMENT_TYPE_TECHNICAL_BID],
        )
    if "personnel_reuse_check" in selected_checks:
        personnel_review_result = bid_review_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=records,
            document_types=[DOCUMENT_TYPE_BUSINESS_BID],
        )
        results["personnel_reuse_check"] = _build_personnel_reuse_result(personnel_review_result)
    if "typo_check" in selected_checks:
        typo_review_result = bid_review_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=records,
            document_types=[DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID],
        )
        results["typo_check"] = _build_typo_check_result(typo_review_result)

    return results


def _normalize_identity_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    return text


def _bidder_identity_tokens(bidder: dict[str, Any]) -> list[str]:
    documents = bidder.get("documents") or {}
    business = documents.get("business") or {}
    technical = documents.get("technical") or {}
    tokens = [
        bidder.get("bidder_key"),
        bidder.get("bidder_name"),
        business.get("file_name"),
        technical.get("file_name"),
    ]
    normalized = []
    seen = set()
    for token in tokens:
        norm = _normalize_identity_token(token)
        if norm and norm not in seen:
            seen.add(norm)
            normalized.append(norm)
    return normalized


def _index_bidders(review: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for bidder in review.get("bidders") or []:
        for token in _bidder_identity_tokens(bidder):
            mapping.setdefault(token, bidder)
    return mapping


def _get_issue_titles(check: dict[str, Any] | None, bucket: str) -> list[str]:
    if not isinstance(check, dict):
        return []
    issues = (check.get("issues") or {}).get(bucket) or []
    titles = []
    for issue in issues:
        title = str(issue.get("title") or "").strip()
        if title:
            titles.append(title)
    return titles


def _get_check_status(check: dict[str, Any] | None) -> str:
    if not isinstance(check, dict):
        return "missing"
    return str(((check.get("review") or {}).get("status") or "missing")).strip().lower() or "missing"


def _issue_counts(check: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(check, dict):
        return {"passed": 0, "failed": 0, "unclear": 0}
    issues = check.get("issues") or {}
    return {
        "passed": len(issues.get("passed") or []),
        "failed": len(issues.get("failed") or []),
        "unclear": len(issues.get("unclear") or []),
    }


def _compare_single_check(old_check: dict[str, Any] | None, new_check: dict[str, Any] | None) -> dict[str, Any]:
    old_status = _get_check_status(old_check)
    new_status = _get_check_status(new_check)
    old_counts = _issue_counts(old_check)
    new_counts = _issue_counts(new_check)

    old_failed = set(_get_issue_titles(old_check, "failed"))
    new_failed = set(_get_issue_titles(new_check, "failed"))
    old_unclear = set(_get_issue_titles(old_check, "unclear"))
    new_unclear = set(_get_issue_titles(new_check, "unclear"))

    change = "unchanged"
    if old_status == "missing" and new_status != "missing":
        change = "new_only"
    elif old_status != "missing" and new_status == "missing":
        change = "missing_in_new"
    else:
        old_rank = STATUS_ORDER.get(old_status, 9)
        new_rank = STATUS_ORDER.get(new_status, 9)
        if new_rank < old_rank:
            change = "improved"
        elif new_rank > old_rank:
            change = "regressed"
        elif (new_counts["failed"], new_counts["unclear"]) < (old_counts["failed"], old_counts["unclear"]):
            change = "improved"
        elif (new_counts["failed"], new_counts["unclear"]) > (old_counts["failed"], old_counts["unclear"]):
            change = "regressed"
        elif old_failed != new_failed or old_unclear != new_unclear:
            change = "changed"

    return {
        "change": change,
        "old_status": old_status,
        "new_status": new_status,
        "old_summary": ((old_check or {}).get("review") or {}).get("summary"),
        "new_summary": ((new_check or {}).get("review") or {}).get("summary"),
        "old_issue_counts": old_counts,
        "new_issue_counts": new_counts,
        "resolved_failed_titles": sorted(old_failed - new_failed),
        "new_failed_titles": sorted(new_failed - old_failed),
        "resolved_unclear_titles": sorted(old_unclear - new_unclear),
        "new_unclear_titles": sorted(new_unclear - old_unclear),
    }


def _summarize_review_statuses(review: dict[str, Any], selected_checks: list[str]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for check_code in selected_checks:
        counter = Counter()
        for bidder in review.get("bidders") or []:
            check = (bidder.get("checks") or {}).get(check_code)
            counter[_get_check_status(check)] += 1
        summary[check_code] = dict(counter)
    return summary


def _build_comparison(
    *,
    original_review: dict[str, Any],
    new_review: dict[str, Any],
    selected_checks: list[str],
) -> dict[str, Any]:
    old_index = _index_bidders(original_review)
    summary_counter = Counter()
    bidder_comparisons: list[dict[str, Any]] = []

    for new_bidder in new_review.get("bidders") or []:
        matched_old = None
        for token in _bidder_identity_tokens(new_bidder):
            matched_old = old_index.get(token)
            if matched_old is not None:
                break

        checks_comparison: dict[str, Any] = {}
        bidder_change_counter = Counter()
        for check_code in selected_checks:
            old_check = ((matched_old or {}).get("checks") or {}).get(check_code)
            new_check = (new_bidder.get("checks") or {}).get(check_code)
            comparison = _compare_single_check(old_check, new_check)
            checks_comparison[check_code] = comparison
            summary_counter[comparison["change"]] += 1
            bidder_change_counter[comparison["change"]] += 1

        bidder_comparisons.append(
            {
                "bidder_key": new_bidder.get("bidder_key"),
                "bidder_name": new_bidder.get("bidder_name"),
                "matched_original_bidder_key": (matched_old or {}).get("bidder_key"),
                "change_counts": dict(bidder_change_counter),
                "checks": checks_comparison,
            }
        )

    comparison = {
        "selected_checks": selected_checks,
        "selected_check_labels": _selected_check_labels(selected_checks),
        "original_summary": {
            "review_status_counts": _summarize_review_statuses(original_review, selected_checks),
        },
        "new_summary": {
            "review_status_counts": _summarize_review_statuses(new_review, selected_checks),
        },
        "change_summary": dict(summary_counter),
        "bidders": bidder_comparisons,
    }
    return comparison


def _metric_tuple_for_extra(result_key: str, result: dict[str, Any] | None) -> tuple[int, ...]:
    summary = (result or {}).get("summary") or {}
    if result_key in {"business_bid_duplicate_check", "technical_bid_duplicate_check"}:
        return (
            int(summary.get("high_risk_pair_count") or 0),
            int(summary.get("medium_risk_pair_count") or 0),
            int(summary.get("suspicious_pair_count") or 0),
            int(summary.get("pair_count") or 0),
        )
    if result_key == "personnel_reuse_check":
        return (
            int(summary.get("reused_name_count") or 0),
            int(summary.get("personnel_count") or 0),
        )
    if result_key == "typo_check":
        return (
            int(summary.get("typo_issue_count") or 0),
            int(summary.get("shared_typo_issue_count") or 0),
            int(summary.get("suspicious_typo_document_count") or 0),
        )
    return tuple()


def _compare_extra_result(result_key: str, old_result: dict[str, Any] | None, new_result: dict[str, Any] | None) -> dict[str, Any]:
    old_tuple = _metric_tuple_for_extra(result_key, old_result)
    new_tuple = _metric_tuple_for_extra(result_key, new_result)

    if old_result is None and new_result is not None:
        change = "new_only"
    elif old_result is not None and new_result is None:
        change = "missing_in_new"
    elif new_tuple < old_tuple:
        change = "improved"
    elif new_tuple > old_tuple:
        change = "regressed"
    elif (old_result or {}).get("summary") != (new_result or {}).get("summary"):
        change = "changed"
    else:
        change = "unchanged"

    return {
        "change": change,
        "old_summary": (old_result or {}).get("summary") or {},
        "new_summary": (new_result or {}).get("summary") or {},
    }


def _build_extra_comparison(
    *,
    original_payload: dict[str, Any],
    new_extra_results: dict[str, Any],
    selected_checks: list[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    change_summary = Counter()
    for check_code in selected_checks:
        old_result = _extract_named_result(original_payload, check_code)
        new_result = new_extra_results.get(check_code)
        item = _compare_extra_result(check_code, old_result, new_result)
        result[check_code] = item
        change_summary[item["change"]] += 1
    return {
        "items": result,
        "change_summary": dict(change_summary),
    }


def _summarize_original_extra_issue(result_key: str, result: dict[str, Any] | None) -> str | None:
    if not isinstance(result, dict):
        return "原结果中缺少该检查项。"

    summary = result.get("summary") or {}
    if result_key in {"business_bid_duplicate_check", "technical_bid_duplicate_check"}:
        high = int(summary.get("high_risk_pair_count") or 0)
        medium = int(summary.get("medium_risk_pair_count") or 0)
        suspicious = int(summary.get("suspicious_pair_count") or 0)
        pair_count = int(summary.get("pair_count") or 0)
        if high or medium or suspicious:
            return f"共 {pair_count} 对，high={high}，medium={medium}，suspicious={suspicious}"
        return None

    if result_key == "personnel_reuse_check":
        reused = int(summary.get("reused_name_count") or 0)
        personnel = int(summary.get("personnel_count") or 0)
        if reused:
            return f"共提取人员 {personnel} 个，复用姓名 {reused} 个"
        return None

    if result_key == "typo_check":
        typo_issue_count = int(summary.get("typo_issue_count") or 0)
        shared_typo_issue_count = int(summary.get("shared_typo_issue_count") or 0)
        suspicious_typo_document_count = int(summary.get("suspicious_typo_document_count") or 0)
        if typo_issue_count or shared_typo_issue_count or suspicious_typo_document_count:
            return (
                f"错别字 {typo_issue_count} 处，共享错别字 {shared_typo_issue_count} 处，"
                f"可疑文档 {suspicious_typo_document_count} 份"
            )
        return None

    return None


def _render_original_issues(
    *,
    original_review: dict[str, Any],
    original_payload: dict[str, Any],
    selected_checks: list[str],
) -> list[str]:
    lines = ["原始结果问题概览:"]
    has_problem = False

    review_checks = [code for code in selected_checks if code in REVIEW_CHECK_CODES]
    for bidder in original_review.get("bidders") or []:
        bidder_name = bidder.get("bidder_name") or bidder.get("bidder_key") or "未命名投标人"
        bidder_lines: list[str] = []
        for check_code in review_checks:
            check = ((bidder.get("checks") or {}).get(check_code) or {})
            status = _get_check_status(check)
            if status == "pass":
                continue
            if status == "missing":
                bidder_lines.append(f"  - {CHECK_LABELS.get(check_code, check_code)}: 原结果中缺少该检查项")
                continue

            issue_parts: list[str] = []
            failed_titles = _get_issue_titles(check, "failed")
            unclear_titles = _get_issue_titles(check, "unclear")
            if failed_titles:
                issue_parts.append("failed: " + "；".join(failed_titles))
            if unclear_titles:
                issue_parts.append("unclear: " + "；".join(unclear_titles))
            summary = str(((check.get("review") or {}).get("summary") or "")).strip()
            suffix = f" | {summary}" if summary else ""
            detail = f" | {' | '.join(issue_parts)}" if issue_parts else ""
            bidder_lines.append(
                f"  - {CHECK_LABELS.get(check_code, check_code)}: {status}{detail}{suffix}"
            )

        if bidder_lines:
            has_problem = True
            lines.append(f"[{bidder_name}]")
            lines.extend(bidder_lines)

    extra_checks = [code for code in selected_checks if code in EXTRA_CHECK_CODES]
    extra_problem_lines: list[str] = []
    for check_code in extra_checks:
        old_result = _extract_named_result(original_payload, check_code)
        summary_text = _summarize_original_extra_issue(check_code, old_result)
        if summary_text:
            has_problem = True
            extra_problem_lines.append(f"  - {CHECK_LABELS.get(check_code, check_code)}: {summary_text}")

    if extra_problem_lines:
        lines.append("[全项目检查项]")
        lines.extend(extra_problem_lines)

    if not has_problem:
        lines.append("  - 所选检查项在原始结果中未发现明确问题。")

    return lines


def _render_terminal_summary(
    *,
    folder: Path,
    result_json_path: Path,
    original_review: dict[str, Any],
    original_payload: dict[str, Any],
    selected_checks: list[str],
    review_comparison: dict[str, Any],
    extra_comparison: dict[str, Any],
) -> str:
    lines = [
        f"项目文件夹: {folder}",
        f"原结果 JSON: {result_json_path}",
        "选中检查项:",
    ]
    lines.extend(f"  - {label}" for label in _selected_check_labels(selected_checks))
    lines.append("")
    lines.extend(
        _render_original_issues(
            original_review=original_review,
            original_payload=original_payload,
            selected_checks=selected_checks,
        )
    )
    lines.append("")
    lines.append("整体变化:")
    for key in ("improved", "regressed", "changed", "unchanged", "new_only", "missing_in_new"):
        value = int((review_comparison.get("change_summary") or {}).get(key) or 0) + int(
            (extra_comparison.get("change_summary") or {}).get(key) or 0
        )
        if value:
            lines.append(f"  - {key}: {value}")

    for bidder in review_comparison.get("bidders") or []:
        bidder_name = bidder.get("bidder_name") or bidder.get("bidder_key") or "未命名投标人"
        lines.append("")
        lines.append(f"[{bidder_name}]")
        for check_code in [code for code in selected_checks if code in REVIEW_CHECK_CODES]:
            item = (bidder.get("checks") or {}).get(check_code) or {}
            change = item.get("change")
            old_status = item.get("old_status")
            new_status = item.get("new_status")
            old_failed = ((item.get("old_issue_counts") or {}).get("failed") or 0)
            new_failed = ((item.get("new_issue_counts") or {}).get("failed") or 0)
            old_unclear = ((item.get("old_issue_counts") or {}).get("unclear") or 0)
            new_unclear = ((item.get("new_issue_counts") or {}).get("unclear") or 0)
            lines.append(
                f"  - {CHECK_LABELS.get(check_code, check_code)}: "
                f"{old_status} -> {new_status} | {change} | "
                f"failed {old_failed}->{new_failed}, unclear {old_unclear}->{new_unclear}"
            )
            resolved_failed = item.get("resolved_failed_titles") or []
            new_failed_titles = item.get("new_failed_titles") or []
            resolved_unclear = item.get("resolved_unclear_titles") or []
            new_unclear_titles = item.get("new_unclear_titles") or []
            if resolved_failed:
                lines.append(f"      resolved_failed: {'; '.join(resolved_failed)}")
            if new_failed_titles:
                lines.append(f"      new_failed: {'; '.join(new_failed_titles)}")
            if resolved_unclear:
                lines.append(f"      resolved_unclear: {'; '.join(resolved_unclear)}")
            if new_unclear_titles:
                lines.append(f"      new_unclear: {'; '.join(new_unclear_titles)}")

    extra_items = extra_comparison.get("items") or {}
    if extra_items:
        lines.append("")
        lines.append("[全项目检查项]")
        for check_code in [code for code in selected_checks if code in EXTRA_CHECK_CODES]:
            item = extra_items.get(check_code) or {}
            lines.append(
                f"  - {CHECK_LABELS.get(check_code, check_code)}: "
                f"{item.get('change')} | old={json.dumps(item.get('old_summary') or {}, ensure_ascii=False)} "
                f"| new={json.dumps(item.get('new_summary') or {}, ensure_ascii=False)}"
            )

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    _configure_console_encoding()
    args = _parse_args(argv)

    folder_input = args.folder or DEFAULT_PROJECT_FOLDER or _prompt_for_folder()
    checks_input = args.checks or DEFAULT_CHECKS or _prompt_for_checks()

    folder = _resolve_folder(folder_input)
    selected_checks = _normalize_selected_checks(checks_input)
    result_json_path, original_payload, original_review = _find_original_result_json(
        folder,
        args.result_json or DEFAULT_RESULT_JSON or None,
    )

    service = UnifiedBusinessReviewService()
    dataset = service._discover_dataset(folder)
    project_identifier = args.project_identifier or DEFAULT_PROJECT_IDENTIFIER or folder.name

    review_checks = [code for code in selected_checks if code in REVIEW_CHECK_CODES]
    extra_checks = [code for code in selected_checks if code in EXTRA_CHECK_CODES]

    selected_review = None
    review_comparison = {"change_summary": {}, "bidders": [], "selected_checks": review_checks}
    if review_checks:
        selected_review = _build_selected_review(
            service,
            dataset_dir=folder,
            selected_checks=review_checks,
            project_identifier=project_identifier,
            source_result_json=result_json_path,
        )
        review_comparison = _build_comparison(
            original_review=original_review,
            new_review=selected_review,
            selected_checks=review_checks,
        )

    extra_results = {}
    extra_comparison = {"items": {}, "change_summary": {}}
    if extra_checks:
        extra_results = _build_selected_extra_results(
            dataset=dataset,
            selected_checks=extra_checks,
            project_identifier=project_identifier,
        )
        extra_comparison = _build_extra_comparison(
            original_payload=original_payload,
            new_extra_results=extra_results,
            selected_checks=extra_checks,
        )

    print(
        _render_terminal_summary(
            folder=folder,
            result_json_path=result_json_path,
            original_review=original_review,
            original_payload=original_payload,
            selected_checks=selected_checks,
            review_comparison=review_comparison,
            extra_comparison=extra_comparison,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
