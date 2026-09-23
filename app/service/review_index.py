"""Build and persist lightweight review summaries and paged issue indexes."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

from app.service import document_blob_store
from app.service.analysis.duplicate_merge.constants import MERGED_RESULT_KEY_BY_DOC_TYPE
from app.service.analysis.duplicate_merge.storage import (
    canonical_json_bytes,
    compact_project_duplicate_results,
    is_compact_duplicate_payload,
    validate_compact_duplicate_payload,
)
from app.service.project_result_summary import is_result_key_visible


REVIEW_INDEX_SCHEMA_VERSION = 1
REMOVED_BUSINESS_SCOPE_ISSUE_TITLE = "商务材料组成范围待确认"


def is_removed_review_issue(value: Any) -> bool:
    """Hide the retired scope-boundary diagnostic from legacy stored results."""
    return (
        isinstance(value, dict)
        and str(value.get("title") or "").strip() == REMOVED_BUSINESS_SCOPE_ISSUE_TITLE
    )


def is_optional_business_review_issue(value: Any) -> bool:
    """Identify legacy rows for material explicitly excluded from review scope."""
    if not isinstance(value, dict):
        return False
    evidence = value.get("evidence") if isinstance(value.get("evidence"), dict) else {}
    skip_reason = evidence.get("skip_reason") if isinstance(evidence.get("skip_reason"), dict) else {}
    requirements = evidence.get("requirements") if isinstance(evidence.get("requirements"), dict) else {}
    conflict = bool(
        evidence.get("optionality_conflict")
        or requirements.get("optionality_conflict")
    )
    if conflict:
        return False
    if evidence.get("is_optional") or requirements.get("is_optional"):
        return True
    applicability = str(
        evidence.get("applicability_status")
        or requirements.get("applicability_status")
        or ""
    ).strip().lower()
    if applicability in {"optional", "not_applicable"}:
        return True
    if str(skip_reason.get("type") or "").strip().lower() == "optional_attachment_not_provided":
        return True
    searchable = "\n".join(
        str(value.get(key) or "")
        for key in ("title", "message", "description", "reason")
    )
    return bool(
        re.search(r"招标文件将.{0,30}列为可选", searchable)
        or re.search(r"(?:本项目|不项目|本项日)\s*(?:为)?\s*不适用", searchable)
    )


def is_excluded_business_review_issue(value: Any) -> bool:
    return is_removed_review_issue(value) or is_optional_business_review_issue(value)


def _business_review_issue_title_key(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    title = str(value.get("title") or "").strip()
    title = re.sub(r"[（(](?:本项目|不项目|本项日)?(?:为)?不适用[）)]", "", title)
    return re.sub(r"[\s：:；;，,。()（）【】\[\]]+", "", title)


def _matches_optional_review_title(value: Any, optional_title_keys: set[str]) -> bool:
    key = _business_review_issue_title_key(value)
    if not key:
        return False
    return any(
        key == optional_key
        or (min(len(key), len(optional_key)) >= 4 and (
            key in optional_key or optional_key in key
        ))
        for optional_key in optional_title_keys
    )


def build_result_version(result: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(result)).hexdigest()


def _manual_latest(result: dict[str, Any]) -> dict[str, Any]:
    manual = result.get("manual_review_results") or {}
    return dict(manual.get("latest") or {}) if isinstance(manual, dict) else {}


def display_result_components(result: dict[str, Any]) -> dict[str, Any]:
    """Build the display-key mapping without deep-copying report bodies."""
    latest = _manual_latest(result)
    visible = {
        key: value
        for key, value in result.items()
        if key != "manual_review_results"
    }
    visible.update(latest)
    aliases = {
        "business_bid_duplicate_check": "business_bid_duplicate_clusters",
        "technical_bid_duplicate_check": "technical_bid_duplicate_clusters",
    }
    for raw_key, merged_key in aliases.items():
        merged = result.get(merged_key)
        if raw_key not in latest and isinstance(merged, dict) and merged:
            visible[raw_key] = merged
    excluded = {"duplicate_check", *MERGED_RESULT_KEY_BY_DOC_TYPE.values()}
    return {
        key: value
        for key, value in visible.items()
        if key not in excluded and is_result_key_visible(key)
    }


def _risk(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"high", "error", "fail", "failed", "missing"}:
        return "high"
    if text in {"medium", "warning", "warn", "unclear", "pending", "bonus"}:
        return "medium"
    if text in {"low", "info"}:
        return "low"
    return "none"


def _status_from_risk(risk: str) -> str:
    return "passed" if risk == "none" else "failed"


def _issue_status(issue: dict[str, Any], risk: str) -> str:
    """Preserve the business conclusion independently from risk severity."""
    context = issue.get("_review_context") or {}
    raw = str(issue.get("status") or context.get("issue_group") or "").strip().lower()
    if raw in {"pass", "passed", "success", "ok"}:
        return "pass"
    if raw in {"fail", "failed", "missing", "error"}:
        return "fail"
    if raw in {"unclear", "pending", "ambiguous"}:
        return "unclear"
    if raw in {"not_applicable", "skipped", "optional"}:
        return "not_applicable"
    return _status_from_risk(risk)


def _generic_issue_risk(issue: dict[str, Any]) -> str:
    group = str((issue.get("_review_context") or {}).get("issue_group") or "").strip().lower()
    if group == "passed":
        return "none"
    if group in {"failed", "missing"}:
        return "high"
    if group in {"unclear", "bonus"}:
        return "medium"
    return _risk(issue.get("risk_level") or issue.get("severity") or issue.get("status") or group)


def _stable_issue_id(result_key: str, path: str, value: dict[str, Any]) -> str:
    explicit = value.get("cluster_id") or value.get("issue_id") or value.get("id")
    if explicit:
        return str(explicit)
    identity = {
        "result_key": result_key,
        "path": path,
        "title": value.get("title") or value.get("check_name") or value.get("message"),
        "files": value.get("files") or value.get("file_name"),
        "page": value.get("page") or value.get("pages"),
    }
    return f"review-{result_key}-{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()[:32]}"


def _unique_issue_id(
    candidate: str,
    *,
    result_key: str,
    path: str,
    used_issue_ids: set[str],
) -> str:
    """Keep existing IDs where possible and deterministically disambiguate collisions."""
    if candidate not in used_issue_ids:
        used_issue_ids.add(candidate)
        return candidate
    suffix = hashlib.sha256(
        canonical_json_bytes({"result_key": result_key, "path": path, "issue_id": candidate})
    ).hexdigest()[:16]
    unique = f"{candidate}-{suffix}"
    counter = 2
    while unique in used_issue_ids:
        unique = f"{candidate}-{suffix}-{counter}"
        counter += 1
    used_issue_ids.add(unique)
    return unique


def _file_names(value: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in value.get("files") or []:
        name = item.get("file_name") if isinstance(item, dict) else item
        if str(name or "").strip() and str(name) not in names:
            names.append(str(name))
    for key in ("file_name", "left_file_name", "right_file_name", "document_file_name"):
        name = str(value.get(key) or "").strip()
        if name and name not in names:
            names.append(name)
    for name in (value.get("doc_ranges_by_file") or {}).keys():
        if str(name) not in names:
            names.append(str(name))
    documents = value.get("documents") or []
    document_values = documents.values() if isinstance(documents, dict) else documents
    for item in document_values or []:
        name = item.get("file_name") if isinstance(item, dict) else None
        if str(name or "").strip() and str(name) not in names:
            names.append(str(name))
    return names


def _iter_generic_issues(result_key: str, component: Any) -> Iterable[tuple[str, dict[str, Any], str]]:
    if not isinstance(component, dict):
        return
    if isinstance(component.get("bidders"), list):
        for bidder_index, bidder in enumerate(component["bidders"]):
            if not isinstance(bidder, dict):
                continue
            bidder_key = str(bidder.get("bidder_key") or bidder.get("bidder_name") or bidder_index)
            checks = bidder.get("checks") or {}
            optional_title_keys = {
                _business_review_issue_title_key(issue)
                for check in checks.values()
                if isinstance(check, dict)
                for group in (
                    (check.get("issues") or {}).values()
                    if isinstance(check.get("issues"), dict)
                    else [check.get("issues") or []]
                )
                if isinstance(group, list)
                for issue in group
                if is_optional_business_review_issue(issue)
            }
            optional_title_keys.discard("")
            for check_key, check in checks.items():
                if not isinstance(check, dict):
                    continue
                raw_issues = check.get("issues") or []
                issue_groups = raw_issues if isinstance(raw_issues, dict) else {"issues": raw_issues}
                emitted = 0
                ordered_groups = ["failed", "missing", "unclear", "bonus", "passed", "not_applicable"]
                ordered_groups.extend(key for key in issue_groups if key not in ordered_groups)
                for group_name in ordered_groups:
                    for issue_index, issue in enumerate(issue_groups.get(group_name) or []):
                        if not isinstance(issue, dict):
                            continue
                        if (
                            is_excluded_business_review_issue(issue)
                            or _matches_optional_review_title(issue, optional_title_keys)
                            or (
                                str(check_key) == "itemized_pricing_check"
                                and any("分项报价表" in key for key in optional_title_keys)
                            )
                        ):
                            continue
                        normalized = dict(issue)
                        normalized["_review_context"] = {
                            "bidder_key": bidder.get("bidder_key"),
                            "bidder_name": bidder.get("bidder_name"),
                            "bidder_identity": bidder.get("bidder_identity"),
                            "documents": bidder.get("documents"),
                            "check_code": str(check_key),
                            "check_name": check.get("check_name"),
                            "issue_group": group_name,
                        }
                        yield (
                            f"bidders/{bidder_key}/{check_key}/{group_name}/{issue_index}",
                            normalized,
                            str(check_key),
                        )
                        emitted += 1
                if str(check_key) == "deviation_check" and not (issue_groups.get("passed") or []):
                    for marker_index, marker in enumerate(check.get("marker_items") or []):
                        if not isinstance(marker, dict) or not marker.get("responded"):
                            continue
                        if str(marker.get("response_status") or "") not in {
                            "positive_deviation", "no_deviation", "listed_response",
                        }:
                            continue
                        normalized = {
                            "title": marker.get("requirement") or "标记项响应",
                            "status": "passed",
                            "severity": "info",
                            "message": marker.get("response_evidence") or "已响应",
                            "evidence": marker,
                            "_review_context": {
                                "bidder_key": bidder.get("bidder_key"),
                                "bidder_name": bidder.get("bidder_name"),
                                "bidder_identity": bidder.get("bidder_identity"),
                                "documents": bidder.get("documents"),
                                "check_code": str(check_key),
                                "check_name": check.get("check_name"),
                                "issue_group": "passed",
                            },
                        }
                        yield (
                            f"bidders/{bidder_key}/{check_key}/marker/{marker_index}",
                            normalized,
                            str(check_key),
                        )
                        emitted += 1
                review_status = str((check.get("review") or {}).get("status") or "").strip().lower()
                if emitted == 0 and review_status not in {"", "pass", "passed", "success", "ok", "none"}:
                    synthetic = {
                        "title": check.get("check_name") or str(check_key),
                        "status": review_status,
                        "message": (check.get("review") or {}).get("summary"),
                        "_review_context": {
                            "bidder_key": bidder.get("bidder_key"),
                            "bidder_name": bidder.get("bidder_name"),
                            "bidder_identity": bidder.get("bidder_identity"),
                            "documents": bidder.get("documents"),
                            "check_code": str(check_key),
                            "check_name": check.get("check_name"),
                            "issue_group": "synthetic",
                        },
                    }
                    yield f"bidders/{bidder_key}/{check_key}/synthetic/0", synthetic, str(check_key)
        return
    combined = component.get("combined_personnel_reuse_check") or {}
    if isinstance(combined, dict) and isinstance(combined.get("issues"), list):
        for index, issue in enumerate(combined["issues"]):
            if isinstance(issue, dict):
                yield f"combined/{index}", issue, "personnel_reuse_check"
        return
    for index, issue in enumerate(component.get("issues") or component.get("items") or []):
        if isinstance(issue, dict):
            yield f"issues/{index}", issue, str(issue.get("check_code") or "")


def _duplicate_issue_parts(
    component: dict[str, Any],
    issue: dict[str, Any],
    source_object_keys: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    validate_compact_duplicate_payload(component)
    previews = issue.get("doc_previews_by_file") or {}
    preview_text = " ".join(
        str(text or "").strip()
        for values in previews.values()
        for text in (values or [])[:1]
        if str(text or "").strip()
    )
    list_payload = {
        key: issue.get(key)
        for key in (
            "cluster_id", "title", "family", "mode", "risk_level", "score_display",
            "score_value", "similarity", "files", "file_count", "metrics",
            "doc_ranges_by_file", "occurrence_count", "source_issue_count", "status",
            "review_only", "typo_check",
        )
        if key in issue
    }
    list_payload["short_summary"] = preview_text[:240]
    detail_payload = {
        key: value
        for key, value in issue.items()
        if key not in {"occurrences", "source_issue_ids"}
    }
    detail_payload["source_issue_ids"] = list(issue.get("source_issue_ids") or [])
    referenced_ids = {
        str(value)
        for value in issue.get("source_issue_ids") or []
        if str(value or "").strip()
    }
    referenced_ids.update(
        str(occurrence.get("source_item_id"))
        for occurrence in issue.get("occurrences") or []
        if isinstance(occurrence, dict) and occurrence.get("source_item_id")
    )
    evidence_payload = {
        "issue_id": issue.get("cluster_id"),
        "occurrences": list(issue.get("occurrences") or []),
        "source_item_object_keys": {
            identifier: source_object_keys[identifier]
            for identifier in referenced_ids
            if identifier in source_object_keys
        },
    }
    return list_payload, detail_payload, evidence_payload


def _generic_list_payload(issue: dict[str, Any]) -> dict[str, Any]:
    payload = {
        key: issue.get(key)
        for key in (
            "id", "issue_id", "title", "status", "severity", "risk_level", "message",
            "summary", "reason", "file_name", "document_file_name", "page", "pages",
        )
        if key in issue
    }
    context = issue.get("_review_context") or {}
    if isinstance(context, dict):
        payload["_review_context"] = {
            key: context.get(key)
            for key in (
                "bidder_key", "bidder_name", "bidder_identity", "check_code",
                "check_name", "issue_group",
            )
            if context.get(key) is not None
        }
    if not payload.get("page") and not payload.get("pages"):
        pages: list[int] = []
        for location in issue.get("locations") or []:
            if not isinstance(location, dict):
                continue
            value = location.get("page")
            if str(value or "").isdigit() and int(value) > 0 and int(value) not in pages:
                pages.append(int(value))
            if len(pages) >= 8:
                break
        if pages:
            payload["pages"] = pages
    for key in ("message", "summary", "reason"):
        if isinstance(payload.get(key), str) and len(payload[key]) > 500:
            payload[key] = payload[key][:500] + "…"
    preview = str(
        payload.get("message")
        or payload.get("summary")
        or payload.get("reason")
        or ""
    ).strip()
    if preview:
        payload["short_summary"] = preview[:240]
    return payload


def build_review_index(
    result: dict[str, Any],
    *,
    project_identifier_id: str,
    result_version: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Write immutable component/detail objects and return summary + DB rows."""
    components = display_result_components(result)
    categories: list[dict[str, Any]] = []
    component_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = []
    overall_counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    overall_status_counts = {"pass": 0, "fail": 0, "unclear": 0, "not_applicable": 0}
    used_issue_ids: set[str] = set()

    for result_key, component in components.items():
        source_object_keys: dict[str, str] = {}
        component_object = component
        if is_compact_duplicate_payload(component):
            validate_compact_duplicate_payload(component)
            for identifier, source_item in (component.get("source_items") or {}).items():
                source_object_keys[str(identifier)] = document_blob_store.save_review_index_object(
                    source_item,
                    project_identifier_id=project_identifier_id,
                    result_version=result_version,
                    kind="source",
                    identity=str(identifier),
                )
            # The category object is first-screen metadata only. Issues, evidence and
            # source bodies are fetched through their dedicated immutable objects.
            component_object = {
                key: value
                for key, value in component.items()
                if key not in {"issues", "source_items"}
            }
            component_object["issue_count"] = len(component.get("issues") or [])
        component_key = document_blob_store.save_review_index_object(
            component_object,
            project_identifier_id=project_identifier_id,
            result_version=result_version,
            kind="component",
            identity=result_key,
        )
        category_counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
        category_rows: list[dict[str, Any]] = []
        if is_compact_duplicate_payload(component):
            source_issue_count = len(component.get("issues") or [])
            for order, issue in enumerate(component.get("issues") or []):
                if not isinstance(issue, dict):
                    continue
                list_payload, detail_payload, evidence_payload = _duplicate_issue_parts(
                    component,
                    issue,
                    source_object_keys,
                )
                path = f"issues/{order}"
                issue_id = _unique_issue_id(
                    _stable_issue_id(result_key, path, issue),
                    result_key=result_key,
                    path=path,
                    used_issue_ids=used_issue_ids,
                )
                risk = _risk(issue.get("risk_level"))
                category_counts[risk] += 1
                detail_key = document_blob_store.save_review_index_object(
                    detail_payload,
                    project_identifier_id=project_identifier_id,
                    result_version=result_version,
                    kind="detail",
                    identity=issue_id,
                )
                evidence_key = document_blob_store.save_review_index_object(
                    evidence_payload,
                    project_identifier_id=project_identifier_id,
                    result_version=result_version,
                    kind="evidence",
                    identity=issue_id,
                )
                category_rows.append({
                    "result_key": result_key,
                    "issue_id": issue_id,
                    "issue_order": order,
                    "risk_level": risk,
                    # Compact duplicate results can be review-only even when their
                    # risk is "none". Preserve that explicit business status so a
                    # pending typo candidate is never indexed as a passed item.
                    "status": _issue_status(issue, risk),
                    "check_code": "duplicate_check",
                    "title": str(issue.get("title") or "疑似重复内容"),
                    "description": f"共 {len(issue.get('occurrences') or [])} 条重复证据",
                    "file_names": _file_names(issue),
                    "list_payload": list_payload,
                    "detail_object_key": detail_key,
                    "evidence_object_key": evidence_key,
                    "evidence_count": len(issue.get("occurrences") or []),
                })
        else:
            generic = list(_iter_generic_issues(result_key, component))
            source_issue_count = len(generic)
            for order, (path, issue, check_code) in enumerate(generic):
                issue_id = _unique_issue_id(
                    _stable_issue_id(result_key, path, issue),
                    result_key=result_key,
                    path=path,
                    used_issue_ids=used_issue_ids,
                )
                context = issue.get("_review_context") or {}
                risk = _generic_issue_risk(issue)
                category_counts[risk] += 1
                detail_key = document_blob_store.save_review_index_object(
                    issue,
                    project_identifier_id=project_identifier_id,
                    result_version=result_version,
                    kind="detail",
                    identity=issue_id,
                )
                category_rows.append({
                    "result_key": result_key,
                    "issue_id": issue_id,
                    "issue_order": order,
                    "risk_level": risk,
                    "status": _issue_status(issue, risk),
                    "check_code": check_code,
                    "title": str(issue.get("title") or issue.get("check_name") or issue.get("message") or "审查项"),
                    "description": str(issue.get("message") or issue.get("summary") or issue.get("reason") or ""),
                    "file_names": _file_names(issue) or _file_names(context),
                    "list_payload": _generic_list_payload(issue),
                    "detail_object_key": detail_key,
                    "evidence_object_key": detail_key,
                    "evidence_count": 1,
                })

        if not category_rows:
            clean_issue = {
                "title": "未发现问题",
                "summary": "该检查已完成，未发现风险项",
                "status": "passed",
                "risk_level": "none",
            }
            issue_id = _unique_issue_id(
                _stable_issue_id(result_key, "clean", clean_issue),
                result_key=result_key,
                path="clean",
                used_issue_ids=used_issue_ids,
            )
            detail_key = document_blob_store.save_review_index_object(
                clean_issue,
                project_identifier_id=project_identifier_id,
                result_version=result_version,
                kind="detail",
                identity=issue_id,
            )
            evidence_key = document_blob_store.save_review_index_object(
                {"issue_id": issue_id, "occurrences": [], "source_item_object_keys": {}},
                project_identifier_id=project_identifier_id,
                result_version=result_version,
                kind="evidence",
                identity=issue_id,
            )
            category_counts["none"] += 1
            category_rows.append({
                "result_key": result_key,
                "issue_id": issue_id,
                "issue_order": 0,
                "risk_level": "none",
                "status": "passed",
                "check_code": "",
                "title": clean_issue["title"],
                "description": clean_issue["summary"],
                "file_names": [],
                "list_payload": clean_issue,
                "detail_object_key": detail_key,
                "evidence_object_key": evidence_key,
                "evidence_count": 0,
            })
            source_issue_count = 1

        issue_rows.extend(category_rows)
        status_counts = {"pass": 0, "fail": 0, "unclear": 0, "not_applicable": 0}
        for row in category_rows:
            status = str(row.get("status") or "").lower()
            canonical = {
                "passed": "pass", "failed": "fail", "missing": "fail",
                "skipped": "not_applicable", "optional": "not_applicable",
            }.get(status, status)
            if canonical in status_counts:
                status_counts[canonical] += 1
                overall_status_counts[canonical] += 1
        for risk, count in category_counts.items():
            overall_counts[risk] += count
        component_summary = component.get("summary") if isinstance(component, dict) else {}
        category = {
            "result_key": result_key,
            "status": "ready",
            "issue_count": source_issue_count,
            "risk_counts": category_counts,
            "status_counts": status_counts,
            "review_item_count": sum(status_counts.values()),
            "inconsistent_count": status_counts["fail"],
            "unclear_count": status_counts["unclear"],
            "not_applicable_count": status_counts["not_applicable"],
            "has_risk": sum(category_counts[key] for key in ("high", "medium", "low")) > 0,
            "summary": component_summary if isinstance(component_summary, dict) else {},
        }
        categories.append(category)
        component_rows.append({
            "result_key": result_key,
            "object_key": component_key,
            "summary": category,
            "issue_count": source_issue_count,
        })

    summary = {
        "schema_version": REVIEW_INDEX_SCHEMA_VERSION,
        "result_version": result_version,
        "status": "ready",
        "category_count": len(categories),
        "issue_count": sum(item["issue_count"] for item in categories),
        "risk_counts": overall_counts,
        "status_counts": overall_status_counts,
        "review_item_count": sum(overall_status_counts.values()),
        "inconsistent_count": overall_status_counts["fail"],
        "unclear_count": overall_status_counts["unclear"],
        "not_applicable_count": overall_status_counts["not_applicable"],
        "categories": categories,
    }
    return summary, component_rows, issue_rows


def prepare_review_storage(
    result: dict[str, Any],
    *,
    project_identifier_id: str,
) -> tuple[dict[str, Any], str, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    compact = compact_project_duplicate_results(result)
    version = build_result_version(compact)
    summary, components, issues = build_review_index(
        compact,
        project_identifier_id=project_identifier_id,
        result_version=version,
    )
    return compact, version, summary, components, issues
