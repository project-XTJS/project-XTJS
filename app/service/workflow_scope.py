from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from app.service.manual_review_state import manual_review_results_from_record


WORKFLOW_INPUTS_KEY = "workflow"
EXCLUDED_BIDDERS_KEY = "excluded_bidders"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def normalize_workflow_scope(scope: Any) -> dict[str, Any]:
    payload = as_dict(scope)
    excluded_bidders: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for raw_item in as_list(payload.get(EXCLUDED_BIDDERS_KEY)):
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        relation_id = item.get("relation_id")
        relation_key = str(relation_id or "").strip()
        business_id = str(
            item.get("business_bid_document_id")
            or item.get("business_document_identifier_id")
            or item.get("business_identifier_id")
            or ""
        ).strip()
        technical_id = str(
            item.get("technical_bid_document_id")
            or item.get("technical_document_identifier_id")
            or item.get("technical_identifier_id")
            or ""
        ).strip()
        if not relation_key and not business_id and not technical_id:
            continue
        key = (relation_key, business_id, technical_id)
        if key in seen:
            continue
        seen.add(key)
        normalized = {
            "relation_id": relation_id,
            "bidder_key": str(item.get("bidder_key") or "").strip(),
            "bidder_name": str(item.get("bidder_name") or "").strip(),
            "business_bid_document_id": business_id,
            "business_file_name": str(item.get("business_file_name") or "").strip(),
            "technical_bid_document_id": technical_id,
            "technical_file_name": str(item.get("technical_file_name") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "source_result_key": str(item.get("source_result_key") or "").strip(),
            "source_alert_id": str(item.get("source_alert_id") or "").strip(),
            "updated_at": str(item.get("updated_at") or "").strip() or utc_now_iso(),
        }
        excluded_bidders.append(normalized)

    return {EXCLUDED_BIDDERS_KEY: excluded_bidders}


def workflow_scope_from_result_record(record: dict[str, Any] | None) -> dict[str, Any]:
    manual_review_results = manual_review_results_from_record(record)
    return normalize_workflow_scope(manual_review_results.get("workflow_scope"))


def _string_candidates(*values: Any) -> set[str]:
    return {str(value).strip() for value in values if str(value or "").strip()}


def _record_relation_keys(record: dict[str, Any]) -> set[str]:
    return _string_candidates(record.get("relation_id"), record.get("id"))


def _record_business_keys(record: dict[str, Any]) -> set[str]:
    return _string_candidates(
        record.get("business_bid_document_id"),
        record.get("business_bid_identifier_id"),
        record.get("business_document_identifier_id"),
        record.get("business_identifier_id"),
        record.get("identifier_id") if str(record.get("relation_role") or "") == "business_bid" else "",
    )


def _record_technical_keys(record: dict[str, Any]) -> set[str]:
    return _string_candidates(
        record.get("technical_bid_document_id"),
        record.get("technical_bid_identifier_id"),
        record.get("technical_document_identifier_id"),
        record.get("technical_identifier_id"),
        record.get("identifier_id") if str(record.get("relation_role") or "") == "technical_bid" else "",
    )


def record_matches_excluded_bidder(record: dict[str, Any], excluded_bidder: dict[str, Any]) -> bool:
    relation_keys = _record_relation_keys(record)
    excluded_relation = str(excluded_bidder.get("relation_id") or "").strip()
    if excluded_relation and excluded_relation in relation_keys:
        return True

    business_keys = _record_business_keys(record)
    excluded_business = str(excluded_bidder.get("business_bid_document_id") or "").strip()
    if excluded_business and excluded_business in business_keys:
        return True

    technical_keys = _record_technical_keys(record)
    excluded_technical = str(excluded_bidder.get("technical_bid_document_id") or "").strip()
    return bool(excluded_technical and excluded_technical in technical_keys)


def is_record_excluded(record: dict[str, Any], workflow_scope: dict[str, Any] | None) -> bool:
    scope = normalize_workflow_scope(workflow_scope or {})
    return any(
        record_matches_excluded_bidder(record, excluded)
        for excluded in scope.get(EXCLUDED_BIDDERS_KEY, [])
    )


def filter_document_records(
    records: list[dict[str, Any]],
    workflow_scope: dict[str, Any] | None,
    *,
    include_excluded: bool = False,
) -> list[dict[str, Any]]:
    if include_excluded:
        return list(records or [])
    return [
        record
        for record in (records or [])
        if not is_record_excluded(record, workflow_scope)
    ]


def filter_project_payload(
    payload: dict[str, Any] | None,
    *,
    include_excluded: bool = False,
) -> dict[str, Any]:
    source = dict(payload or {})
    scope = normalize_workflow_scope(source.get(WORKFLOW_INPUTS_KEY) or source.get("workflow_scope"))
    source["workflow_scope"] = scope
    source["documents"] = filter_document_records(
        list(source.get("documents") or []),
        scope,
        include_excluded=include_excluded,
    )
    return source


def build_excluded_bidders_from_technical_ids(
    payload: dict[str, Any],
    technical_document_ids: list[str],
    *,
    existing_scope: dict[str, Any] | None = None,
    reason: str = "",
    source_result_key: str = "",
) -> dict[str, Any]:
    selected_ids = _string_candidates(*(technical_document_ids or []))
    scope = normalize_workflow_scope(existing_scope or payload.get("workflow_scope") or {})
    existing = list(scope.get(EXCLUDED_BIDDERS_KEY, []))
    existing_keys = {
        (
            str(item.get("relation_id") or "").strip(),
            str(item.get("business_bid_document_id") or "").strip(),
            str(item.get("technical_bid_document_id") or "").strip(),
        )
        for item in existing
    }

    records = list((payload or {}).get("documents") or [])
    business_by_relation = {
        str(record.get("relation_id") or "").strip(): record
        for record in records
        if str(record.get("relation_role") or "") == "business_bid"
    }
    for record in records:
        if str(record.get("relation_role") or "") != "technical_bid":
            continue
        technical_id = str(record.get("identifier_id") or "").strip()
        if technical_id not in selected_ids:
            continue
        relation_key = str(record.get("relation_id") or "").strip()
        business_record = business_by_relation.get(relation_key) or {}
        item = {
            "relation_id": record.get("relation_id"),
            "bidder_key": "",
            "bidder_name": str(business_record.get("file_name") or record.get("file_name") or "").strip(),
            "business_bid_document_id": str(business_record.get("identifier_id") or "").strip(),
            "business_file_name": str(business_record.get("file_name") or "").strip(),
            "technical_bid_document_id": technical_id,
            "technical_file_name": str(record.get("file_name") or "").strip(),
            "reason": reason,
            "source_result_key": source_result_key,
            "source_alert_id": "",
            "updated_at": utc_now_iso(),
        }
        key = (
            str(item.get("relation_id") or "").strip(),
            str(item.get("business_bid_document_id") or "").strip(),
            str(item.get("technical_bid_document_id") or "").strip(),
        )
        if key in existing_keys:
            continue
        existing_keys.add(key)
        existing.append(item)

    return normalize_workflow_scope({EXCLUDED_BIDDERS_KEY: existing})


def clone_without_reserved_result_keys(result: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(result or {})
    return payload
