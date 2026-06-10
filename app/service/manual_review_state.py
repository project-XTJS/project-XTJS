from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any


REVIEW_CONTENT_SCHEMA_VERSION = "1.0"
MANUAL_REVIEW_RESULTS_KEY = "manual_review_results"
MANUAL_REVIEW_LATEST_KEY = "latest"
WORKFLOW_SCOPE_KEY = "workflow_scope"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def effective_document_content(document: dict[str, Any] | None) -> dict[str, Any]:
    record = as_dict(document)
    review_content = as_dict(record.get("review_content"))
    if "effective_content" in review_content and isinstance(
        review_content.get("effective_content"),
        dict,
    ):
        return deepcopy(review_content["effective_content"])
    return deepcopy(as_dict(record.get("content")))


def normalize_review_content(
    review_content: Any,
    *,
    content: Any = None,
) -> dict[str, Any]:
    source = as_dict(review_content)
    base_content = source.get("base_content")
    effective_content = source.get("effective_content")
    return {
        "schema_version": str(source.get("schema_version") or REVIEW_CONTENT_SCHEMA_VERSION),
        "base_content": deepcopy(base_content) if isinstance(base_content, dict) else {},
        "effective_content": (
            deepcopy(effective_content)
            if isinstance(effective_content, dict)
            else deepcopy(as_dict(content))
        ),
        "inputs": deepcopy(as_dict(source.get("inputs"))),
        "updated_at": str(source.get("updated_at") or ""),
    }


def build_review_content(
    *,
    content: Any,
    existing_review_content: Any,
    effective_content: dict[str, Any] | None = None,
    input_key: str | None = None,
    input_value: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing = as_dict(existing_review_content)
    first_save = not isinstance(existing.get("base_content"), dict)
    normalized = normalize_review_content(existing, content=content)
    if first_save:
        normalized["base_content"] = deepcopy(as_dict(content))
    if effective_content is not None:
        normalized["effective_content"] = deepcopy(as_dict(effective_content))
    elif "effective_content" not in existing:
        normalized["effective_content"] = deepcopy(as_dict(content))
    if input_key:
        inputs = dict(normalized["inputs"])
        inputs[str(input_key)] = deepcopy(as_dict(input_value))
        normalized["inputs"] = inputs
    normalized["schema_version"] = REVIEW_CONTENT_SCHEMA_VERSION
    normalized["updated_at"] = utc_now_iso()
    return normalized


def manual_review_results_from_record(record: dict[str, Any] | None) -> dict[str, Any]:
    payload = as_dict(record)
    result = as_dict(payload.get("result"))
    source = as_dict(result.get(MANUAL_REVIEW_RESULTS_KEY))
    latest = as_dict(source.get(MANUAL_REVIEW_LATEST_KEY))
    workflow_scope = as_dict(source.get(WORKFLOW_SCOPE_KEY))

    return {
        MANUAL_REVIEW_LATEST_KEY: deepcopy(latest),
        WORKFLOW_SCOPE_KEY: deepcopy(workflow_scope),
        "updated_at": str(source.get("updated_at") or ""),
    }


def build_manual_review_results(
    existing: Any,
    *,
    latest_key: str | None = None,
    latest_value: dict[str, Any] | None = None,
    workflow_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = as_dict(existing)
    latest = as_dict(source.get(MANUAL_REVIEW_LATEST_KEY))
    if latest_key:
        latest[str(latest_key)] = deepcopy(as_dict(latest_value))
    next_scope = (
        deepcopy(as_dict(workflow_scope))
        if workflow_scope is not None
        else deepcopy(as_dict(source.get(WORKFLOW_SCOPE_KEY)))
    )
    return {
        MANUAL_REVIEW_LATEST_KEY: latest,
        WORKFLOW_SCOPE_KEY: next_scope,
        "updated_at": utc_now_iso(),
    }


def raw_result_view(result: Any) -> dict[str, Any]:
    payload = deepcopy(as_dict(result))
    payload.pop(MANUAL_REVIEW_RESULTS_KEY, None)
    return payload


def display_result_view(
    result: Any,
    *,
    manual_review_results: Any = None,
) -> dict[str, Any]:
    raw = raw_result_view(result)
    source = as_dict(manual_review_results)
    latest = as_dict(source.get(MANUAL_REVIEW_LATEST_KEY))
    raw.update(deepcopy(latest))
    return raw
