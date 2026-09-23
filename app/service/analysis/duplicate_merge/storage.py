"""Compact storage helpers for merged duplicate-check results.

Schema v2 stores each source pair once and references it from clusters and
occurrences.  Readers can resolve one issue at a time without expanding the
whole result tree.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from typing import Any, Iterator


DUPLICATE_STORAGE_SCHEMA_VERSION = 2
MERGED_DUPLICATE_KEYS = frozenset({
    "business_bid_duplicate_clusters",
    "technical_bid_duplicate_clusters",
})
DISPLAY_DUPLICATE_KEYS = frozenset({
    "business_bid_duplicate_check",
    "technical_bid_duplicate_check",
})


class DuplicateSourceReferenceError(ValueError):
    """A compact duplicate result contains a missing or invalid source ID."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def source_item_id(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def is_compact_duplicate_payload(value: Any) -> bool:
    return isinstance(value, dict) and int(value.get("storage_schema_version") or 0) == DUPLICATE_STORAGE_SCHEMA_VERSION


def _validate_source_id(source_items: dict[str, Any], value: Any) -> str:
    identifier = str(value or "").strip()
    if not identifier or identifier not in source_items:
        raise DuplicateSourceReferenceError(f"查重来源引用不存在：{identifier or '<empty>'}")
    if not isinstance(source_items[identifier], dict):
        raise DuplicateSourceReferenceError(f"查重来源引用内容无效：{identifier}")
    return identifier


def validate_compact_duplicate_payload(payload: Any) -> dict[str, Any]:
    if not is_compact_duplicate_payload(payload):
        raise DuplicateSourceReferenceError("查重结果不是 storage schema v2")
    source_items = payload.get("source_items")
    if not isinstance(source_items, dict):
        raise DuplicateSourceReferenceError("查重结果缺少 source_items")
    for issue in payload.get("issues") or []:
        if not isinstance(issue, dict):
            raise DuplicateSourceReferenceError("查重问题项格式无效")
        for identifier in issue.get("source_issue_ids") or []:
            _validate_source_id(source_items, identifier)
        for occurrence in issue.get("occurrences") or []:
            if not isinstance(occurrence, dict):
                raise DuplicateSourceReferenceError("查重证据格式无效")
            identifier = occurrence.get("source_item_id")
            if identifier is not None:
                _validate_source_id(source_items, identifier)
    return payload


def compact_duplicate_payload(payload: Any) -> Any:
    """Return an idempotent compact copy of one merged duplicate payload."""
    if not isinstance(payload, dict):
        return payload
    if is_compact_duplicate_payload(payload):
        validate_compact_duplicate_payload(payload)
        return payload

    source_items: dict[str, dict[str, Any]] = {}

    def remember(item: Any) -> str:
        if not isinstance(item, dict):
            raise DuplicateSourceReferenceError("查重来源项必须是对象")
        identifier = source_item_id(item)
        source_items.setdefault(identifier, item)
        return identifier

    issues: list[dict[str, Any]] = []
    for raw_issue in payload.get("issues") or []:
        if not isinstance(raw_issue, dict):
            raise DuplicateSourceReferenceError("查重问题项格式无效")
        issue = {key: value for key, value in raw_issue.items() if key not in {"source_issues", "source_issue_ids", "occurrences"}}
        source_ids = [remember(item) for item in raw_issue.get("source_issues") or []]
        if source_ids:
            issue["source_issue_ids"] = source_ids

        occurrences: list[dict[str, Any]] = []
        for raw_occurrence in raw_issue.get("occurrences") or []:
            if not isinstance(raw_occurrence, dict):
                raise DuplicateSourceReferenceError("查重证据格式无效")
            occurrence = {key: value for key, value in raw_occurrence.items() if key not in {"item", "source_item_id"}}
            source = raw_occurrence.get("item")
            if isinstance(source, dict):
                identifier = remember(source)
                occurrence["source_item_id"] = identifier
            occurrences.append(occurrence)
        if "occurrences" in raw_issue:
            issue["occurrences"] = occurrences
        issues.append(issue)

    compact = dict(payload)
    compact["storage_schema_version"] = DUPLICATE_STORAGE_SCHEMA_VERSION
    compact["source_items"] = source_items
    compact["issues"] = issues
    return validate_compact_duplicate_payload(compact)


def compact_project_duplicate_results(result: Any) -> Any:
    """Compact merged duplicate payloads at the raw and manual-latest levels."""
    if not isinstance(result, dict):
        return result
    compact = dict(result)
    for key in MERGED_DUPLICATE_KEYS:
        if isinstance(compact.get(key), dict):
            compact[key] = compact_duplicate_payload(compact[key])
    manual = compact.get("manual_review_results")
    if isinstance(manual, dict) and isinstance(manual.get("latest"), dict):
        latest = dict(manual["latest"])
        for key in DISPLAY_DUPLICATE_KEYS | MERGED_DUPLICATE_KEYS:
            if isinstance(latest.get(key), dict) and "issues" in latest[key]:
                latest[key] = compact_duplicate_payload(latest[key])
        compact["manual_review_results"] = {**manual, "latest": latest}
    return compact


def resolve_source_item(payload: dict[str, Any], identifier: Any) -> dict[str, Any]:
    if not is_compact_duplicate_payload(payload):
        raise DuplicateSourceReferenceError("旧格式查重结果没有来源引用")
    source_items = payload.get("source_items") or {}
    resolved = source_items[_validate_source_id(source_items, identifier)]
    return resolved


def hydrate_duplicate_issue(payload: dict[str, Any], issue: dict[str, Any]) -> dict[str, Any]:
    """Expand a single issue for legacy consumers; never expands all issues."""
    if not is_compact_duplicate_payload(payload):
        return deepcopy(issue)
    validate_compact_duplicate_payload(payload)
    hydrated = deepcopy({key: value for key, value in issue.items() if key != "source_issue_ids"})
    if "source_issue_ids" in issue:
        hydrated["source_issues"] = [
            deepcopy(resolve_source_item(payload, identifier))
            for identifier in issue.get("source_issue_ids") or []
        ]
    hydrated_occurrences: list[dict[str, Any]] = []
    for occurrence in issue.get("occurrences") or []:
        item = deepcopy({key: value for key, value in occurrence.items() if key != "source_item_id"})
        if occurrence.get("source_item_id") is not None:
            item["item"] = deepcopy(resolve_source_item(payload, occurrence["source_item_id"]))
        hydrated_occurrences.append(item)
    if "occurrences" in issue:
        hydrated["occurrences"] = hydrated_occurrences
    return hydrated


def iter_hydrated_duplicate_issues(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
    for issue in payload.get("issues") or []:
        if isinstance(issue, dict):
            yield hydrate_duplicate_issue(payload, issue)
