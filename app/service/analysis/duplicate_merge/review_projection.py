"""Read-only, conservative presentation grouping for duplicate evidence.

Pair scores and the saved detection result are intentionally left untouched.
"""

from __future__ import annotations

from copy import deepcopy
from difflib import SequenceMatcher
import hashlib
import re
from typing import Any

from app.service.analysis.location_utils import append_location, make_location


PROJECTION_VERSION = 1
RISK_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}
KIND_ORDER = {"block": 0, "similar_block": 1, "table": 2, "similar_table": 3,
              "section": 4, "similar_section": 5, "image": 6}


def _text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip()


def _side_text(occurrence: dict[str, Any], side: str) -> str:
    evidence = occurrence.get("evidence") or {}
    for key in (f"{side}_analysis_text", f"{side}_text", f"{side}_preview", "text", "preview"):
        if evidence.get(key):
            return _text(evidence[key])
    for key in (f"{side}_rows", f"{side}_sample_rows", "sample_rows"):
        value = evidence.get(key)
        if value:
            return _text(" ".join(map(str, value)) if isinstance(value, list) else value)
    docs = occurrence.get("docs") or {}
    names = list(docs)
    index = 0 if side == "left" else 1
    return _text((docs.get(names[index]) or {}).get("preview")) if len(names) > index else ""


def _anchor(occurrence: dict[str, Any]) -> str:
    evidence = occurrence.get("evidence") or {}
    if str(occurrence.get("family") or "") == "image":
        return _text(evidence.get("hash") or evidence.get("phash"))
    left, right = _side_text(occurrence, "left"), _side_text(occurrence, "right")
    if not left or not right:
        return ""
    if left in right:
        return left
    if right in left:
        return right
    # A shared prefix around different amounts or model numbers is not the
    # same requirement/response, even when the surrounding prose is identical.
    if re.findall(r"\d+(?:[.,]\d+)*", left) != re.findall(r"\d+(?:[.,]\d+)*", right):
        return ""
    # Similar evidence has two different strings. Use only a verified common span.
    if max(len(left), len(right)) > 2000:
        return ""
    match = SequenceMatcher(None, left, right, autojunk=False).find_longest_match()
    return left[match.a:match.a + match.size] if match.size >= 10 and match.size >= .8 * min(len(left), len(right)) else ""


def _source_id(occurrence: dict[str, Any]) -> str:
    return str(occurrence.get("source_item_id") or "")


def _representative_rank(occurrence: dict[str, Any]) -> tuple[int, int, int]:
    evidence = occurrence.get("evidence") or {}
    located = bool(evidence.get("left_bbox") or evidence.get("bbox")) and bool(
        evidence.get("right_bbox") or evidence.get("bbox"))
    return (0 if located else 1, KIND_ORDER.get(str(occurrence.get("kind")), 9), len(_anchor(occurrence)))


def _paired_positions(occurrence: dict[str, Any], source_items: dict[str, Any]) -> tuple:
    source = source_items.get(_source_id(occurrence)) or {}
    docs = occurrence.get("docs") or {}
    evidence = occurrence.get("evidence") or {}
    result = []
    for side in ("left", "right"):
        name = str(occurrence.get(f"{side}_file_name") or source.get(f"{side}_file_name") or "")
        doc = docs.get(name) or {}
        same_name = str(source.get("left_file_name") or "") == str(source.get("right_file_name") or "")
        pages = ((evidence.get(f"{side}_pages") or evidence.get(f"{side}_page")) if same_name else None)
        pages = pages or doc.get("pages") or evidence.get(f"{side}_pages") or evidence.get(f"{side}_page") or []
        if not isinstance(pages, list):
            pages = [pages]
        bbox = evidence.get(f"{side}_bbox") or evidence.get("bbox")
        if isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
            try:
                box = tuple(round(float(value), 2) for value in bbox[:4])
            except (TypeError, ValueError):
                box = ()
        else:
            box = ()
        identity = str(source.get(f"{side}_document_identifier") or source.get(f"{side}_document_identifier_id")
                       or source.get(f"{side}_document_id") or name)
        result.append((identity, tuple(str(page) for page in pages), box))
    return tuple(result)


def _same_physical_evidence(left: dict[str, Any], right: dict[str, Any], sources: dict[str, Any]) -> bool:
    left_anchor, right_anchor = _anchor(left), _anchor(right)
    if not left_anchor or not right_anchor or not (left_anchor in right_anchor or right_anchor in left_anchor):
        return False
    left_positions, right_positions = _paired_positions(left, sources), _paired_positions(right, sources)
    if len(left_positions) != 2 or left_positions != right_positions:
        return False
    # Page equality alone cannot distinguish two occurrences on the same page.
    if all(position[2] for position in left_positions):
        return True
    left_evidence, right_evidence = left.get("evidence") or {}, right.get("evidence") or {}
    same_evidence_id = (left_evidence.get("typo_evidence_id") and
                        left_evidence.get("typo_evidence_id") == right_evidence.get("typo_evidence_id"))
    return bool(same_evidence_id and _source_id(left) == _source_id(right))


def _locations(occurrence: dict[str, Any], sources: dict[str, Any]) -> list[dict[str, Any]]:
    source = sources.get(_source_id(occurrence)) or {}
    evidence = occurrence.get("evidence") or {}
    docs = occurrence.get("docs") or {}
    positions = _paired_positions(occurrence, sources)
    locations: list[dict[str, Any]] = []
    for index, side in enumerate(("left", "right")):
        name = str(occurrence.get(f"{side}_file_name") or source.get(f"{side}_file_name") or "")
        if not name:
            continue
        for page_index, page in enumerate(positions[index][1]):
            if not str(page).isdigit() or int(page) <= 0:
                continue
            append_location(locations, make_location(
                document_identifier_id=(source.get(f"{side}_document_identifier") or
                                        source.get(f"{side}_document_identifier_id") or
                                        source.get(f"{side}_document_id")),
                file_name=name, page=int(page),
                bbox=(evidence.get(f"{side}_bbox") or evidence.get("bbox")) if page_index == 0 else None,
                text=_side_text(occurrence, side) or (docs.get(name) or {}).get("preview") or "",
            ))
    return locations


def _prune_nested_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer a specific matching span over a containing table/section rectangle."""
    def box(value: dict[str, Any]) -> tuple[float, ...]:
        raw = value.get("bbox")
        if not isinstance(raw, (list, tuple)) or len(raw) < 4:
            return ()
        try:
            return tuple(float(number) for number in raw[:4])
        except (TypeError, ValueError):
            return ()

    def contains(outer: tuple[float, ...], inner: tuple[float, ...]) -> bool:
        return bool(outer and inner and outer != inner and
                    outer[0] <= inner[0] and outer[1] <= inner[1] and
                    outer[2] >= inner[2] and outer[3] >= inner[3])

    result = []
    for index, location in enumerate(locations):
        identity = location.get("document_identifier_id") or location.get("file_name")
        own_box = box(location)
        own_text = _text(location.get("text"))
        nested = False
        if own_box and len(own_text) >= 10:
            for other_index, other in enumerate(locations):
                if index == other_index or identity != (other.get("document_identifier_id") or other.get("file_name")):
                    continue
                if location.get("page") != other.get("page"):
                    continue
                other_text = _text(other.get("text"))
                if len(other_text) < 10 or not (own_text in other_text or other_text in own_text):
                    continue
                if contains(own_box, box(other)):
                    nested = True
                    break
        if not nested:
            result.append(location)
    return result


def project_duplicate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return grouped review cards without changing a stored compact payload."""
    if not isinstance(payload, dict) or payload.get("review_projection_version") == PROJECTION_VERSION:
        return payload
    if not isinstance(payload.get("source_items"), dict):
        from .storage import compact_duplicate_payload
        payload = compact_duplicate_payload(payload)
    issues = payload.get("issues") or []
    if not isinstance(issues, list):
        return payload
    sources = payload.get("source_items") or {}
    if not isinstance(sources, dict):
        sources = {}
    entries: list[dict[str, Any]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        for index, raw in enumerate(issue.get("occurrences") or []):
            if not isinstance(raw, dict):
                continue
            occurrence = deepcopy(raw)
            occurrence.setdefault("left_file_name", (sources.get(_source_id(raw)) or {}).get("left_file_name"))
            occurrence.setdefault("right_file_name", (sources.get(_source_id(raw)) or {}).get("right_file_name"))
            entries.append({"occurrence": occurrence, "issue": issue, "index": index, "anchor": _anchor(occurrence)})
        if not issue.get("occurrences"):
            entries.append({"occurrence": None, "issue": issue, "index": 0, "anchor": ""})

    # First collapse multiple detectors describing the same text at the same pair of positions.
    canonical: list[dict[str, Any]] = []
    for entry in entries:
        occurrence = entry["occurrence"]
        found = None
        if occurrence:
            for candidate in canonical:
                representative = candidate["occurrence"]
                if representative and _same_physical_evidence(representative, occurrence, sources):
                    found = candidate
                    break
        if found is None:
            found = {"occurrence": occurrence, "members": []}
            canonical.append(found)
        found["members"].append(entry)
        if occurrence and found["occurrence"] is not occurrence:
            current = found["occurrence"]
            if _representative_rank(occurrence) < _representative_rank(current):
                found["occurrence"] = occurrence

    # Keep each existing card intact. Only join cards when they share a verified
    # content identity; splitting a broad historical card increases review work.
    parent = list(range(len(canonical)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    issue_owner: dict[int, int] = {}
    content_owner: dict[str, int] = {}
    for index, candidate in enumerate(canonical):
        anchors = {member["anchor"] for member in candidate["members"] if len(member["anchor"]) >= 10}
        for member in candidate["members"]:
            issue_identity = id(member["issue"])
            if issue_identity in issue_owner:
                union(index, issue_owner[issue_identity])
            else:
                issue_owner[issue_identity] = index
        for anchor in anchors:
            if anchor in content_owner:
                union(index, content_owner[anchor])
            else:
                content_owner[anchor] = index
    groups: dict[int, list[dict[str, Any]]] = {}
    for index, candidate in enumerate(canonical):
        groups.setdefault(find(index), []).append(candidate)

    projected: list[dict[str, Any]] = []
    doc_type = str(payload.get("document_type") or payload.get("source_result_key") or "")
    for candidates in groups.values():
        members = [member for candidate in candidates for member in candidate["members"]]
        original_issues = list({id(member["issue"]): member["issue"] for member in members}.values())
        content_keys = sorted({member["anchor"] for member in members if len(member["anchor"]) >= 10})
        risk = max((str(issue.get("risk_level") or "none") for issue in original_issues),
                   key=lambda value: RISK_ORDER.get(value, 0), default="none")
        score = max((float(issue.get("score_value") or 0) for issue in original_issues), default=0)
        file_names = list(dict.fromkeys(
            name for member in members
            for name in (member["issue"].get("files") or list((member["occurrence"] or {}).get("docs") or {}))
            if name
        ))
        source_ids = list(dict.fromkeys(
            str(identifier) for identifier in [
                *[member["occurrence"].get("source_item_id") for member in members if member["occurrence"]],
                *[identifier for issue in original_issues for identifier in issue.get("source_issue_ids") or []],
            ] if identifier
        ))
        participant_documents: list[dict[str, str]] = []
        seen_documents: set[str] = set()
        for source_id in source_ids:
            source = sources.get(source_id) or {}
            for side in ("left", "right"):
                name = str(source.get(f"{side}_file_name") or "")
                identifier = str(source.get(f"{side}_document_identifier") or
                                 source.get(f"{side}_document_identifier_id") or
                                 source.get(f"{side}_document_id") or "")
                identity = identifier or name
                if identity and identity not in seen_documents:
                    participant_documents.append({"document_identifier_id": identifier, "file_name": name})
                    seen_documents.add(identity)
        review_ids = list(dict.fromkeys(str(identifier) for issue in original_issues
            for identifier in (issue.get("source_review_issue_ids") or [issue.get("cluster_id")]) if identifier))
        ranges: dict[str, list[Any]] = {}
        previews: dict[str, list[str]] = {}
        file_urls: dict[str, Any] = {}
        tokens: list[str] = []
        typo_issues: list[dict[str, Any]] = []
        typo_candidates: list[dict[str, Any]] = []
        seen_typos: set[str] = set()
        for issue in original_issues:
            for name, values in (issue.get("doc_ranges_by_file") or {}).items():
                target = ranges.setdefault(name, [])
                for value in values or []:
                    if value not in target:
                        target.append(value)
            for name, values in (issue.get("doc_previews_by_file") or {}).items():
                target = previews.setdefault(name, [])
                for value in values or []:
                    if value not in target:
                        target.append(value)
            file_urls.update(issue.get("file_urls_by_file") or {})
            for token in issue.get("tokens") or []:
                if token not in tokens:
                    tokens.append(token)
            for field, target in (("short_duplicate_typo_issues", typo_issues),
                                  ("typo_review_candidates", typo_candidates)):
                for typo in issue.get(field) or []:
                    identity = str(typo.get("shared_id") or typo.get("source_evidence_id") or repr(typo))
                    if identity not in seen_typos:
                        target.append(typo)
                        seen_typos.add(identity)
        locations: list[dict[str, Any]] = []
        occurrences: list[dict[str, Any]] = []
        for candidate in candidates:
            representative = candidate["occurrence"]
            if not representative:
                continue
            occurrence = deepcopy(representative)
            representative_source = sources.get(_source_id(representative)) or {}
            for side in ("left", "right"):
                occurrence[f"{side}_document_identifier_id"] = (
                    representative_source.get(f"{side}_document_identifier") or
                    representative_source.get(f"{side}_document_identifier_id") or
                    representative_source.get(f"{side}_document_id")
                )
            occurrence["source_item_ids"] = list(dict.fromkeys(
                _source_id(member["occurrence"]) for member in candidate["members"]
                if member["occurrence"] and _source_id(member["occurrence"])))
            occurrence["source_evidence"] = [{
                "kind": member["occurrence"].get("kind"),
                "source_item_id": member["occurrence"].get("source_item_id"),
                "evidence": member["occurrence"].get("evidence"),
            } for member in candidate["members"] if member["occurrence"]]
            occurrences.append(occurrence)
            for location in _locations(representative, sources):
                append_location(locations, location)
        old_ids = sorted(str(issue.get("cluster_id") or "") for issue in original_issues)
        group_id = "dupgroup-" + hashlib.sha256((doc_type + "|" + "|".join(old_ids)).encode()).hexdigest()[:32]
        lead = original_issues[0]
        typo_check = dict(lead.get("typo_check") or {})
        typo_check["confirmed_count"] = len(typo_issues)
        typo_check["review_candidate_count"] = len(typo_candidates)
        if any((issue.get("typo_check") or {}).get("status") == "incomplete" for issue in original_issues):
            typo_check["status"] = "incomplete"
        result = {field: deepcopy(value) for field, value in lead.items()
                  if field not in {"occurrences", "locations", "source_issue_ids", "cluster_id"}}
        result.update({
            "cluster_id": group_id,
            "review_projection_version": PROJECTION_VERSION,
            "title": lead.get("title") or (content_keys[0][:65] if content_keys else "疑似重复内容"),
            "risk_level": risk,
            "status": "failed" if risk != "none" else ("unclear" if any(issue.get("review_only") for issue in original_issues) else "passed"),
            "review_only": any(issue.get("review_only") for issue in original_issues),
            "score_value": score,
            "score_display": str(score),
            "similarity": max((float(issue.get("similarity") or 0) for issue in original_issues), default=0),
            "files": file_names,
            "file_count": len(participant_documents) or len(file_names),
            "doc_ranges_by_file": ranges,
            "doc_previews_by_file": previews,
            "file_urls_by_file": file_urls,
            "tokens": tokens,
            "short_duplicate_typo_issues": typo_issues,
            "typo_review_candidates": typo_candidates,
            "typo_check": typo_check,
            "participants": file_names,
            "participant_documents": participant_documents,
            "source_review_issue_ids": review_ids,
            "source_issue_ids": source_ids,
            "source_issue_count": len(source_ids),
            "source_evidence_count": len(members),
            "occurrence_count": len(occurrences),
            "pair_scores": [{"source_review_issue_id": issue.get("cluster_id"),
                             "score_value": issue.get("score_value"), "risk_level": issue.get("risk_level"),
                             "metrics": issue.get("metrics")}
                            for issue in original_issues],
            "occurrences": occurrences,
            "locations": _prune_nested_locations(locations),
        })
        projected.append(result)
    summary = dict(payload.get("summary") or {})
    summary["cluster_count"] = len(projected)
    summary["suspicious_cluster_count"] = sum(issue.get("risk_level") != "none" for issue in projected)
    summary["high_risk_cluster_count"] = sum(issue.get("risk_level") == "high" for issue in projected)
    summary["medium_risk_cluster_count"] = sum(issue.get("risk_level") == "medium" for issue in projected)
    return {**payload, "review_projection_version": PROJECTION_VERSION, "summary": summary, "issues": projected}
