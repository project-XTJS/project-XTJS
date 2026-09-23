#!/usr/bin/env python3
"""Read-only report of broad document pages versus bounded business-review evidence pages."""

from __future__ import annotations

import argparse
import json
from unittest.mock import patch

from app.service.analysis.project_input_loader import ProjectAnalysisInputLoader
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.postgresql_service import PostgreSQLService


def page_numbers(payload: dict) -> list[int]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    return sorted({
        int(item["page"])
        for item in data.get("layout_sections") or []
        if isinstance(item, dict) and isinstance(item.get("page"), int) and item["page"] > 0
    })


def source_page_summary(payload: dict) -> dict:
    pages = page_numbers(payload)
    return {
        "source_page_count": len(pages),
        "source_page_range": [pages[0], pages[-1]] if pages else [],
    }


def page_ranges(pages: list[int]) -> list[str]:
    values = sorted(set(pages))
    if not values:
        return []
    result = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        result.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    result.append(str(start) if start == previous else f"{start}-{previous}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("project_identifier")
    args = parser.parse_args()

    db = PostgreSQLService()
    payload_data = ProjectAnalysisInputLoader(db).load(args.project_identifier)
    if not payload_data:
        raise SystemExit("project not found or has no analysis documents")
    records = list(payload_data.get("documents") or [])
    service = UnifiedBusinessReviewService(db_service=db)
    tender_record = next(
        record for record in records
        if isinstance(service._coerce_stored_payload(record.get("tender_content")), dict)
    )
    tender = service._coerce_stored_payload(tender_record["tender_content"])

    empty_evidence = lambda payload, pages: {  # noqa: E731
        "version": "scope-audit",
        "pages": {str(page): {"status": "ready", "spans": [], "issues": []} for page in pages},
    }
    with patch(
        "app.service.analysis.compliance.structured_consistency.evidence_for",
        side_effect=empty_evidence,
    ):
        skeletons = service.consistency_checker.build_template_skeleton(tender)

    expected = [
        {"title": item.get("title"), "text": item.get("reference_text")}
        for item in skeletons
        if item.get("title")
    ]
    report = {
        "project_identifier_id": args.project_identifier,
        "tender": {
            **source_page_summary(tender),
            "evidence_pages": sorted({
                page for skeleton in skeletons for page in skeleton.get("template_pages") or []
            }),
            "attachments": [
                {
                    "title": skeleton.get("title"),
                    "status": skeleton.get("template_scope_status"),
                    "pages": skeleton.get("template_pages") or [],
                }
                for skeleton in skeletons
            ],
        },
        "business_documents": [],
    }
    seen = set()
    for record in records:
        if service._normalize_project_document_role(record.get("relation_role")) != "business_bid":
            continue
        document_id = str(record.get("identifier_id") or "")
        if not document_id or document_id in seen:
            continue
        seen.add(document_id)
        content = service._coerce_stored_payload(record.get("content"))
        _, sections = service.consistency_checker._build_attachment_lookup(content, expected)
        attachments = []
        evidence_pages = []
        old_pages = []
        for skeleton in skeletons:
            raw_scope = service.consistency_checker._structured_engine._match_attachment(skeleton, sections)
            raw_section = raw_scope.get("section") if isinstance(raw_scope, dict) else None
            raw_pages = list((raw_section or {}).get("pages") or [])
            scope = service.consistency_checker._structured_engine.resolve_attachment_scope(skeleton, sections)
            pages = scope.get("evidence_pages") or []
            evidence_pages.extend(pages)
            old_pages.extend(raw_pages)
            attachments.append({
                "title": skeleton.get("title"),
                "status": scope.get("location_status"),
                "old_page_ranges": page_ranges(raw_pages),
                "evidence_page_ranges": page_ranges(pages),
                "excluded_page_ranges": page_ranges(sorted(set(raw_pages) - set(pages))),
                "candidates": [
                    {"title": item.get("title"), "page_ranges": page_ranges(item.get("pages") or [])}
                    for item in scope.get("candidates") or []
                ] if scope.get("location_status") != "matched" else [],
            })
        effective_pages = sorted(set(evidence_pages))
        report["business_documents"].append({
            "identifier_id": document_id,
            "file_name": record.get("file_name"),
            **source_page_summary(content),
            "old_page_ranges": page_ranges(old_pages),
            "evidence_pages": effective_pages,
            "evidence_page_ranges": page_ranges(effective_pages),
            "excluded_page_ranges": page_ranges(sorted(set(old_pages) - set(effective_pages))),
            "attachments": attachments,
        })
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
