from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.service.analysis.consistency import ConsistencyChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.verification import VerificationChecker
from app.service.postgresql_service import PostgreSQLService


class UnifiedBusinessReviewService:
    RESULT_SCHEMA_VERSION = "1.1"
    EXTRACTION_TABLE_SCHEMA_VERSION = "1.0"
    DEFAULT_RESULT_KEY = "unified_business_review"
    BUSINESS_RESULT_KEY = "business_bid_format_review"

    PAGE_KEYS = {"page", "page_no", "page_num", "page_index"}
    PAGE_LIST_KEYS = {"pages", "page_numbers", "page_nos"}
    ATTACHMENT_REF_RE = re.compile(r"附件\s*\d+(?:\s*[-－]\s*\d+)?")
    PAGE_REF_RE = re.compile(r"第\s*\d+\s*页")
    BUSINESS_FILE_RE = re.compile(r"[\s_-]*商务标\s*$")
    TECHNICAL_FILE_RE = re.compile(r"[\s_-]*技术标\s*$")
    CHECK_DISPLAY_ORDER = (
        "integrity_check",
        "consistency_check",
        "pricing_check",
        "itemized_pricing_check",
        "deviation_check",
        "verification_check",
    )

    def __init__(self, db_service: PostgreSQLService | None = None) -> None:
        self.db_service = db_service or PostgreSQLService()
        self.integrity_checker = IntegrityChecker()
        self.consistency_checker = ConsistencyChecker()
        self.reasonableness_checker = ReasonablenessChecker()
        self.itemized_checker = ItemizedPricingChecker()
        self.deviation_checker = DeviationChecker()
        self.verification_checker = VerificationChecker(None)

    def review_dataset(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
    ) -> dict[str, Any]:
        dataset = self._discover_dataset(dataset_dir)
        resolved_project_identifier = project_identifier or self._default_project_identifier(dataset["base_dir"])

        bidders: list[dict[str, Any]] = []
        for bidder in dataset["bidders"]:
            bidders.append(
                self._review_bidder(
                    tender_payload=dataset["tender"]["content"],
                    tender_meta=dataset["tender"]["meta"],
                    bidder=bidder,
                )
            )

        extraction_tables = self._build_review_extraction_tables(
            tender_payload=dataset["tender"]["content"],
            tender_meta=dataset["tender"]["meta"],
            bidder_sources=dataset["bidders"],
            bidder_reviews=bidders,
        )
        reading_guide = self._build_review_reading_guide(
            tender_meta=dataset["tender"]["meta"],
            bidders=bidders,
        )

        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "unified_business_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": resolved_project_identifier,
            "dataset": {
                "base_dir": str(dataset["base_dir"]),
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
            "extraction_tables": extraction_tables,
            "function_validation": self._summarize_function_validation(bidders),
            "summary": self._summarize_review(bidders),
            "bidders": bidders,
        }

    def persist_dataset_review(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
        result_key: str = DEFAULT_RESULT_KEY,
    ) -> dict[str, Any]:
        review = self.review_dataset(
            dataset_dir,
            project_identifier=project_identifier,
        )
        project = self._get_or_create_project(review["project_identifier_id"])
        result_record = self.db_service.upsert_project_result_item(
            project["identifier_id"],
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }

    def persist_uploaded_business_review(
        self,
        *,
        tender_file_name: str,
        tender_payload: dict[str, Any],
        tender_raw_bytes: bytes,
        business_bid_documents: list[dict[str, Any]],
        project_identifier: str | None = None,
        result_key: str = BUSINESS_RESULT_KEY,
    ) -> dict[str, Any]:
        project = self._ensure_project(project_identifier)
        review = self._review_uploaded_business_documents(
            tender_file_name=tender_file_name,
            tender_payload=tender_payload,
            tender_raw_bytes=tender_raw_bytes,
            business_bid_documents=business_bid_documents,
            project_identifier=project["identifier_id"],
        )
        result_record = self.db_service.upsert_project_result_item(
            project["identifier_id"],
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }

    def _review_uploaded_business_documents(
        self,
        *,
        tender_file_name: str,
        tender_payload: dict[str, Any],
        tender_raw_bytes: bytes,
        business_bid_documents: list[dict[str, Any]],
        project_identifier: str,
    ) -> dict[str, Any]:
        tender_document = self._load_uploaded_document(
            file_name=tender_file_name,
            raw_bytes=tender_raw_bytes,
            payload=tender_payload,
            role="tender",
            bidder_key=None,
        )

        bidders: list[dict[str, Any]] = []
        bidder_entries = []
        bidder_sources = []
        for business_doc in business_bid_documents:
            bidder_key = str(business_doc.get("bidder_key") or "").strip() or "unknown_bidder"
            business_document = self._load_uploaded_document(
                file_name=str(business_doc.get("file_name") or ""),
                raw_bytes=business_doc["raw_bytes"],
                payload=business_doc["payload"],
                role="business",
                bidder_key=bidder_key,
            )
            bidder_entries.append(
                {
                    "bidder_key": bidder_key,
                    "business": business_document["meta"],
                }
            )
            bidder_sources.append(
                {
                    "bidder_key": bidder_key,
                    "business": business_document,
                }
            )
            bidders.append(
                self._review_business_bidder(
                    tender_payload=tender_document["content"],
                    tender_meta=tender_document["meta"],
                    bidder_key=bidder_key,
                    business_payload=business_document["content"],
                    business_meta=business_document["meta"],
                )
            )

        extraction_tables = self._build_review_extraction_tables(
            tender_payload=tender_document["content"],
            tender_meta=tender_document["meta"],
            bidder_sources=bidder_sources,
            bidder_reviews=bidders,
        )
        reading_guide = self._build_review_reading_guide(
            tender_meta=tender_document["meta"],
            bidders=bidders,
        )

        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "business_bid_format_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": project_identifier,
            "dataset": {
                "input_mode": "uploaded_json_files",
                "tender": tender_document["meta"],
                "bidders": bidder_entries,
                "file_count": 1 + len(bidder_entries),
            },
            "reading_guide": reading_guide,
            "extraction_tables": extraction_tables,
            "function_validation": self._summarize_function_validation(bidders),
            "summary": self._summarize_review(bidders),
            "bidders": bidders,
        }

    def _discover_dataset(self, dataset_dir: str | Path) -> dict[str, Any]:
        base_dir = Path(dataset_dir).expanduser().resolve()
        if not base_dir.exists():
            raise FileNotFoundError(f"dataset_dir does not exist: {base_dir}")
        if not base_dir.is_dir():
            raise NotADirectoryError(f"dataset_dir is not a directory: {base_dir}")

        files = sorted(
            {
                path.resolve()
                for pattern in ("*.json", "*.JSON")
                for path in base_dir.glob(pattern)
                if path.is_file()
            },
            key=lambda item: item.name,
        )
        if not files:
            raise FileNotFoundError(f"no JSON files found under {base_dir}")

        tender_candidates: list[Path] = []
        bidder_docs: dict[str, dict[str, Any]] = {}

        for path in files:
            stem = path.stem.strip()
            if "招标" in stem:
                tender_candidates.append(path)
                continue

            role: str | None = None
            bidder_key = stem
            if "商务标" in stem:
                role = "business"
                bidder_key = self.BUSINESS_FILE_RE.sub("", stem).strip() or stem
            elif "技术标" in stem:
                role = "technical"
                bidder_key = self.TECHNICAL_FILE_RE.sub("", stem).strip() or stem

            if role is None:
                continue

            bidder_entry = bidder_docs.setdefault(
                bidder_key,
                {"bidder_key": bidder_key, "business_path": None, "technical_path": None},
            )
            bidder_entry[f"{role}_path"] = path

        if len(tender_candidates) != 1:
            raise ValueError(
                f"expected exactly one tender JSON file, found {len(tender_candidates)} under {base_dir}"
            )

        incomplete = [
            bidder_key
            for bidder_key, entry in bidder_docs.items()
            if not entry["business_path"] or not entry["technical_path"]
        ]
        if incomplete:
            raise ValueError(f"incomplete bidder document pairs: {', '.join(sorted(incomplete))}")

        tender_path = tender_candidates[0]
        dataset = {
            "base_dir": base_dir,
            "tender": self._load_document(tender_path, role="tender", bidder_key=None),
            "bidders": [],
        }

        for bidder_key in sorted(bidder_docs):
            entry = bidder_docs[bidder_key]
            dataset["bidders"].append(
                {
                    "bidder_key": bidder_key,
                    "business": self._load_document(entry["business_path"], role="business", bidder_key=bidder_key),
                    "technical": self._load_document(entry["technical_path"], role="technical", bidder_key=bidder_key),
                }
            )

        return dataset

    def _load_document(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        content = json.loads(path.read_text(encoding="utf-8-sig"))
        return {
            "content": content,
            "meta": self._build_file_meta(path, role=role, bidder_key=bidder_key),
        }

    def _load_uploaded_document(
        self,
        *,
        file_name: str,
        raw_bytes: bytes,
        payload: dict[str, Any],
        role: str,
        bidder_key: str | None,
    ) -> dict[str, Any]:
        return {
            "content": payload,
            "meta": self._build_uploaded_file_meta(
                file_name=file_name,
                raw_bytes=raw_bytes,
                payload=payload,
                role=role,
                bidder_key=bidder_key,
            ),
        }

    def _build_file_meta(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        raw_bytes = path.read_bytes()
        stat = path.stat()
        data_node = self._data_node(json.loads(raw_bytes.decode("utf-8-sig")))
        return {
            "role": role,
            "bidder_key": bidder_key,
            "file_name": path.name,
            "file_path": str(path),
            "file_size": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
        }

    def _build_uploaded_file_meta(
        self,
        *,
        file_name: str,
        raw_bytes: bytes,
        payload: dict[str, Any],
        role: str,
        bidder_key: str | None,
    ) -> dict[str, Any]:
        data_node = self._data_node(payload)
        return {
            "role": role,
            "bidder_key": bidder_key,
            "file_name": file_name,
            "file_path": None,
            "file_size": len(raw_bytes),
            "modified_at": self._utc_now_iso(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
            "source_type": "upload",
        }

    def _ensure_project(self, project_identifier: str | None) -> dict[str, Any]:
        normalized_identifier = (project_identifier or "").strip()
        if normalized_identifier:
            existing = self.db_service.get_project_by_identifier(normalized_identifier)
            if existing:
                return existing
            return self.db_service.create_project(normalized_identifier)
        return self.db_service.create_project()

    def _review_bidder(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder: dict[str, Any],
    ) -> dict[str, Any]:
        business_payload = bidder["business"]["content"]
        technical_payload = bidder["technical"]["content"]
        combined_payload = self._merge_bid_documents(business_payload, technical_payload)

        integrity_check = self._execute_check(
            check_code="integrity_check",
            check_name="商务标完整性审查",
            runner=lambda: self.integrity_checker.check_integrity(tender_payload, business_payload),
            normalizer=self._normalize_integrity,
        )
        consistency_check = self._execute_consistency_check(
            tender_payload=tender_payload,
            business_payload=business_payload,
            integrity_check=integrity_check,
        )

        checks = {
            "integrity_check": integrity_check,
            "consistency_check": consistency_check,
            "pricing_check": self._execute_check(
                check_code="pricing_check",
                check_name="报价合理性审查",
                runner=lambda: {
                    "self_check": self.reasonableness_checker.check_price_reasonableness(business_payload),
                    "tender_limit_check": self.reasonableness_checker.check_bid_price_against_tender_limit(
                        tender_payload,
                        business_payload,
                    ),
                },
                normalizer=self._normalize_pricing,
            ),
            "itemized_pricing_check": self._execute_check(
                check_code="itemized_pricing_check",
                check_name="分项报价表审查",
                runner=lambda: self.itemized_checker.check_itemized_logic(
                    business_payload,
                    tender_text=tender_payload,
                ),
                normalizer=self._normalize_itemized,
            ),
            "deviation_check": self._execute_check(
                check_code="deviation_check",
                check_name="偏离条款审查",
                runner=lambda: self.deviation_checker.check_technical_deviation(tender_payload, combined_payload),
                normalizer=self._normalize_deviation,
            ),
            "verification_check": self._execute_check(
                check_code="verification_check",
                check_name="签字盖章日期审查",
                runner=lambda: self.verification_checker.check_seal_and_date(tender_payload, business_payload),
                normalizer=self._normalize_verification,
            ),
        }

        bidder_name = self._extract_bidder_name(checks, bidder["bidder_key"])
        aggregate_issues = self._aggregate_bidder_issues(checks)
        summary = self._summarize_bidder_checks(checks)
        reading_guide = self._build_bidder_reading_guide(
            bidder_key=bidder["bidder_key"],
            bidder_name=bidder_name,
            summary=summary,
            checks=checks,
            tender_meta=tender_meta,
            business_meta=bidder["business"]["meta"],
            technical_meta=bidder["technical"]["meta"],
        )

        return {
            "bidder_key": bidder["bidder_key"],
            "bidder_name": bidder_name,
            "reading_guide": reading_guide,
            "documents": {
                "tender": tender_meta,
                "business": bidder["business"]["meta"],
                "technical": bidder["technical"]["meta"],
            },
            "summary": summary,
            "checks": checks,
            "issues": aggregate_issues,
        }

    def _review_business_bidder(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder_key: str,
        business_payload: dict[str, Any],
        business_meta: dict[str, Any],
    ) -> dict[str, Any]:
        integrity_check = self._execute_check(
            check_code="integrity_check",
            check_name="商务标完整性审查",
            runner=lambda: self.integrity_checker.check_integrity(tender_payload, business_payload),
            normalizer=self._normalize_integrity,
        )
        consistency_check = self._execute_consistency_check(
            tender_payload=tender_payload,
            business_payload=business_payload,
            integrity_check=integrity_check,
        )

        checks = {
            "integrity_check": integrity_check,
            "consistency_check": consistency_check,
            "pricing_check": self._execute_check(
                check_code="pricing_check",
                check_name="报价合理性审查",
                runner=lambda: {
                    "self_check": self.reasonableness_checker.check_price_reasonableness(business_payload),
                    "tender_limit_check": self.reasonableness_checker.check_bid_price_against_tender_limit(
                        tender_payload,
                        business_payload,
                    ),
                },
                normalizer=self._normalize_pricing,
            ),
            "itemized_pricing_check": self._execute_check(
                check_code="itemized_pricing_check",
                check_name="分项报价表审查",
                runner=lambda: self.itemized_checker.check_itemized_logic(
                    business_payload,
                    tender_text=tender_payload,
                ),
                normalizer=self._normalize_itemized,
            ),
            "verification_check": self._execute_check(
                check_code="verification_check",
                check_name="签字盖章日期审查",
                runner=lambda: self.verification_checker.check_seal_and_date(tender_payload, business_payload),
                normalizer=self._normalize_verification,
            ),
        }

        bidder_name = self._extract_bidder_name(checks, bidder_key)
        aggregate_issues = self._aggregate_bidder_issues(checks)
        summary = self._summarize_bidder_checks(checks)
        reading_guide = self._build_bidder_reading_guide(
            bidder_key=bidder_key,
            bidder_name=bidder_name,
            summary=summary,
            checks=checks,
            tender_meta=tender_meta,
            business_meta=business_meta,
        )

        return {
            "bidder_key": bidder_key,
            "bidder_name": bidder_name,
            "reading_guide": reading_guide,
            "documents": {
                "tender": tender_meta,
                "business": business_meta,
            },
            "summary": summary,
            "checks": checks,
            "issues": aggregate_issues,
        }

    def _build_review_extraction_tables(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder_sources: list[dict[str, Any]],
        bidder_reviews: list[dict[str, Any]],
    ) -> dict[str, Any]:
        source_by_key = {
            str(item.get("bidder_key") or "").strip(): item
            for item in bidder_sources
            if str(item.get("bidder_key") or "").strip()
        }
        tender_rows = self._build_tender_extraction_rows(
            tender_payload=tender_payload,
            bidder_reviews=bidder_reviews,
        )

        bidder_tables: list[dict[str, Any]] = []
        all_bid_rows: list[dict[str, Any]] = []
        for bidder in bidder_reviews:
            bidder_key = str(bidder.get("bidder_key") or "").strip()
            source = source_by_key.get(bidder_key) or {}
            business_payload = ((source.get("business") or {}).get("content")) if isinstance(source, dict) else None
            technical_payload = ((source.get("technical") or {}).get("content")) if isinstance(source, dict) else None
            rows = self._build_bid_extraction_rows(
                bidder=bidder,
                business_payload=business_payload if isinstance(business_payload, dict) else None,
                technical_payload=technical_payload if isinstance(technical_payload, dict) else None,
            )
            all_bid_rows.extend(rows)
            documents = bidder.get("documents") or {}
            bidder_tables.append(
                {
                    "bidder_key": bidder_key,
                    "bidder_name": bidder.get("bidder_name"),
                    "documents": {
                        "business_file_name": ((documents.get("business") or {}).get("file_name")),
                        "technical_file_name": ((documents.get("technical") or {}).get("file_name")),
                    },
                    "row_count": len(rows),
                    "check_row_counts": self._count_extraction_rows(rows),
                    "rows": rows,
                }
            )

        return {
            "schema_version": self.EXTRACTION_TABLE_SCHEMA_VERSION,
            "catalog": self._build_extraction_catalog(
                tender_rows=tender_rows,
                bid_rows=all_bid_rows,
            ),
            "tender_table": {
                "document": self._document_source_brief(
                    tender_meta,
                    purpose="tender_extraction_source",
                ),
                "row_count": len(tender_rows),
                "check_row_counts": self._count_extraction_rows(tender_rows),
                "rows": tender_rows,
            },
            "bidder_tables": bidder_tables,
            "summary": {
                "tender_row_count": len(tender_rows),
                "bidder_count": len(bidder_tables),
                "bid_row_count": len(all_bid_rows),
                "total_row_count": len(tender_rows) + len(all_bid_rows),
            },
        }

    def _build_tender_extraction_rows(
        self,
        *,
        tender_payload: dict[str, Any],
        bidder_reviews: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        requirements, attachment_mapping = TemplateExtractor.extract_requirements(tender_payload or {})
        for item_name in requirements:
            sequence = self._extract_requirement_sequence(item_name)
            attachment_refs = list(
                attachment_mapping.get(item_name)
                or attachment_mapping.get(sequence)
                or []
            )
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="tender",
                    document_role="tender",
                    check_code="integrity_check",
                    field_group="required_item",
                    field_name=item_name,
                    value={"attachment_refs": attachment_refs},
                    status="extracted",
                    expected_document_role="business",
                    evidence={"match_basis": "business_heading_title"},
                )
            )

        for template in TemplateExtractor.extract_consistency_templates(tender_payload or {}):
            title = str(template.get("title") or "").strip() or "unnamed_template"
            content_text = "\n".join(template.get("content") or [])
            body = self.consistency_checker._trim_non_body_lines(
                self.consistency_checker._strip_title_line(content_text, title)
            )
            anchors = self.consistency_checker._get_anchors(body)
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="tender",
                    document_role="tender",
                    check_code="consistency_check",
                    field_group="template_attachment",
                    field_name=title,
                    value={
                        "anchor_count": len(anchors),
                        "anchor_sample": anchors[:8],
                    },
                    status="extracted",
                    expected_document_role="business",
                    evidence={
                        "content_preview": self._trim_text(body or content_text, max_length=200),
                    },
                )
            )

        tender_limit = self.reasonableness_checker._extract_tender_max_limit(tender_payload)
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="tender",
                document_role="tender",
                check_code="pricing_check",
                field_group="price_constraint",
                field_name="tender_limit_or_budget",
                value=(
                    {
                        "raw_amount": tender_limit.get("raw_amount"),
                        "amount_yuan": tender_limit.get("amount_yuan"),
                        "keyword": tender_limit.get("keyword"),
                    }
                    if tender_limit
                    else None
                ),
                status="extracted" if tender_limit else "missing",
                page_refs=self._coerce_page_refs(tender_limit),
                expected_document_role="business",
                evidence=(
                    {
                        "context": self._trim_text(tender_limit.get("context"), max_length=200),
                    }
                    if tender_limit
                    else {"reason": "tender_limit_not_detected"}
                ),
            )
        )

        reference_items: list[dict[str, Any]] = []
        if bidder_reviews:
            first_checks = (bidder_reviews[0].get("checks") or {})
            itemized_raw = (first_checks.get("itemized_pricing_check") or {}).get("raw_result") or {}
            reference_items = list(((itemized_raw.get("evidence") or {}).get("reference_items")) or [])
        for item in reference_items:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="tender",
                    document_role="tender",
                    check_code="itemized_pricing_check",
                    field_group="reference_item",
                    field_name=str(item.get("label") or item.get("serial") or "reference_item"),
                    value={
                        "serial": item.get("serial"),
                        "name": item.get("name"),
                    },
                    status="extracted",
                    expected_document_role="business",
                    evidence=item,
                )
            )

        star_requirements = self.deviation_checker._extract_star_requirements(tender_payload)
        if star_requirements:
            for item in star_requirements:
                rows.append(
                    self._make_extraction_row(
                        row_index=len(rows) + 1,
                        document_side="tender",
                        document_role="tender",
                        check_code="deviation_check",
                        field_group="star_requirement",
                        field_name=str(item.get("requirement") or "star_requirement"),
                        value={
                            "requirement_id": item.get("requirement_id"),
                            "section_type": item.get("section_type"),
                            "chapter_title": item.get("chapter_title"),
                        },
                        status="extracted",
                        page_refs=self._coerce_page_refs(item),
                        expected_document_role="combined",
                    )
                )
        else:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="tender",
                    document_role="tender",
                    check_code="deviation_check",
                    field_group="star_requirement",
                    field_name="star_requirements",
                    value=[],
                    status="missing",
                    expected_document_role="combined",
                    evidence={"reason": "no_star_requirements_detected"},
                )
            )

        required_attachments = self.verification_checker._required_attachments(tender_payload)
        if required_attachments:
            for item in required_attachments:
                rows.append(
                    self._make_extraction_row(
                        row_index=len(rows) + 1,
                        document_side="tender",
                        document_role="tender",
                        check_code="verification_check",
                        field_group="required_attachment",
                        field_name=str(item.get("title") or "required_attachment"),
                        value=item.get("requirements") or {},
                        status="extracted",
                        expected_document_role="business",
                        evidence={
                            "attachment_number": item.get("attachment_number"),
                        },
                    )
                )
        else:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="tender",
                    document_role="tender",
                    check_code="verification_check",
                    field_group="required_attachment",
                    field_name="required_attachments",
                    value=[],
                    status="missing",
                    expected_document_role="business",
                    evidence={"reason": "no_signature_or_seal_attachment_detected"},
                )
            )

        deadline = self.verification_checker._deadline_from_doc(tender_payload)
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="tender",
                document_role="tender",
                check_code="verification_check",
                field_group="deadline",
                field_name="submission_deadline",
                value=(
                    {
                        "date": deadline["date"].isoformat(),
                        "text": deadline.get("text"),
                    }
                    if deadline
                    else None
                ),
                status="extracted" if deadline else "missing",
                page_refs=self._coerce_page_refs(deadline),
                expected_document_role="business",
                evidence=(
                    {"matched_text": deadline.get("text")}
                    if deadline
                    else {"reason": "deadline_not_detected"}
                ),
            )
        )

        return rows

    def _build_bid_extraction_rows(
        self,
        *,
        bidder: dict[str, Any],
        business_payload: dict[str, Any] | None,
        technical_payload: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        del technical_payload
        rows: list[dict[str, Any]] = []
        checks = bidder.get("checks") or {}

        integrity_raw = (checks.get("integrity_check") or {}).get("raw_result") or {}
        for item_name, detail in (integrity_raw.get("details") or {}).items():
            if not isinstance(detail, dict) or not detail.get("scored", True):
                continue
            detail_status = str(detail.get("status") or "").strip().lower()
            if detail.get("is_passed"):
                status = "found"
            elif "optional" in detail_status or "可选" in detail_status:
                status = "optional"
            else:
                status = "missing"
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="integrity_check",
                    field_group="required_item_match",
                    field_name=item_name,
                    value=str(detail.get("preview") or "").strip() or None,
                    status=status,
                    evidence={
                        "detail_status": detail.get("status"),
                        "category": detail.get("category"),
                    },
                )
            )

        consistency_raw = (checks.get("consistency_check") or {}).get("raw_result") or {}
        for segment in consistency_raw.get("evaluated_segments") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="consistency_check",
                    field_group="template_segment",
                    field_name=str(segment.get("name") or "template_segment"),
                    value={
                        "missing_anchors": list(segment.get("missing_anchors") or []),
                    },
                    status="pass" if segment.get("is_passed") else "fail",
                )
            )
        for segment in consistency_raw.get("skipped_segments") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="consistency_check",
                    field_group="template_segment",
                    field_name=str(segment.get("name") or "template_segment"),
                    value={
                        "missing_anchors": list(segment.get("missing_anchors") or []),
                    },
                    status="skipped",
                    evidence={"skip_reason": segment.get("skip_reason")},
                )
            )

        pricing_raw = (checks.get("pricing_check") or {}).get("raw_result") or {}
        bid_total = (
            self.reasonableness_checker._extract_bid_total_amount(business_payload)
            if business_payload
            else None
        )
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="bid",
                document_role="business",
                check_code="pricing_check",
                field_group="bid_total_amount",
                field_name="bid_total_amount",
                value=(
                    {
                        "raw_amount": bid_total.get("raw_amount"),
                        "amount_yuan": bid_total.get("amount_yuan"),
                    }
                    if bid_total
                    else None
                ),
                status="found" if bid_total else "missing",
                page_refs=self._coerce_page_refs(bid_total),
                evidence=(
                    {"context": self._trim_text(bid_total.get("context"), max_length=200)}
                    if bid_total
                    else {"reason": "bid_total_not_detected"}
                ),
            )
        )
        for subcheck_code, payload in (
            ("price_reasonableness", pricing_raw.get("self_check") or {}),
            ("tender_limit_check", pricing_raw.get("tender_limit_check") or {}),
        ):
            if not payload:
                continue
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="pricing_check",
                    field_group="pricing_subcheck",
                    field_name=subcheck_code,
                    value={
                        "type": payload.get("type"),
                        "summary": payload.get("summary"),
                    },
                    status=self._map_price_result(
                        payload.get("result"),
                        self._join_text(payload.get("summary")),
                    ),
                )
            )

        itemized_raw = (checks.get("itemized_pricing_check") or {}).get("raw_result") or {}
        itemized_evidence = itemized_raw.get("evidence") or {}
        for item in itemized_evidence.get("extracted_items") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="itemized_pricing_check",
                    field_group="itemized_amount",
                    field_name=str(item.get("label") or item.get("serial") or "itemized_amount"),
                    value={
                        "serial": item.get("serial"),
                        "amount": item.get("amount"),
                        "source": item.get("source"),
                        "section_anchor": item.get("section_anchor"),
                    },
                    status="found",
                    page_refs=self._coerce_page_refs(item.get("section_pages"), item),
                )
            )
        for total in itemized_evidence.get("total_candidates") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="itemized_pricing_check",
                    field_group="itemized_total",
                    field_name=str(total.get("label") or "itemized_total"),
                    value={
                        "amount": total.get("amount"),
                        "source": total.get("source"),
                    },
                    status="found",
                    page_refs=self._coerce_page_refs(total.get("section_pages"), total),
                )
            )
        for item in itemized_evidence.get("comparison_items") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="itemized_pricing_check",
                    field_group="comparison_item",
                    field_name=str(item.get("label") or item.get("serial") or "comparison_item"),
                    value={
                        "serial": item.get("serial"),
                        "name": item.get("name"),
                    },
                    status="found",
                    evidence=item,
                )
            )
        missing_item_check = (itemized_raw.get("checks") or {}).get("missing_item") or {}
        for item_name in missing_item_check.get("missing_items") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="itemized_pricing_check",
                    field_group="missing_item",
                    field_name=str(item_name),
                    value={"comparison_basis": missing_item_check.get("comparison_basis")},
                    status="missing",
                )
            )

        if "deviation_check" in checks:
            deviation_raw = (checks.get("deviation_check") or {}).get("raw_result") or {}
            match_results = deviation_raw.get("match_results") or []
            if match_results:
                for item in match_results:
                    response_section = str(item.get("response_section") or "").strip().lower()
                    if response_section not in {"business", "technical"}:
                        response_section = "combined"
                    rows.append(
                        self._make_extraction_row(
                            row_index=len(rows) + 1,
                            document_side="bid",
                            document_role=response_section,
                            check_code="deviation_check",
                            field_group="deviation_response",
                            field_name=str(item.get("requirement") or "deviation_response"),
                            value={
                                "requirement_id": item.get("requirement_id"),
                                "deviation_type": item.get("deviation_type"),
                                "response_evidence": item.get("response_evidence"),
                                "match_score": item.get("match_score"),
                                "response_section_title": item.get("response_section_title"),
                            },
                            status=str(item.get("response_status") or "missing"),
                            page_refs=self._coerce_page_refs(item),
                        )
                    )
            else:
                rows.append(
                    self._make_extraction_row(
                        row_index=len(rows) + 1,
                        document_side="bid",
                        document_role="combined",
                        check_code="deviation_check",
                        field_group="deviation_response",
                        field_name="deviation_response",
                        value={"summary": deviation_raw.get("summary")},
                        status="skipped"
                        if deviation_raw.get("deviation_status") == "no_star_requirements"
                        else "missing",
                    )
                )

        verification_raw = (checks.get("verification_check") or {}).get("raw_result") or {}
        bidder_name = str(
            verification_raw.get("bidder_name")
            or bidder.get("bidder_name")
            or ""
        ).strip()
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="bid",
                document_role="business",
                check_code="verification_check",
                field_group="bidder_identity",
                field_name="bidder_name",
                value=bidder_name or None,
                status="found" if bidder_name else "missing",
            )
        )
        seal_texts = list(verification_raw.get("seal_contents") or [])
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="bid",
                document_role="business",
                check_code="verification_check",
                field_group="seal_detection",
                field_name="seal_texts",
                value=seal_texts[:10],
                status="found" if seal_texts else "missing",
            )
        )
        signature_texts = list(verification_raw.get("signature_contents") or [])
        rows.append(
            self._make_extraction_row(
                row_index=len(rows) + 1,
                document_side="bid",
                document_role="business",
                check_code="verification_check",
                field_group="signature_detection",
                field_name="signature_texts",
                value=signature_texts[:10],
                status="found" if signature_texts else "missing",
            )
        )
        for title in verification_raw.get("skipped_missing_attachments") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="verification_check",
                    field_group="attachment_result",
                    field_name=str(title),
                    value=None,
                    status="missing",
                    evidence={"reason": "attachment_not_found_in_bid"},
                )
            )
        for item in verification_raw.get("attachment_results") or []:
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="verification_check",
                    field_group="attachment_result",
                    field_name=str(item.get("title") or "attachment_result"),
                    value={
                        "attachment_number": item.get("attachment_number"),
                        "matched_bid_title": item.get("matched_bid_title"),
                        "signature_status": (item.get("signature_check") or {}).get("status"),
                        "seal_status": (item.get("seal_check") or {}).get("status"),
                        "date_status": (item.get("date_check") or {}).get("status"),
                    },
                    status=str(item.get("status") or ("found" if item.get("found") else "missing")),
                    page_refs=self._coerce_page_refs(item.get("pages"), item),
                    evidence={"requirements": item.get("requirements") or {}},
                )
            )

        return rows

    def _build_extraction_catalog(
        self,
        *,
        tender_rows: list[dict[str, Any]],
        bid_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        catalog: dict[str, dict[str, set[str]]] = {}
        tender_groups: set[str] = set()
        bid_groups: set[str] = set()

        for row in tender_rows:
            check_code = str(row.get("check_code") or "")
            field_group = str(row.get("field_group") or "")
            if not check_code or not field_group:
                continue
            entry = catalog.setdefault(
                check_code,
                {"tender_field_groups": set(), "bid_field_groups": set()},
            )
            entry["tender_field_groups"].add(field_group)
            tender_groups.add(field_group)

        for row in bid_rows:
            check_code = str(row.get("check_code") or "")
            field_group = str(row.get("field_group") or "")
            if not check_code or not field_group:
                continue
            entry = catalog.setdefault(
                check_code,
                {"tender_field_groups": set(), "bid_field_groups": set()},
            )
            entry["bid_field_groups"].add(field_group)
            bid_groups.add(field_group)

        ordered_checks = {
            check_code: {
                "tender_field_groups": sorted(entry["tender_field_groups"]),
                "bid_field_groups": sorted(entry["bid_field_groups"]),
            }
            for check_code, entry in sorted(
                catalog.items(),
                key=lambda item: self._check_display_index(item[0]),
            )
        }
        return {
            "tender_field_groups": sorted(tender_groups),
            "bid_field_groups": sorted(bid_groups),
            "checks": ordered_checks,
        }

    def _count_extraction_rows(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            check_code = str(row.get("check_code") or "").strip()
            if not check_code:
                continue
            counts[check_code] = counts.get(check_code, 0) + 1
        return counts

    def _make_extraction_row(
        self,
        *,
        row_index: int,
        document_side: str,
        document_role: str,
        check_code: str,
        field_group: str,
        field_name: Any,
        value: Any,
        status: str,
        page_refs: list[int] | None = None,
        expected_document_role: str | None = None,
        evidence: Any | None = None,
    ) -> dict[str, Any]:
        row = {
            "row_id": f"{document_side}:{check_code}:{row_index:04d}",
            "document_side": document_side,
            "document_role": document_role,
            "check_code": check_code,
            "field_group": field_group,
            "field_name": str(field_name or field_group),
            "value": value,
            "status": status,
            "page_refs": list(page_refs or []),
        }
        if expected_document_role:
            row["expected_document_role"] = expected_document_role
        if evidence is not None:
            row["evidence"] = evidence
        return row

    def _coerce_page_refs(self, *values: Any) -> list[int]:
        pages: list[int] = []
        seen: set[int] = set()

        def add(value: Any) -> None:
            if isinstance(value, bool) or value is None:
                return
            if isinstance(value, int):
                if value not in seen:
                    seen.add(value)
                    pages.append(value)
                return
            if isinstance(value, str):
                text = value.strip()
                if text.isdigit():
                    add(int(text))
                return
            if isinstance(value, dict):
                for key in self.PAGE_KEYS:
                    if key in value:
                        add(value.get(key))
                for key in self.PAGE_LIST_KEYS:
                    if key in value:
                        add(value.get(key))
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    add(item)

        for value in values:
            add(value)
        return pages

    def _extract_requirement_sequence(self, item_name: Any) -> str:
        text = str(item_name or "").strip()
        match = re.match(r"^\s*([^.]+?)\s*\.", text)
        return match.group(1).strip() if match else text

    def _execute_consistency_check(
        self,
        *,
        tender_payload: dict[str, Any],
        business_payload: dict[str, Any],
        integrity_check: dict[str, Any],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        check_code = "consistency_check"
        check_name = "模板一致性审查"
        try:
            raw_segments = self.consistency_checker.compare_raw_data(tender_payload, business_payload)
            evaluated_segments, skipped_segments = self._filter_consistency_segments(
                raw_segments,
                integrity_check.get("raw_result"),
            )
            raw_result = {
                "evaluated_segments": evaluated_segments,
                "skipped_segments": skipped_segments,
                "original_segment_count": len(raw_segments) if isinstance(raw_segments, list) else 0,
            }
            normalized = self._normalize_consistency(raw_result)
            execution = {
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": execution,
                "validation": normalized["validation"],
                "review": normalized["review"],
                "metrics": normalized.get("metrics", {}),
                "issues": normalized.get("issues", self._empty_issue_bucket()),
                "raw_result": raw_result,
            }
        except Exception as exc:  # pragma: no cover - defensive wrapper
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            issue = self._issue(
                status="fail",
                title=f"{check_name}执行失败",
                message=str(exc),
                evidence={"error_type": exc.__class__.__name__},
            )
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": {
                    "status": "error",
                    "duration_ms": duration_ms,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                "validation": {
                    "status": "failed",
                    "reason": "审查模块执行异常，未产出可用结果。",
                },
                "review": {
                    "status": "unclear",
                    "summary": f"{check_name}执行失败，当前无法得出可靠结论。",
                },
                "metrics": {},
                "issues": {
                    "passed": [],
                    "failed": [issue],
                    "unclear": [],
                },
                "raw_result": None,
            }

    def _execute_check(
        self,
        *,
        check_code: str,
        check_name: str,
        runner: Callable[[], Any],
        normalizer: Callable[[Any], dict[str, Any]],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            raw_result = runner()
            normalized = normalizer(raw_result)
            execution = {
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": execution,
                "validation": normalized["validation"],
                "review": normalized["review"],
                "metrics": normalized.get("metrics", {}),
                "issues": normalized.get("issues", self._empty_issue_bucket()),
                "raw_result": raw_result,
            }
        except Exception as exc:  # pragma: no cover - defensive wrapper
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            issue = self._issue(
                status="fail",
                title=f"{check_name}执行失败",
                message=str(exc),
                evidence={"error_type": exc.__class__.__name__},
            )
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": {
                    "status": "error",
                    "duration_ms": duration_ms,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                "validation": {
                    "status": "failed",
                    "reason": "审查模块执行异常，未产出可用结果。",
                },
                "review": {
                    "status": "unclear",
                    "summary": f"{check_name}执行失败，当前无法得出可靠结论。",
                },
                "metrics": {},
                "issues": {
                    "passed": [],
                    "failed": [issue],
                    "unclear": [],
                },
                "raw_result": None,
            }

    def _normalize_integrity(self, raw: dict[str, Any]) -> dict[str, Any]:
        details = raw.get("details", {}) if isinstance(raw, dict) else {}
        score = raw.get("integrity_score") if isinstance(raw, dict) else None
        ignored_count = raw.get("ignored_item_count", 0) if isinstance(raw, dict) else 0

        passed = []
        failed = []
        for item_name, detail in details.items():
            detail = detail or {}
            if not detail.get("scored", True):
                continue
            preview = str(detail.get("preview") or "-")
            category = str(detail.get("category") or "")
            evidence = {
                "status": detail.get("status"),
                "preview": preview,
                "category": category,
            }
            if detail.get("is_passed"):
                passed.append(
                    self._issue(
                        status="pass",
                        title=item_name,
                        message=f"已找到，命中内容：{preview}",
                        evidence=evidence,
                    )
                )
            else:
                failed.append(
                    self._issue(
                        status="fail",
                        title=item_name,
                        message="未在商务标中找到该必备项。",
                        evidence=evidence,
                    )
                )

        total = (
            raw.get("scored_item_count")
            if isinstance(raw, dict) and isinstance(raw.get("scored_item_count"), int)
            else len(passed) + len(failed)
        )
        review_status = "pass" if not failed else "fail"
        summary = f"完整性得分 {score}，共校验 {total} 项，缺失 {len(failed)} 项。"
        if ignored_count:
            summary += f" 另有 {ignored_count} 个父级标题由子项覆盖，不单独计分。"
        return {
            "validation": {
                "status": "correct" if isinstance(details, dict) else "failed",
                "reason": "模块返回了完整性得分和逐项命中明细。",
            },
            "review": {
                "status": review_status,
                "summary": summary,
            },
            "metrics": {
                "integrity_score": score,
                "total_item_count": total,
                "passed_item_count": len(passed),
                "failed_item_count": len(failed),
                "ignored_item_count": ignored_count,
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": [],
            },
        }

    def _normalize_consistency(self, raw: Any) -> dict[str, Any]:
        skipped_segments = []
        original_segment_count = 0
        if isinstance(raw, dict):
            segments = raw.get("evaluated_segments", raw.get("segments", [])) or []
            skipped_segments = raw.get("skipped_segments", []) or []
            original_segment_count = int(raw.get("original_segment_count") or 0)
        else:
            segments = raw if isinstance(raw, list) else []
            original_segment_count = len(segments)
        passed = []
        failed = []

        for segment in segments:
            title = str(segment.get("name") or "未命名模板段")
            missing = segment.get("missing_anchors") or []
            evidence = {
                "missing_anchors": missing,
            }
            if segment.get("is_passed"):
                passed.append(
                    self._issue(
                        status="pass",
                        title=title,
                        message="模板关键锚点齐全。",
                        evidence=evidence,
                    )
                )
            else:
                failed.append(
                    self._issue(
                        status="fail",
                        title=title,
                        message=f"缺少模板锚点：{self._join_text(missing)}",
                        evidence=evidence,
                    )
                )

        has_results = bool(segments or skipped_segments)
        if skipped_segments:
            validation_status = "correct"
            validation_reason = "模块返回了逐模板段的一致性结果，并已根据完整性缺失跳过对应附件。"
        else:
            validation_status = "correct" if segments else "unclear"
            validation_reason = (
                "模块返回了逐模板段的缺漏锚点结果。"
                if segments
                else "未提取到可比较的模板段，需人工复核模板抽取是否成功。"
            )

        if failed:
            review_status = "fail"
        elif has_results:
            review_status = "pass"
        else:
            review_status = "unclear"

        if has_results:
            total_segments = original_segment_count or (len(segments) + len(skipped_segments))
            summary = (
                f"共比对 {total_segments} 个模板段，实际校验 {len(segments)} 个，"
                f"因完整性缺失跳过 {len(skipped_segments)} 个，通过 {len(passed)} 个，"
                f"存在缺漏 {len(failed)} 个。"
            )
        else:
            summary = "未提取到可比较的模板段。"

        return {
            "validation": {
                "status": validation_status,
                "reason": validation_reason,
            },
            "review": {
                "status": review_status,
                "summary": summary,
            },
            "metrics": {
                "template_segment_count": original_segment_count or len(segments),
                "evaluated_segment_count": len(segments),
                "skipped_segment_count": len(skipped_segments),
                "passed_segment_count": len(passed),
                "failed_segment_count": len(failed),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": [],
            },
        }

    def _filter_consistency_segments(
        self,
        raw_segments: Any,
        integrity_raw: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        segments = raw_segments if isinstance(raw_segments, list) else []
        evaluated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for segment in segments:
            skip_reason = self._match_integrity_failure_for_segment(segment, integrity_raw)
            if skip_reason is None:
                evaluated.append(segment)
                continue
            skipped.append(
                {
                    **copy.deepcopy(segment),
                    "skip_reason": skip_reason,
                }
            )

        return evaluated, skipped

    def _match_integrity_failure_for_segment(
        self,
        segment: dict[str, Any],
        integrity_raw: Any,
    ) -> dict[str, Any] | None:
        details = integrity_raw.get("details", {}) if isinstance(integrity_raw, dict) else {}
        segment_title = str(segment.get("name") or "")
        normalized_segment_title = self._normalize_match_text(segment_title)

        for item_name, detail in details.items():
            if not isinstance(detail, dict):
                continue
            if detail.get("is_passed") or not detail.get("scored", True):
                continue

            missing_tokens = self._integrity_missing_tokens(item_name, detail)
            if not missing_tokens:
                continue

            if any(token in normalized_segment_title for token in missing_tokens):
                return {
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_tokens": sorted(missing_tokens),
                }

        return None

    def _integrity_missing_tokens(self, item_name: str, detail: dict[str, Any]) -> set[str]:
        tokens: set[str] = set()
        status_text = str(detail.get("status") or "")
        normalized_item = self._normalize_match_text(item_name)
        normalized_status = self._normalize_match_text(status_text)

        for attachment_ref in self._extract_attachment_refs(item_name):
            tokens.add(self._normalize_match_text(attachment_ref))

        simplified_title = self._simplify_integrity_item_title(item_name)
        if simplified_title:
            tokens.add(self._normalize_match_text(simplified_title))

        if "法定代表人" in normalized_item and "证明书" in normalized_item and "授权委托书" in normalized_item:
            missing_certificate = "缺失证明书" in normalized_status or (
                "缺失" in normalized_status and "证明书" in normalized_status and "授权" in normalized_status
            )
            missing_authorization = "缺失授权委托书" in normalized_status or (
                "缺失" in normalized_status and "授权" in normalized_status and "证明书" in normalized_status
            )
            if missing_certificate:
                tokens.add(self._normalize_match_text("附件 7-1"))
                tokens.add(self._normalize_match_text("法定代表人资格证明书"))
            if missing_authorization:
                tokens.add(self._normalize_match_text("附件 7-2"))
                tokens.add(self._normalize_match_text("法定代表人授权委托书"))

        return {token for token in tokens if token}

    def _extract_attachment_refs(self, text: str) -> list[str]:
        refs = []
        for match in self.ATTACHMENT_REF_RE.findall(str(text or "")):
            refs.append(re.sub(r"\s+", " ", match).strip())
        return refs

    def _simplify_integrity_item_title(self, item_name: str) -> str:
        text = str(item_name or "").strip()
        text = re.sub(r"^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百]+)[.、]\s*", "", text)
        text = re.sub(r"（.*?）|\(.*?\)", "", text).strip()
        if not text or len(text) > 24:
            return ""
        if any(sep in text for sep in ("；", ";", "，", ",")):
            return ""
        return text

    def _normalize_match_text(self, text: str) -> str:
        return "".join(ch for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")

    def _normalize_pricing(self, raw: dict[str, Any]) -> dict[str, Any]:
        self_check = raw.get("self_check", {}) if isinstance(raw, dict) else {}
        tender_limit_check = raw.get("tender_limit_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        for subcheck_code, title, payload in (
            ("price_reasonableness", "投标总价自检", self_check),
            ("tender_limit_check", "招标限价校验", tender_limit_check),
        ):
            summary_text = self._join_text(payload.get("summary"))
            status = self._map_price_result(payload.get("result"), summary_text)
            issue = self._issue(
                status=status,
                title=title,
                message=summary_text or "未返回明确结论。",
                evidence={
                    "result": payload.get("result"),
                    "type": payload.get("type"),
                    "summary": payload.get("summary"),
                    "subcheck_code": subcheck_code,
                },
            )
            if status == "pass":
                passed.append(issue)
            elif status == "fail":
                failed.append(issue)
            else:
                unclear.append(issue)

        review_status = self._combine_review_status(
            [issue["status"] for issue in passed + failed + unclear]
        )
        return {
            "validation": {
                "status": "correct" if self_check or tender_limit_check else "unclear",
                "reason": "模块返回了报价自检和招标限价校验两部分结果。",
            },
            "review": {
                "status": review_status,
                "summary": "；".join(issue["message"] for issue in passed + failed + unclear if issue["message"]),
            },
            "metrics": {
                "passed_subcheck_count": len(passed),
                "failed_subcheck_count": len(failed),
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_itemized(self, raw: dict[str, Any]) -> dict[str, Any]:
        top_status = self._map_generic_status(raw.get("status"))
        checks = raw.get("checks", {}) if isinstance(raw, dict) else {}
        manual_review = raw.get("manual_review", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        subcheck_labels = {
            "row_arithmetic": "分项行算术校验",
            "sum_consistency": "分项汇总一致性校验",
            "duplicate_items": "疑似重复报价校验",
            "missing_item": "招标列项缺失校验",
        }

        for subcheck_code, payload in checks.items():
            sub_status = str(payload.get("status") or "").strip().lower()
            if sub_status == "not_applicable":
                continue

            normalized_status = self._map_generic_status(sub_status)
            label = subcheck_labels.get(subcheck_code, subcheck_code)
            message = self._summarize_itemized_subcheck(subcheck_code, payload)
            issue = self._issue(
                status=normalized_status,
                title=label,
                message=message,
                evidence=payload,
            )
            if normalized_status == "pass":
                passed.append(issue)
            elif normalized_status == "fail":
                failed.append(issue)
            else:
                unclear.append(issue)

        if manual_review.get("required"):
            unclear.append(
                self._issue(
                    status="unclear",
                    title="人工复核提示",
                    message="分项报价识别存在歧义，建议人工核对识别总价和未完整识别行。",
                    evidence=manual_review,
                )
            )

        validation_status = "correct"
        validation_reason = "模块返回了分项报价校验明细。"
        if top_status == "unclear":
            validation_status = "unclear"
            validation_reason = "模块已执行，但当前样本存在未完整识别的分项行，结论需人工复核。"

        return {
            "validation": {
                "status": validation_status,
                "reason": validation_reason,
            },
            "review": {
                "status": top_status,
                "summary": str(raw.get("summary") or "未返回分项报价结论。"),
            },
            "metrics": {
                "itemized_table_detected": raw.get("itemized_table_detected"),
                "passed_subcheck_count": len(passed),
                "failed_subcheck_count": len(failed),
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_deviation(self, raw: dict[str, Any]) -> dict[str, Any]:
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        missing_items = raw.get("missing_response_items", []) if isinstance(raw, dict) else []
        negative_items = raw.get("negative_deviation_items", []) if isinstance(raw, dict) else []
        unclear_items = raw.get("unclear_response_items", []) if isinstance(raw, dict) else []

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        if raw.get("deviation_status") == "no_star_requirements":
            passed.append(
                self._issue(
                    status="pass",
                    title="星标条款检查",
                    message=str(raw.get("summary") or "未发现带 ★ 的强制性要求。"),
                    evidence={
                        "core_requirements_count": raw.get("core_requirements_count"),
                        "deviation_tables": raw.get("deviation_tables"),
                    },
                )
            )

        for item in missing_items:
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "缺失响应条款",
                    message="未找到对应响应内容。",
                    evidence=item,
                )
            )
        for item in negative_items:
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "负偏离条款",
                    message=f"检测到负偏离：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=item,
                )
            )
        for item in unclear_items:
            unclear.append(
                self._issue(
                    status="unclear",
                    title=item.get("requirement") or "响应不明确条款",
                    message=f"响应内容不明确：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=item,
                )
            )

        if compliance_status == "pass" and not passed:
            passed.append(
                self._issue(
                    status="pass",
                    title="偏离条款校验",
                    message=str(raw.get("summary") or "偏离条款审查通过。"),
                    evidence={"stats": raw.get("stats")},
                )
            )

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了星标条款抽取、响应匹配和偏离分类结果。",
            },
            "review": {
                "status": compliance_status,
                "summary": str(raw.get("summary") or "未返回偏离条款结论。"),
            },
            "metrics": {
                "core_requirements_count": raw.get("core_requirements_count"),
                "missing_count": len(missing_items),
                "negative_deviation_count": len(negative_items),
                "unclear_deviation_count": len(unclear_items),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_verification(self, raw: dict[str, Any]) -> dict[str, Any]:
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        position_check = raw.get("position_check", {}) if isinstance(raw, dict) else {}
        date_check = raw.get("date_check", {}) if isinstance(raw, dict) else {}
        seal_company_check = raw.get("seal_company_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        missing_attachments = position_check.get("missing_attachments") or []
        missing_signature = position_check.get("missing_signature_attachments") or []
        pending_signature = position_check.get("pending_signature_attachments") or []
        missing_seal = position_check.get("missing_seal_attachments") or []
        missing_date = date_check.get("missing_date_attachments") or []
        late_date = date_check.get("late_date_attachments") or []

        if seal_company_check.get("status") == "pass":
            passed.append(
                self._issue(
                    status="pass",
                    title="公章与投标人匹配",
                    message="检测到的公章与投标人名称匹配。",
                    evidence=seal_company_check,
                )
            )
        elif seal_company_check:
            failed.append(
                self._issue(
                    status="fail",
                    title="公章与投标人匹配",
                    message="检测到的公章与投标人名称不匹配。",
                    evidence=seal_company_check,
                )
            )

        if position_check.get("status") == "pass":
            passed.append(
                self._issue(
                    status="pass",
                    title="签字盖章位置检查",
                    message="所有必需附件均已找到，未发现缺失签字或盖章。",
                    evidence=position_check,
                )
            )
        else:
            for attachment in missing_attachments:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="未找到要求签章的附件。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )
            for attachment in missing_signature:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少签字。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )
            for attachment in missing_seal:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少盖章。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )

        for attachment in pending_signature:
            unclear.append(
                self._issue(
                    status="unclear",
                    title=attachment,
                    message="签字字段处于待填写状态，建议人工复核。",
                    evidence={"attachment": attachment, "source": "position_check"},
                )
            )

        if date_check.get("status") == "pass":
            passed.append(
                self._issue(
                    status="pass",
                    title="落款日期检查",
                    message="已检测到落款日期，且均未晚于投标截止时间。",
                    evidence=date_check,
                )
            )
        else:
            for attachment in missing_date:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少落款日期。",
                        evidence={"attachment": attachment, "source": "date_check"},
                    )
                )
            for attachment in late_date:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件落款日期晚于招标截止时间。",
                        evidence={"attachment": attachment, "source": "date_check"},
                    )
                )

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了附件级签字、盖章、日期和公章匹配结果。",
            },
            "review": {
                "status": compliance_status,
                "summary": str(raw.get("summary") or "未返回签字盖章日期结论。"),
            },
            "metrics": {
                "required_attachment_count": raw.get("required_attachment_count"),
                "missing_attachment_count": len(missing_attachments),
                "missing_signature_count": len(missing_signature),
                "pending_signature_count": len(pending_signature),
                "missing_seal_count": len(missing_seal),
                "missing_date_count": len(missing_date),
                "late_date_count": len(late_date),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _aggregate_bidder_issues(self, checks: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        bucket = self._empty_issue_bucket()
        for check_code, check in checks.items():
            check_name = check["check_name"]
            for status_key in ("passed", "failed", "unclear"):
                for issue in check["issues"][status_key]:
                    bucket[status_key].append(
                        {
                            "check_code": check_code,
                            "check_name": check_name,
                            **issue,
                        }
                    )
        return bucket

    def _summarize_bidder_checks(self, checks: dict[str, Any]) -> dict[str, Any]:
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0}
        validation_status_counts = {"correct": 0, "failed": 0, "unclear": 0}
        execution_status_counts = {"ok": 0, "error": 0}

        for check in checks.values():
            review_status = check["review"]["status"]
            validation_status = check["validation"]["status"]
            execution_status = check["execution"]["status"]
            review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1
            validation_status_counts[validation_status] = validation_status_counts.get(validation_status, 0) + 1
            execution_status_counts[execution_status] = execution_status_counts.get(execution_status, 0) + 1

        return {
            "check_count": len(checks),
            "review_status_counts": review_status_counts,
            "validation_status_counts": validation_status_counts,
            "execution_status_counts": execution_status_counts,
            "overall_review_status": self._combine_review_status(list(review_status_counts.keys()), counts=review_status_counts),
            "overall_validation_status": self._combine_validation_status(validation_status_counts),
        }

    def _summarize_review(self, bidders: list[dict[str, Any]]) -> dict[str, Any]:
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0}
        validation_status_counts = {"correct": 0, "failed": 0, "unclear": 0}
        execution_status_counts = {"ok": 0, "error": 0}

        for bidder in bidders:
            for check in bidder["checks"].values():
                review_status = check["review"]["status"]
                validation_status = check["validation"]["status"]
                execution_status = check["execution"]["status"]
                review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1
                validation_status_counts[validation_status] = validation_status_counts.get(validation_status, 0) + 1
                execution_status_counts[execution_status] = execution_status_counts.get(execution_status, 0) + 1

        return {
            "bidder_count": len(bidders),
            "total_check_count": sum(review_status_counts.values()),
            "review_status_counts": review_status_counts,
            "validation_status_counts": validation_status_counts,
            "execution_status_counts": execution_status_counts,
        }

    def _summarize_function_validation(self, bidders: list[dict[str, Any]]) -> dict[str, Any]:
        function_summary: dict[str, Any] = {}
        for bidder in bidders:
            for check_code, check in bidder["checks"].items():
                entry = function_summary.setdefault(
                    check_code,
                    {
                        "check_name": check["check_name"],
                        "execution_status_counts": {"ok": 0, "error": 0},
                        "validation_status_counts": {"correct": 0, "failed": 0, "unclear": 0},
                        "review_status_counts": {"pass": 0, "fail": 0, "unclear": 0},
                    },
                )
                entry["execution_status_counts"][check["execution"]["status"]] += 1
                entry["validation_status_counts"][check["validation"]["status"]] += 1
                entry["review_status_counts"][check["review"]["status"]] += 1
        return function_summary

    def _build_response_overview(self, review: dict[str, Any]) -> dict[str, Any]:
        guide = review.get("reading_guide") or {}
        bidder_overview = guide.get("bidder_overview") or []
        return {
            "review_type": review.get("review_type"),
            "project_identifier_id": review.get("project_identifier_id"),
            "tender_file_name": guide.get("tender_file_name"),
            "bidder_count": len(bidder_overview),
            "review_status_counts": (review.get("summary") or {}).get("review_status_counts"),
            "recommended_fields": [
                "overview.bidder_overview",
                "review.reading_guide",
                "review.extraction_tables.catalog",
                "review.extraction_tables.tender_table",
                "review.extraction_tables.bidder_tables[].rows",
                "review.bidders[].reading_guide.check_navigation",
                "review.bidders[].checks",
            ],
            "bidder_overview": bidder_overview,
        }

    def _build_review_reading_guide(
        self,
        *,
        tender_meta: dict[str, Any],
        bidders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        bidder_overview = []
        for bidder in bidders:
            documents = bidder.get("documents") or {}
            business_meta = documents.get("business") or {}
            technical_meta = documents.get("technical") or {}
            checks = bidder.get("checks") or {}
            failed_check_codes = [
                check_code
                for check_code, check in checks.items()
                if (check.get("review") or {}).get("status") == "fail"
            ]
            bidder_overview.append(
                {
                    "bidder_key": bidder.get("bidder_key"),
                    "bidder_name": bidder.get("bidder_name"),
                    "business_file_name": business_meta.get("file_name"),
                    "technical_file_name": technical_meta.get("file_name"),
                    "overall_review_status": (bidder.get("summary") or {}).get("overall_review_status"),
                    "failed_check_codes": failed_check_codes,
                    "failed_check_names": [
                        checks[check_code].get("check_name")
                        for check_code in failed_check_codes
                        if check_code in checks
                    ],
                }
            )

        bidder_overview.sort(
            key=lambda item: (
                self._review_status_sort_key(item.get("overall_review_status")),
                str(item.get("bidder_key") or ""),
            )
        )
        return {
            "tender_file_name": tender_meta.get("file_name"),
            "bidder_count": len(bidders),
            "recommended_reading_order": [
                "1) overview.bidder_overview",
                "2) review.extraction_tables.catalog",
                "3) review.extraction_tables.tender_table.rows",
                "4) review.extraction_tables.bidder_tables[].rows",
                "5) review.bidders[].reading_guide.check_navigation",
                "6) review.bidders[].checks.<check_code>.source_context",
                "7) review.bidders[].checks.<check_code>.issues",
            ],
            "bidder_overview": bidder_overview,
        }

    def _build_bidder_reading_guide(
        self,
        *,
        bidder_key: str,
        bidder_name: str,
        summary: dict[str, Any],
        checks: dict[str, Any],
        tender_meta: dict[str, Any],
        business_meta: dict[str, Any],
        technical_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        check_navigation = []
        for check_code in self.CHECK_DISPLAY_ORDER:
            check = checks.get(check_code)
            if not isinstance(check, dict):
                continue
            source_context = self._build_check_source_context(
                check_code=check_code,
                tender_meta=tender_meta,
                business_meta=business_meta,
                technical_meta=technical_meta,
            )
            focus_sections = self._collect_check_focus_sections(check)
            source_context["focus_sections"] = focus_sections
            check["source_context"] = source_context
            check_navigation.append(
                {
                    "check_code": check_code,
                    "check_name": check.get("check_name"),
                    "status": (check.get("review") or {}).get("status"),
                    "summary": (check.get("review") or {}).get("summary"),
                    "source_documents": source_context["source_documents"],
                    "focus_scope": source_context["focus_scope"],
                    "focus_sections": focus_sections,
                    "top_findings": self._select_issue_highlights(check),
                }
            )

        check_navigation.sort(
            key=lambda item: (
                self._review_status_sort_key(item.get("status")),
                self._check_display_index(item.get("check_code")),
            )
        )
        return {
            "bidder_key": bidder_key,
            "bidder_name": bidder_name,
            "business_file_name": business_meta.get("file_name"),
            "technical_file_name": (technical_meta or {}).get("file_name"),
            "tender_file_name": tender_meta.get("file_name"),
            "overall_review_status": summary.get("overall_review_status"),
            "check_status_counts": summary.get("review_status_counts"),
            "check_navigation": check_navigation,
        }

    def _build_check_source_context(
        self,
        *,
        check_code: str,
        tender_meta: dict[str, Any],
        business_meta: dict[str, Any],
        technical_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if check_code == "integrity_check":
            return {
                "focus_scope": "对照招标文件中的商务标要求，检查商务标 OCR 结果里是否识别到必备附件和资格证明材料。",
                "source_documents": [
                    self._document_source_brief(tender_meta, purpose="requirement_source"),
                    self._document_source_brief(business_meta, purpose="recognized_business_content"),
                ],
            }
        if check_code == "consistency_check":
            return {
                "focus_scope": "对照招标文件模板锚点，检查商务标对应附件的模板一致性。",
                "source_documents": [
                    self._document_source_brief(tender_meta, purpose="template_source"),
                    self._document_source_brief(business_meta, purpose="recognized_attachment_content"),
                ],
            }
        if check_code == "pricing_check":
            return {
                "focus_scope": "检查商务标报价页和招标限价条款，确认总价书写与限价对比结果。",
                "source_documents": [
                    self._document_source_brief(business_meta, purpose="quoted_price_source"),
                    self._document_source_brief(tender_meta, purpose="tender_limit_reference"),
                ],
            }
        if check_code == "itemized_pricing_check":
            return {
                "focus_scope": "检查商务标分项报价表中的单价、数量、合计和汇总一致性。",
                "source_documents": [
                    self._document_source_brief(business_meta, purpose="itemized_pricing_source"),
                    self._document_source_brief(tender_meta, purpose="missing_item_reference"),
                ],
            }
        if check_code == "deviation_check":
            source_documents = [
                self._document_source_brief(tender_meta, purpose="requirement_source"),
                self._document_source_brief(business_meta, purpose="business_response_context"),
            ]
            if technical_meta:
                source_documents.append(
                    self._document_source_brief(technical_meta, purpose="technical_response_source")
                )
            return {
                "focus_scope": "对照招标文件要求，检查商务标/技术标中的响应与偏离情况。",
                "source_documents": source_documents,
            }
        return {
            "focus_scope": "检查商务标中的签字、盖章、落款日期，并对照招标截止时间。",
            "source_documents": [
                self._document_source_brief(business_meta, purpose="signature_seal_source"),
                self._document_source_brief(tender_meta, purpose="deadline_reference"),
            ],
        }

    def _document_source_brief(self, meta: dict[str, Any], *, purpose: str) -> dict[str, Any]:
        return {
            "role": meta.get("role"),
            "file_name": meta.get("file_name"),
            "bidder_key": meta.get("bidder_key"),
            "page_count": meta.get("page_count"),
            "purpose": purpose,
        }

    def _collect_check_focus_sections(self, check: dict[str, Any], *, max_items: int = 6) -> list[str]:
        focus_sections: list[str] = []
        focus_sections.extend(self._extract_focus_tokens((check.get("review") or {}).get("summary")))

        issues = check.get("issues") or {}
        ordered_issues = (
            list(issues.get("failed") or [])
            + list(issues.get("unclear") or [])
            + list(issues.get("passed") or [])
        )
        for issue in ordered_issues:
            focus_sections.extend(self._extract_focus_tokens(issue.get("title")))
            focus_sections.extend(self._extract_focus_tokens(issue.get("message")))
            focus_sections.extend(self._extract_focus_tokens_from_evidence(issue.get("evidence")))
            simplified_title = self._simplify_issue_title(issue.get("title"))
            if simplified_title:
                focus_sections.append(simplified_title)

        return self._unique_texts(focus_sections)[:max_items]

    def _extract_focus_tokens_from_evidence(self, evidence: Any) -> list[str]:
        tokens: list[str] = []
        if isinstance(evidence, dict):
            for key in (
                "preview",
                "summary",
                "attachment",
                "matched_deadline_text",
                "missing_attachments",
                "missing_signature_attachments",
                "pending_signature_attachments",
                "missing_seal_attachments",
                "missing_date_attachments",
                "late_date_attachments",
            ):
                if key in evidence:
                    tokens.extend(self._extract_focus_tokens(evidence.get(key)))
        elif isinstance(evidence, list):
            for item in evidence:
                tokens.extend(self._extract_focus_tokens(item))
        else:
            tokens.extend(self._extract_focus_tokens(evidence))
        return tokens

    def _extract_focus_tokens(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            tokens: list[str] = []
            for item in value:
                tokens.extend(self._extract_focus_tokens(item))
            return tokens

        text = str(value).strip()
        if not text:
            return []

        tokens = []
        tokens.extend(self._extract_attachment_refs(text))
        tokens.extend(match.strip() for match in self.PAGE_REF_RE.findall(text))
        return tokens

    def _select_issue_highlights(self, check: dict[str, Any], *, max_items: int = 2) -> list[dict[str, Any]]:
        issues = check.get("issues") or {}
        ordered_issues = (
            list(issues.get("failed") or [])
            + list(issues.get("unclear") or [])
            + list(issues.get("passed") or [])
        )
        highlights = []
        for issue in ordered_issues[:max_items]:
            highlights.append(
                {
                    "status": issue.get("status"),
                    "title": issue.get("title"),
                    "message": self._trim_text(issue.get("message"), max_length=120),
                }
            )
        return highlights

    def _simplify_issue_title(self, title: Any) -> str:
        text = self._simplify_integrity_item_title(str(title or ""))
        if text:
            return text
        raw_text = str(title or "").strip()
        raw_text = re.sub(r"^\s*(?:\d+|[A-Z])[.、\s]*", "", raw_text)
        raw_text = re.sub(r"[（(].*?[）)]", "", raw_text).strip()
        if 2 <= len(raw_text) <= 24:
            return raw_text
        return ""

    def _unique_texts(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        unique: list[str] = []
        for value in values:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique.append(normalized)
        return unique

    def _trim_text(self, value: Any, *, max_length: int) -> str:
        text = str(value or "").strip()
        if len(text) <= max_length:
            return text
        return f"{text[: max_length - 3].rstrip()}..."

    def _check_display_index(self, check_code: Any) -> int:
        text = str(check_code or "")
        if text in self.CHECK_DISPLAY_ORDER:
            return self.CHECK_DISPLAY_ORDER.index(text)
        return len(self.CHECK_DISPLAY_ORDER)

    def _review_status_sort_key(self, status: Any) -> int:
        text = str(status or "").strip().lower()
        order = {"fail": 0, "unclear": 1, "pass": 2}
        return order.get(text, 3)

    def _merge_bid_documents(self, business_payload: dict[str, Any], technical_payload: dict[str, Any]) -> dict[str, Any]:
        business_data = self._data_node(business_payload)
        technical_data = self._data_node(technical_payload)

        page_offset = self._page_count(business_data)
        merged_data: dict[str, Any] = {}
        keys = set(business_data.keys()) | set(technical_data.keys())

        for key in keys:
            left = business_data.get(key)
            right = technical_data.get(key)
            if isinstance(left, list) or isinstance(right, list):
                left_list = left if isinstance(left, list) else []
                right_list = right if isinstance(right, list) else []
                merged_data[key] = copy.deepcopy(left_list) + self._offset_page_refs(right_list, page_offset, parent_key=key)
            elif left is not None:
                merged_data[key] = copy.deepcopy(left)
            else:
                merged_data[key] = self._offset_page_refs(right, page_offset, parent_key=key)

        return {"data": merged_data}

    def _offset_page_refs(self, value: Any, offset: int, *, parent_key: str | None = None) -> Any:
        if offset <= 0:
            return copy.deepcopy(value)

        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key in self.PAGE_KEYS and isinstance(item, int):
                    result[key] = item + offset
                elif key in self.PAGE_LIST_KEYS and isinstance(item, list):
                    result[key] = [member + offset if isinstance(member, int) else copy.deepcopy(member) for member in item]
                else:
                    result[key] = self._offset_page_refs(item, offset, parent_key=key)
            return result

        if isinstance(value, list):
            if parent_key in self.PAGE_LIST_KEYS:
                return [member + offset if isinstance(member, int) else copy.deepcopy(member) for member in value]
            return [self._offset_page_refs(member, offset, parent_key=parent_key) for member in value]

        return copy.deepcopy(value)

    def _page_count(self, data_node: dict[str, Any]) -> int:
        pages = data_node.get("pages") or []
        if isinstance(pages, list) and pages:
            return len(pages)

        max_page = 0
        for collection_key in ("layout_sections", "logical_tables", "native_tables"):
            for item in data_node.get(collection_key, []) or []:
                max_page = max(max_page, self._max_page_in_payload(item))
        return max_page

    def _max_page_in_payload(self, payload: Any) -> int:
        if isinstance(payload, dict):
            values = []
            for key, value in payload.items():
                if key in self.PAGE_KEYS and isinstance(value, int):
                    values.append(value)
                elif key in self.PAGE_LIST_KEYS and isinstance(value, list):
                    values.extend(member for member in value if isinstance(member, int))
                else:
                    values.append(self._max_page_in_payload(value))
            return max(values or [0])
        if isinstance(payload, list):
            return max((self._max_page_in_payload(item) for item in payload), default=0)
        return 0

    def _extract_bidder_name(self, checks: dict[str, Any], fallback: str) -> str:
        verification_raw = checks["verification_check"].get("raw_result") or {}
        bidder_name = str(verification_raw.get("bidder_name") or "").strip()
        return bidder_name or fallback

    def _summarize_itemized_subcheck(self, subcheck_code: str, payload: dict[str, Any]) -> str:
        if subcheck_code == "row_arithmetic":
            return f"存在 {payload.get('issue_count', 0)} 条算术错误，另有 {payload.get('unresolved_count', 0)} 条未解析行。"
        if subcheck_code == "sum_consistency":
            return (
                f"计算合计 {payload.get('calculated_total')}，声明总价 {payload.get('declared_total')}，"
                f"差额 {payload.get('difference')}。"
            )
        if subcheck_code == "duplicate_items":
            return f"检测到 {payload.get('issue_count', 0)} 组疑似重复报价项。"
        if subcheck_code == "missing_item":
            missing_items = payload.get("missing_items") or []
            return f"检测到 {len(missing_items)} 个疑似缺失列项。"
        return str(payload.get("status") or "未返回详细说明。")

    def _map_generic_status(self, raw_status: Any) -> str:
        text = str(raw_status or "").strip().lower()
        if text in {"pass", "passed", "ok", "success", "合格"}:
            return "pass"
        if text in {"fail", "failed", "error", "不合格"}:
            return "fail"
        return "unclear"

    def _map_price_result(self, result_value: Any, summary_text: str) -> str:
        result_text = str(result_value or "").strip()
        combined = f"{result_text} {summary_text}".strip()
        if "合格" in combined or re.search(r"\bpass\b", combined, re.IGNORECASE):
            return "pass"
        fail_keywords = ("不合格", "超出", "异常", "失败", "错误")
        if any(keyword in combined for keyword in fail_keywords):
            return "fail"
        unclear_keywords = ("未识别", "未找到", "无法", "暂无法", "人工复核", "待复核")
        if any(keyword in combined for keyword in unclear_keywords):
            return "unclear"
        return "unclear"

    def _combine_review_status(
        self,
        statuses: list[str],
        *,
        counts: dict[str, int] | None = None,
    ) -> str:
        if counts:
            if counts.get("fail", 0) > 0:
                return "fail"
            if counts.get("unclear", 0) > 0:
                return "unclear"
            return "pass"

        if any(status == "fail" for status in statuses):
            return "fail"
        if any(status == "unclear" for status in statuses):
            return "unclear"
        return "pass"

    def _combine_validation_status(self, counts: dict[str, int]) -> str:
        if counts.get("failed", 0) > 0:
            return "failed"
        if counts.get("unclear", 0) > 0:
            return "unclear"
        return "correct"

    def _default_project_identifier(self, dataset_dir: Path) -> str:
        digest = hashlib.sha1(str(dataset_dir).encode("utf-8")).hexdigest()[:10]
        return f"unified_business_review_{digest}"

    def _get_or_create_project(self, identifier_id: str) -> dict[str, Any]:
        project = self.db_service.get_project_by_identifier(identifier_id)
        if project:
            return project
        return self.db_service.create_project(identifier_id)

    def _data_node(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _join_text(self, value: Any) -> str:
        if isinstance(value, list):
            return "；".join(str(item) for item in value if item is not None and str(item).strip())
        if value is None:
            return ""
        return str(value)

    def _issue(
        self,
        *,
        status: str,
        title: str,
        message: str,
        evidence: Any | None = None,
    ) -> dict[str, Any]:
        severity = {
            "pass": "info",
            "fail": "error",
            "unclear": "warning",
        }[status]
        issue = {
            "status": status,
            "severity": severity,
            "title": str(title),
            "message": str(message),
        }
        if evidence is not None:
            issue["evidence"] = evidence
        return issue

    def _empty_issue_bucket(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "passed": [],
            "failed": [],
            "unclear": [],
        }

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
