# unified/extraction_tables.py
"""
统一商务标审查 - 抽数表构建 Mixin

生成审查过程中的结构化抽取数据，包括招标方抽取行、投标方抽取行、
以及按检查项分组的目录概要。
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from app.service.analysis.compliance.template_extractor import TemplateExtractor


class ExtractionTablesMixin:
    """
    抽数表构建相关的所有方法。

    依赖：
    - 实例属性：reasonableness_checker, consistency_checker, deviation_checker, verification_checker
    - 常量：PAGE_KEYS, PAGE_LIST_KEYS, ATTACHMENT_REF_RE, EXTRACTION_TABLE_SCHEMA_VERSION
    """

    reasonableness_checker: Any
    consistency_checker: Any
    deviation_checker: Any
    verification_checker: Any
    PAGE_KEYS: set
    PAGE_LIST_KEYS: set
    ATTACHMENT_REF_RE: Any
    EXTRACTION_TABLE_SCHEMA_VERSION: str

    def _build_review_extraction_tables(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder_sources: list[dict[str, Any]],
        bidder_reviews: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """组装完整的抽数表（招标方 + 各投标方）。"""
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

    # 招标方抽数行
    def _build_tender_extraction_rows(
        self,
        *,
        tender_payload: dict[str, Any],
        bidder_reviews: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """从招标文件中提取各审查模块需要的参照数据。"""
        rows: list[dict[str, Any]] = []

        # 完整性检查项
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

        # 一致性检查项：招标侧按结构化骨架元素逐行展示和编辑。
        for attachment in self.consistency_checker.build_template_skeleton(
            tender_payload or {}
        ):
            for skeleton_item in attachment.get("items") or []:
                if not isinstance(skeleton_item, dict):
                    continue
                locations = [
                    location
                    for location in skeleton_item.get("source_locations") or []
                    if isinstance(location, dict)
                ]
                rows.append(
                    self._make_extraction_row(
                        row_index=len(rows) + 1,
                        document_side="tender",
                        document_role="tender",
                        check_code="consistency_check",
                        field_group="template_skeleton_item",
                        field_name=str(
                            skeleton_item.get("label")
                            or skeleton_item.get("item_id")
                            or "template_skeleton_item"
                        ),
                        value={
                            key: skeleton_item.get(key)
                            for key in (
                                "item_id",
                                "kind",
                                "label",
                                "reference_text",
                                "required",
                                "enabled",
                                "confirmation_status",
                                "source",
                                "extraction_confidence",
                            )
                        },
                        status=(
                            "confirmed"
                            if skeleton_item.get("source") == "manual"
                            else "extracted"
                        ),
                        page_refs=self._coerce_page_refs(locations),
                        expected_document_role="business",
                        evidence={
                            "attachment_key": attachment.get("attachment_key"),
                            "attachment_number": attachment.get("attachment_number"),
                            "attachment_title": attachment.get("title"),
                        },
                        locations=locations,
                    )
                )

        # 报价限价检查项
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
                        "amount_yuan": tender_limit.get("amount_yuan"),
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
                locations=(tender_limit.get("locations") or []) if tender_limit else None,
            )
        )

        # 分项报价参考项
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

        # 偏离条款检查项
        # Star-clause deviation checks are handled by the standalone deviation review.

        # 签章验证检查项
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

        # 截止日期检查项
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

    # 投标方抽数行
    def _build_bid_extraction_rows(
        self,
        *,
        bidder: dict[str, Any],
        business_payload: dict[str, Any] | None,
        technical_payload: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        """从一个投标人的审查结果中生成标准化的抽样数据行。"""
        del technical_payload  # 当前版本暂不使用技术标 payload
        rows: list[dict[str, Any]] = []
        checks = bidder.get("checks") or {}

        # 完整性
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

        # 一致性
        consistency_raw = (checks.get("consistency_check") or {}).get("raw_result") or {}
        for segment in consistency_raw.get("evaluated_segments") or []:
            missing_anchors = list(segment.get("missing_anchors") or [])
            unfilled_fields = list(segment.get("unfilled_fields") or [])
            segment_locations = [location for location in (segment.get("locations") or []) if isinstance(location, dict)]
            status = str(segment.get("status") or "").strip().lower()
            if status not in {"pass", "missing", "unclear", "skipped"}:
                status = "pass" if segment.get("is_passed") else (
                    "missing" if missing_anchors or unfilled_fields else "unclear"
                )
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="consistency_check",
                    field_group="template_segment",
                    field_name=str(segment.get("name") or "template_segment"),
                    value={
                        "missing_anchors": missing_anchors,
                        "unfilled_fields": unfilled_fields,
                        "pages": list(segment.get("pages") or []),
                        "template_text": segment.get("template_text") or "",
                        "bid_text": segment.get("bid_text") or "",
                        "difference_items": list(segment.get("difference_items") or []),
                        "difference_summary": segment.get("difference_summary") or "",
                        "element_results": list(segment.get("element_results") or []),
                        "template_attachment_locations": segment.get("template_attachment_locations")
                        or segment.get("template_locations")
                        or [],
                        "attachment_match": segment.get("attachment_match") or {},
                        "engine_version": segment.get("engine_version"),
                        "model_status": segment.get("model_status") or {},
                        "manual_status": status,
                    },
                    status=status,
                    page_refs=self._coerce_page_refs(segment.get("pages"), segment_locations, segment),
                    evidence={
                        "template_attachment_locations": segment.get("template_attachment_locations")
                        or segment.get("template_locations")
                        or [],
                        "template_locations": segment.get("template_locations") or [],
                    },
                    locations=segment_locations,
                )
            )
        for segment in consistency_raw.get("skipped_segments") or []:
            segment_locations = [location for location in (segment.get("locations") or []) if isinstance(location, dict)]
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
                        "unfilled_fields": list(segment.get("unfilled_fields") or []),
                        "pages": list(segment.get("pages") or []),
                        "template_text": segment.get("template_text") or "",
                        "bid_text": segment.get("bid_text") or "",
                        "difference_items": list(segment.get("difference_items") or []),
                        "difference_summary": segment.get("difference_summary") or "",
                        "element_results": list(segment.get("element_results") or []),
                        "template_attachment_locations": segment.get("template_attachment_locations")
                        or segment.get("template_locations")
                        or [],
                        "attachment_match": segment.get("attachment_match") or {},
                        "engine_version": segment.get("engine_version"),
                        "model_status": segment.get("model_status") or {},
                        "manual_status": "skipped",
                    },
                    status="skipped",
                    page_refs=self._coerce_page_refs(segment.get("pages"), segment_locations, segment),
                    evidence={
                        "skip_reason": segment.get("skip_reason"),
                        "template_attachment_locations": segment.get("template_attachment_locations")
                        or segment.get("template_locations")
                        or [],
                        "template_locations": segment.get("template_locations") or [],
                    },
                    locations=segment_locations,
                )
            )

        # 报价合理性
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
                    {"raw_amount": bid_total.get("raw_amount"), "amount_yuan": bid_total.get("amount_yuan")}
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
            field_group = (
                "price_case_consistency"
                if subcheck_code == "price_reasonableness"
                else "price_limit_comparison"
            )
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="combined" if subcheck_code == "tender_limit_check" else "business",
                    check_code="pricing_check",
                    field_group=field_group,
                    field_name=subcheck_code,
                    value={
                        "type": payload.get("type"),
                        "result": payload.get("result"),
                        "status": self._map_price_result(payload.get("result"), self._join_text(payload.get("summary"))),
                        "summary": payload.get("summary"),
                        "amount_yuan": payload.get("amount_yuan"),
                        "raw_amount": payload.get("raw_amount"),
                        "capital_amount": payload.get("capital_amount"),
                        "capital_raw_amount": payload.get("capital_raw_amount"),
                        "case_consistency_status": payload.get("case_consistency_status"),
                        "case_consistency_summary": payload.get("case_consistency_summary"),
                    },
                    status=self._map_price_result(payload.get("result"), self._join_text(payload.get("summary"))),
                    page_refs=self._coerce_page_refs(payload.get("pages"), payload.get("locations")),
                    locations=[location for location in (payload.get("locations") or []) if isinstance(location, dict)],
                )
            )

        # 分项报价
        itemized_raw = (checks.get("itemized_pricing_check") or {}).get("raw_result") or {}
        itemized_evidence = itemized_raw.get("evidence") or {}
        for item in itemized_evidence.get("extracted_items") or []:
            itemized_value = self._build_itemized_amount_value(item)
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="itemized_pricing_check",
                    field_group="itemized_amount",
                    field_name=str(item.get("label") or item.get("serial") or "itemized_amount"),
                    value=itemized_value,
                    status=str(itemized_value.get("row_status") or "found"),
                    page_refs=self._coerce_page_refs(item.get("section_pages"), item),
                )
            )
        itemized_sum_check = (itemized_raw.get("checks") or {}).get("sum_consistency") or {}
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
                        "amount_yuan": total.get("amount"),
                        "declared_total": total.get("amount"),
                        "calculated_total": itemized_sum_check.get("calculated_total"),
                        "difference": itemized_sum_check.get("difference"),
                        "matched_total_label": itemized_sum_check.get("matched_total_label"),
                        "summary_status": itemized_sum_check.get("status"),
                        "raw_amount": total.get("source"),
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
                    value={"serial": item.get("serial"), "name": item.get("name")},
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

        # 偏离条款
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

        # 签章验证
        verification_raw = (checks.get("verification_check") or {}).get("raw_result") or {}
        bidder_name = str(verification_raw.get("bidder_name") or bidder.get("bidder_name") or "").strip()
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
            signature_evidence = self._attachment_signature_evidence_texts(item)
            signature_check = item.get("signature_check") or {}
            signature_status = signature_check.get("status") if isinstance(signature_check, dict) else None
            signature_parse_status = None
            if signature_evidence:
                signature_parse_status = "parsed"
            elif str(signature_status or "").strip().lower() in {"pass", "found", "pending"}:
                signature_parse_status = "unparsed"
            seal_evidence = self._attachment_seal_evidence_texts(item)
            date_check = item.get("date_check") or {}
            date_text = None
            deadline_date = None
            deadline_text = None
            deadline_page = None
            deadline_locations = []
            if isinstance(date_check, dict):
                date_text = date_check.get("matched_sign_text") or date_check.get("sign_date")
                deadline_date = date_check.get("deadline_date")
                deadline_text = date_check.get("matched_deadline_text")
                deadline_page = date_check.get("matched_deadline_page")
                deadline_locations = date_check.get("deadline_locations") or []
            date_status = date_check.get("status") if isinstance(date_check, dict) else None
            row_status = str(item.get("status") or ("found" if item.get("found") else "missing"))
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
                        "signature_status": signature_status,
                        "signature_parse_status": signature_parse_status,
                        "seal_status": (item.get("seal_check") or {}).get("status"),
                        "date_status": date_status,
                        "date_text": date_text,
                        "deadline_date": deadline_date,
                        "deadline_text": deadline_text,
                        "deadline_page": deadline_page,
                        "deadline_locations": deadline_locations,
                        "signature_evidence": signature_evidence,
                        "seal_texts": seal_evidence,
                        "seal_evidence": seal_evidence,
                    },
                    status=row_status,
                    page_refs=self._coerce_page_refs(item.get("pages"), item),
                    evidence={"requirements": item.get("requirements") or {}},
                )
            )

        return rows

    # 抽数表工具
    def _itemized_decimal(self, value: Any) -> Decimal | None:
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError):
                return None
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return Decimal(match.group(0))
        except (InvalidOperation, ValueError):
            return None

    def _format_itemized_decimal(self, value: Any) -> str | None:
        amount = self._itemized_decimal(value)
        if amount is None:
            return None
        return format(amount.quantize(Decimal("0.01")), "f")

    def _build_itemized_amount_value(self, item: dict[str, Any]) -> dict[str, Any]:
        quantity = self._itemized_decimal(item.get("quantity"))
        unit_price = self._itemized_decimal(item.get("unit_price"))
        ocr_total = (
            self._itemized_decimal(item.get("declared_line_total"))
            or self._itemized_decimal(item.get("line_total"))
            or self._itemized_decimal(item.get("amount"))
        )
        calculated_total = (
            quantity * unit_price
            if quantity is not None and unit_price is not None
            else (
                self._itemized_decimal(item.get("expected_total"))
                or self._itemized_decimal(item.get("amount"))
            )
        )
        difference = (
            calculated_total - ocr_total
            if calculated_total is not None and ocr_total is not None
            else self._itemized_decimal(item.get("difference"))
        )
        if calculated_total is None and ocr_total is None:
            row_status = "missing"
        elif quantity is None or unit_price is None:
            row_status = "unclear" if item.get("relation_type") not in {"amount_only_row"} else "found"
        elif ocr_total is None:
            row_status = "missing"
        else:
            row_status = "pass" if abs(difference or Decimal("0")) <= Decimal("0.01") else "fail"

        display_total = calculated_total if calculated_total is not None else ocr_total
        return {
            "serial": item.get("serial"),
            "item_label": item.get("label") or item.get("serial"),
            "quantity": self._format_itemized_decimal(quantity),
            "unit_price": self._format_itemized_decimal(unit_price),
            "ocr_total": self._format_itemized_decimal(ocr_total),
            "declared_line_total": self._format_itemized_decimal(ocr_total),
            "calculated_total": self._format_itemized_decimal(calculated_total),
            "expected_total": self._format_itemized_decimal(calculated_total),
            "difference": self._format_itemized_decimal(difference),
            "row_status": row_status,
            "relation_type": item.get("relation_type"),
            "amount": self._format_itemized_decimal(display_total),
            "amount_yuan": self._format_itemized_decimal(display_total),
            "raw_amount": item.get("source"),
            "source": item.get("source"),
            "section_anchor": item.get("section_anchor"),
        }

    def _attachment_signature_evidence_texts(self, attachment_result: dict[str, Any]) -> list[str]:
        """提取真实 OCR 签字文本，不混入通过状态或占位符。"""
        if not isinstance(attachment_result, dict):
            return []
        signature_check = attachment_result.get("signature_check") or {}
        if not isinstance(signature_check, dict):
            return []

        texts: list[str] = []
        seen: set[str] = set()
        ocr_evidence_modes = {
            "",
            "text_inline",
            "ocr_signature_section",
            "ocr_signature_region",
            "ocr_signature_location",
            "ocr_signature_location_fallback",
            "nearby_text_mark",
            "personal_seal_as_alternative",
        }
        non_content_values = {
            "",
            "已签字",
            "已盖章",
            "已签章",
            "通过",
            "pass",
            "found",
            "ok",
            "true",
        }

        def compact(text: Any) -> str:
            return re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", str(text or ""))

        non_content_keys = {compact(value).lower() for value in non_content_values}

        def content_value(raw: Any) -> str:
            value = str(raw or "").strip()
            if not value or compact(value).lower() in non_content_keys:
                return ""
            return value

        def candidates(item: dict[str, Any], mode: str) -> list[Any]:
            if mode == "personal_seal_as_alternative":
                return [item.get("value"), item.get("seal_text")]
            if mode == "nearby_text_mark":
                return [item.get("value"), item.get("evidence_text")]
            return [item.get("signature_text"), item.get("value")]

        for item in signature_check.get("filled_values") or []:
            if not isinstance(item, dict):
                continue
            mode = str(item.get("mode") or "").strip()
            if mode not in ocr_evidence_modes:
                continue
            evidence_text = ""
            for candidate in candidates(item, mode):
                evidence_text = content_value(candidate)
                if evidence_text:
                    break
            if not evidence_text:
                continue
            key = compact(evidence_text)
            if key not in seen:
                seen.add(key)
                texts.append(evidence_text)
        return texts

    def _compact_seal_evidence_text(self, text: Any) -> str:
        return re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", str(text or ""))

    def _looks_like_company_seal_evidence_text(self, text: Any) -> bool:
        compact = self._compact_seal_evidence_text(text)
        return bool(re.search(r"(有限责任公司|股份有限公司|集团有限公司|有限公司|公司|专用章)$", compact) or "公司" in compact)

    def _looks_like_person_seal_evidence_text(self, text: Any) -> bool:
        compact = self._compact_seal_evidence_text(text)
        return bool(re.fullmatch(r"[\u4e00-\u9fa5]{2,4}|[A-Za-z]{2,20}", compact))

    def _official_seal_evidence_texts(self, values: Any) -> list[str]:
        texts = list(dict.fromkeys(str(value or "").strip() for value in (values or []) if str(value or "").strip()))
        company_texts = [text for text in texts if self._looks_like_company_seal_evidence_text(text)]
        if company_texts:
            return company_texts
        return [text for text in texts if not self._looks_like_person_seal_evidence_text(text)]

    def _attachment_seal_evidence_texts(self, attachment_result: dict[str, Any]) -> list[str]:
        """提取该附件中真实识别到的盖章文本。"""
        if not isinstance(attachment_result, dict):
            return []
        seal_check = attachment_result.get("seal_check") or {}
        if not isinstance(seal_check, dict):
            return []

        texts: list[str] = []
        seen: set[str] = set()

        def add(text: Any, page: Any = None) -> None:
            value = str(text or "").strip()
            if not value:
                return
            key = self._compact_seal_evidence_text(value)
            if key in seen:
                return
            seen.add(key)
            label = f"{value}（P{page}）" if page else value
            texts.append(label)

        for seal_text in self._official_seal_evidence_texts(seal_check.get("seal_texts") or []):
            add(seal_text)
        best_match = seal_check.get("best_match") or {}
        if isinstance(best_match, dict) and str(best_match.get("mode") or "").strip() != "textual_seal_line":
            for seal_text in self._official_seal_evidence_texts([best_match.get("seal_text")]):
                add(seal_text, best_match.get("page"))
        return texts[:10]

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
        locations: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """构建一条标准化的抽数行。"""
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
        if locations:
            row["locations"] = locations
        return row

    def _coerce_page_refs(self, *values: Any) -> list[int]:
        """从任意结构的值中提取页码列表。"""
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
        """从条款名称中提取序号部分。"""
        text = str(item_name or "").strip()
        match = re.match(r"^\s*([^.]+?)\s*\.", text)
        return match.group(1).strip() if match else text

    def _count_extraction_rows(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        """统计每个 check_code 下的抽数行数量。"""
        counts: dict[str, int] = {}
        for row in rows:
            check_code = str(row.get("check_code") or "").strip()
            if not check_code:
                continue
            counts[check_code] = counts.get(check_code, 0) + 1
        return counts

    def _build_extraction_catalog(
        self,
        *,
        tender_rows: list[dict[str, Any]],
        bid_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """构建抽数目录，汇总招标方与投标方各有哪些字段组。"""
        catalog: dict[str, dict[str, set[str]]] = {}
        tender_groups: set[str] = set()
        bid_groups: set[str] = set()

        for row in tender_rows:
            check_code = str(row.get("check_code") or "")
            field_group = str(row.get("field_group") or "")
            if not check_code or not field_group:
                continue
            entry = catalog.setdefault(check_code, {"tender_field_groups": set(), "bid_field_groups": set()})
            entry["tender_field_groups"].add(field_group)
            tender_groups.add(field_group)

        for row in bid_rows:
            check_code = str(row.get("check_code") or "")
            field_group = str(row.get("field_group") or "")
            if not check_code or not field_group:
                continue
            entry = catalog.setdefault(check_code, {"tender_field_groups": set(), "bid_field_groups": set()})
            entry["bid_field_groups"].add(field_group)
            bid_groups.add(field_group)

        ordered_checks = {
            check_code: {
                "tender_field_groups": sorted(entry["tender_field_groups"]),
                "bid_field_groups": sorted(entry["bid_field_groups"]),
            }
            for check_code, entry in sorted(catalog.items(), key=lambda item: self._check_display_index(item[0]))
        }
        return {
            "tender_field_groups": sorted(tender_groups),
            "bid_field_groups": sorted(bid_groups),
            "checks": ordered_checks,
        }
