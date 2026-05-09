# unified/extraction_tables.py
"""
统一商务标审查 - 抽数表构建 Mixin

生成审查过程中的结构化抽取数据，包括招标方抽取行、投标方抽取行、
以及按检查项分组的目录概要。
"""

from __future__ import annotations

import json
import re
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

        # 一致性检查项
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
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="consistency_check",
                    field_group="template_segment",
                    field_name=str(segment.get("name") or "template_segment"),
                    value={"missing_anchors": list(segment.get("missing_anchors") or [])},
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
                    value={"missing_anchors": list(segment.get("missing_anchors") or [])},
                    status="skipped",
                    evidence={"skip_reason": segment.get("skip_reason")},
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
            rows.append(
                self._make_extraction_row(
                    row_index=len(rows) + 1,
                    document_side="bid",
                    document_role="business",
                    check_code="pricing_check",
                    field_group="pricing_subcheck",
                    field_name=subcheck_code,
                    value={"type": payload.get("type"), "summary": payload.get("summary")},
                    status=self._map_price_result(payload.get("result"), self._join_text(payload.get("summary"))),
                )
            )

        # 分项报价
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
                    value={"amount": total.get("amount"), "source": total.get("source")},
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

    # 抽数表工具
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