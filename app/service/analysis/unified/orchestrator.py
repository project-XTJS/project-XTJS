# unified/orchestrator.py
"""
统一商务标审查 - 审查流程编排 Mixin

负责编排审查流程，包括上传 JSON 文件审查、数据库项目文档审查、
单个投标人审查（含技术标）和仅商务标审查。
"""

from __future__ import annotations

import time
from typing import Any, Callable

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID


class OrchestratorMixin:
    """审查流程编排 Mixin。"""

    # 声明实例属性类型提示（实际值由 __init__ 赋值）
    db_service: Any
    integrity_checker: Any
    consistency_checker: Any
    reasonableness_checker: Any
    itemized_checker: Any
    deviation_checker: Any
    verification_checker: Any
    RESULT_SCHEMA_VERSION: str
    BUSINESS_RESULT_KEY: str
    CHECK_DISPLAY_ORDER: tuple

    # 上传 JSON 文件审查编排
    def _review_uploaded_business_documents(
        self,
        *,
        tender_file_name: str,
        tender_payload: dict[str, Any],
        tender_raw_bytes: bytes,
        business_bid_documents: list[dict[str, Any]],
        project_identifier: str,
    ) -> dict[str, Any]:
        """编排上传 JSON 文件的审查流程，输出标准化结果。"""
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

    # 数据库项目文档审查编排
    def _review_project_business_documents(
        self,
        *,
        project_identifier: str,
        payload_data: dict[str, Any],
    ) -> dict[str, Any]:
        """通过数据库查询获得的文档记录执行审查流程。"""
        document_records = list(payload_data.get("documents") or [])
        if not document_records:
            return self._build_empty_project_business_review(
                project_identifier=project_identifier,
                reason=f"project has no bound documents: {project_identifier}",
            )

        tender_record = next(
            (
                record
                for record in document_records
                if isinstance(self._coerce_stored_payload(record.get("tender_content")), dict)
                and self._coerce_stored_payload(record.get("tender_content"))
            ),
            None,
        )
        if not tender_record:
            return self._build_empty_project_business_review(
                project_identifier=project_identifier,
                reason=f"project has no tender content: {project_identifier}",
            )

        tender_payload = self._coerce_stored_payload(tender_record.get("tender_content"))
        tender_meta = self._build_project_record_meta(
            record=tender_record,
            payload=tender_payload,
            role="tender",
            bidder_key=None,
            file_name_key="tender_file_name",
            file_url_key="tender_file_url",
            identifier_key="tender_identifier_id",
        )

        bidders: list[dict[str, Any]] = []
        bidder_entries: list[dict[str, Any]] = []
        bidder_sources: list[dict[str, Any]] = []
        used_bidder_keys: set[str] = set()
        seen_business_documents: set[str] = set()

        for record in document_records:
            if self._normalize_project_document_role(record.get("relation_role")) != DOCUMENT_TYPE_BUSINESS_BID:
                continue

            business_identifier = str(record.get("identifier_id") or "").strip()
            if not business_identifier or business_identifier in seen_business_documents:
                continue
            seen_business_documents.add(business_identifier)

            business_payload = self._coerce_stored_payload(record.get("content"))
            bidder_key = self._ensure_project_bidder_key(
                self._derive_project_bidder_key(record.get("file_name"), business_identifier),
                used_bidder_keys,
            )
            business_meta = self._build_project_record_meta(
                record=record,
                payload=business_payload,
                role="business",
                bidder_key=bidder_key,
                file_name_key="file_name",
                file_url_key="file_url",
                identifier_key="identifier_id",
            )

            bidder_entries.append(
                {
                    "bidder_key": bidder_key,
                    "business": business_meta,
                }
            )
            bidder_sources.append(
                {
                    "bidder_key": bidder_key,
                    "business": {
                        "content": business_payload,
                        "meta": business_meta,
                    },
                }
            )
            bidders.append(
                self._review_business_bidder(
                    tender_payload=tender_payload,
                    tender_meta=tender_meta,
                    bidder_key=bidder_key,
                    business_payload=business_payload,
                    business_meta=business_meta,
                )
            )

        if not bidders:
            return self._build_empty_project_business_review(
                project_identifier=project_identifier,
                reason=f"project has no business bid documents: {project_identifier}",
            )

        extraction_tables = self._build_review_extraction_tables(
            tender_payload=tender_payload,
            tender_meta=tender_meta,
            bidder_sources=bidder_sources,
            bidder_reviews=bidders,
        )
        reading_guide = self._build_review_reading_guide(
            tender_meta=tender_meta,
            bidders=bidders,
        )

        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "business_bid_format_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": project_identifier,
            "dataset": {
                "input_mode": "project_documents",
                "tender": tender_meta,
                "bidders": bidder_entries,
                "file_count": 1 + len(bidder_entries),
            },
            "reading_guide": reading_guide,
            "extraction_tables": extraction_tables,
            "function_validation": self._summarize_function_validation(bidders),
            "summary": self._summarize_review(bidders),
            "bidders": bidders,
        }

    # 空审查兜底
    def _build_empty_project_business_review(
        self,
        *,
        project_identifier: str,
        reason: str,
    ) -> dict[str, Any]:
        """生成一个空审查结果，用于没有文档或招标文件的情况。"""
        summary = self._summarize_review([])
        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "business_bid_format_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": project_identifier,
            "empty": True,
            "empty_reason": reason,
            "dataset": {
                "input_mode": "project_documents",
                "tender": None,
                "bidders": [],
                "file_count": 0,
            },
            "reading_guide": {
                "tender_file_name": None,
                "bidder_overview": [],
                "message": reason,
            },
            "extraction_tables": {
                "catalog": [],
                "tender_table": {},
                "bidder_tables": [],
            },
            "function_validation": self._summarize_function_validation([]),
            "summary": summary,
            "bidders": [],
        }

    # 单个投标人审查（含技术标）
    def _review_bidder(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder: dict[str, Any],
    ) -> dict[str, Any]:
        """对单个投标人执行完整的商务标+技术标审查。"""
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
        checks["verification_check"] = self._suppress_integrity_duplicates_in_verification(
            verification_check=checks["verification_check"],
            integrity_check=integrity_check,
        )

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

    # 单个投标人审查（仅商务标）
    def _review_business_bidder(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder_key: str,
        business_payload: dict[str, Any],
        business_meta: dict[str, Any],
    ) -> dict[str, Any]:
        """对提供商务标的投标人执行审查，偏离检查仅基于当前商务标内容。"""
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
            # 商务标形式审查只检查当前商务标文件中出现的商务/技术偏离表内容，不读取技术标文件。
            "deviation_check": self._execute_check(
                check_code="deviation_check",
                check_name="偏离条款审查",
                runner=lambda: self.deviation_checker.check_technical_deviation(tender_payload, business_payload),
                normalizer=self._normalize_deviation,
            ),
            "verification_check": self._execute_check(
                check_code="verification_check",
                check_name="签字盖章日期审查",
                runner=lambda: self.verification_checker.check_seal_and_date(tender_payload, business_payload),
                normalizer=self._normalize_verification,
            ),
        }
        checks["verification_check"] = self._suppress_integrity_duplicates_in_verification(
            verification_check=checks["verification_check"],
            integrity_check=integrity_check,
        )

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
