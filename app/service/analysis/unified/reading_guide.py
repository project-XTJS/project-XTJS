# unified/reading_guide.py
"""
统一商务标审查 - 阅读指南生成 Mixin

负责生成报告级和投标人级的阅读指南，包含检查项导航、关注点摘要、
源文档上下文等信息。
"""

from __future__ import annotations

import re
from typing import Any


class ReadingGuideMixin:
    """
    阅读指南生成相关的所有方法。

    依赖：
    - 实例属性：CHECK_DISPLAY_ORDER, ATTACHMENT_REF_RE, PAGE_REF_RE
    - 其他 Mixin：_review_status_sort_key, _check_display_index, _trim_text, _unique_texts,
                 _extract_attachment_refs, _simplify_integrity_item_title, _normalize_match_text
    """

    CHECK_DISPLAY_ORDER: tuple
    ATTACHMENT_REF_RE: Any
    PAGE_REF_RE: Any

    # 报告级阅读指南
    def _build_review_reading_guide(
        self,
        *,
        tender_meta: dict[str, Any],
        bidders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """构建报告级别的阅读指南，包含投标人概览与排序。"""
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

    # 投标人级阅读指南
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
        """构建单个投标人的阅读指南，包含各检查项的导航信息和重点关注。"""
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

    # 检查项源上下文
    def _build_check_source_context(
        self,
        *,
        check_code: str,
        tender_meta: dict[str, Any],
        business_meta: dict[str, Any],
        technical_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """为每个检查项生成“审查范围”和“所依赖的源文件”说明。"""
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
                "focus_scope": "对照招标文件响应文件格式中的模板正文固定内容，只检查正文是否被改动；标题、页眉页脚、落款和填写项不纳入一致性判断。",
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
        """生成文档的简要来源信息。"""
        return {
            "role": meta.get("role"),
            "file_name": meta.get("file_name"),
            "bidder_key": meta.get("bidder_key"),
            "page_count": meta.get("page_count"),
            "purpose": purpose,
        }

    # 检查项关注点提取
    def _collect_check_focus_sections(self, check: dict[str, Any], *, max_items: int = 6) -> list[str]:
        """收集检查项的关键关注点文本，供前端导航使用。"""
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
        """从证据字典/列表中提取关键文本 token。"""
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
        """从任意值中提取附件引用或页码引用等关注 token。"""
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
        """选取最重要的几个问题（优先失败 > 不明确 > 通过）作为亮点展示。"""
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
        """尝试将问题标题简化为更短的表示。"""
        text = self._simplify_integrity_item_title(str(title or ""))
        if text:
            return text
        raw_text = str(title or "").strip()
        raw_text = re.sub(r"^\s*(?:\d+|[A-Z])[.、\s]*", "", raw_text)
        raw_text = re.sub(r"[（(].*?[）)]", "", raw_text).strip()
        if 2 <= len(raw_text) <= 24:
            return raw_text
        return ""

    def _review_status_sort_key(self, status: Any) -> int:
        """用于将状态字符串映射为排序权重，fail 优先。"""
        text = str(status or "").strip().lower()
        order = {"fail": 0, "unclear": 1, "pass": 2}
        return order.get(text, 3)

    def _check_display_index(self, check_code: Any) -> int:
        """返回检查项在预定展示顺序中的索引，未知项排最后。"""
        text = str(check_code or "")
        if text in self.CHECK_DISPLAY_ORDER:
            return self.CHECK_DISPLAY_ORDER.index(text)
        return len(self.CHECK_DISPLAY_ORDER)
