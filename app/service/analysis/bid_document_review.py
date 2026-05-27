# -*- coding: utf-8 -*-
"""
投标文件审查服务。

提供基于规则的错别字检测、人员信息提取及跨文档人员复用分析功能，
主要用于商务标/技术标的自动化审查。
"""

from __future__ import annotations

import html
import json
import logging
import re
from collections import defaultdict
from html.parser import HTMLParser
from threading import Lock
from typing import Any

from app.config.settings import settings
from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
from app.service.analysis.location_utils import (
    collect_locations,
    make_location,
    normalize_bbox as normalize_location_bbox,
)

try:
    import language_tool_python
except ImportError:  # pragma: no cover - optional dependency fallback
    language_tool_python = None


logger = logging.getLogger(__name__)
_LANGUAGE_TOOL_INSTANCE: Any | None = None
_LANGUAGE_TOOL_INIT_ATTEMPTED = False
_LANGUAGE_TOOL_INIT_ERROR: str | None = None
_LANGUAGE_TOOL_INIT_LOCK = Lock()
_LANGUAGE_TOOL_CHECK_LOCK = Lock()


class _TableHTMLParser(HTMLParser):
    """用于审查模块的轻量 HTML 表格解析器，提取单元格文本及 rowspan/colspan。"""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[dict[str, Any]]] = []
        self._current_row: list[dict[str, Any]] | None = None
        self._current_cell: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized == "tr":
            self._current_row = []
            return
        if normalized not in {"td", "th"} or self._current_row is None:
            return

        attr_map = {key: value for key, value in attrs}
        self._current_cell = {
            "text_parts": [],
            "rowspan": self._safe_span(attr_map.get("rowspan")),
            "colspan": self._safe_span(attr_map.get("colspan")),
        }

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"td", "th"} and self._current_row is not None and self._current_cell is not None:
            text = html.unescape("".join(self._current_cell["text_parts"]))
            text = re.sub(r"\s+", " ", text).strip()
            self._current_row.append(
                {
                    "text": text,
                    "rowspan": self._current_cell["rowspan"],
                    "colspan": self._current_cell["colspan"],
                }
            )
            self._current_cell = None
            return

        if normalized == "tr" and self._current_row is not None:
            self.rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell["text_parts"].append(data)

    @staticmethod
    def _safe_span(value: str | None) -> int:
        try:
            return max(1, int(str(value or "1")))
        except ValueError:
            return 1


class BidDocumentReviewService:
    """投标文件审查服务，提供错别字检查和人员复用分析。"""

    SUPPORTED_DOCUMENT_TYPES = (
        DOCUMENT_TYPE_BUSINESS_BID,
        DOCUMENT_TYPE_TECHNICAL_BID,
    )

    # 分句正则
    SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[。！？；;.!?])|[\r\n]+")
    # 人名标题匹配（如 "1. 张三"）
    NUMBERED_NAME_HEADING_PATTERN = re.compile(r"^\s*\d+\s*[.、]?\s*([\u4e00-\u9fa5A-Za-z]{2,20})\s*$")
    # 人员字段正则（如 "姓名：张三"）
    FIELD_NAME_PATTERN = re.compile(
        r"(?:姓名|缴费人名称|法定代表人|授权代表|授权委托人|委托代理人|被授权人)[^：:\n]{0,12}[：:]\s*([A-Za-z\u4e00-\u9fa5\[\]【】()（）]{2,30})"
    )
    PERSONNEL_ROLE_HINTS = (
        "法定代表人",
        "授权代表",
        "授权委托人",
        "委托代理人",
        "被授权人",
    )
    PERSONNEL_CONTEXT_HINTS = PERSONNEL_ROLE_HINTS + (
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人授权委托书",
        "合法代理人",
        "执行事务合伙人",
    )
    PERSONNEL_SOURCE_PRIORITY = {
        "personnel_certificate": 100,
        "personnel_authorizer": 95,
        "personnel_authorized_agent": 95,
        "personnel_key_value_table": 90,
        "personnel_table": 80,
        "personnel_reverse_role": 70,
        "personnel_line": 65,
        "personnel_inline_role": 60,
    }

    # 人员相关章节标题关键词
    PERSONNEL_SECTION_HINTS = (
        "项目人员情况",
        "人员组成名单",
        "主要人员简历",
        "核心人员简历",
        "核心人员资质",
        "核心人员简介",
        "项目团队核心成员简介",
        "项目团队",
        "团队配置",
        "团队成员",
        "人员配置",
    )
    # 表头中代表岗位的列名
    ROLE_HEADER_HINTS = ("岗位", "职位", "职务", "职责")
    # 常见岗位名称
    ROLE_TEXT_HINTS = (
        "项目经理",
        "项目负责人",
        "总负责人",
        "技术负责人",
        "驻场工程师",
        "驻场服务主管",
        "运维工程师",
        "运维主管",
        "技术支持",
        "全栈工程师",
        "全能运维工程师",
        "服务主管",
        "授权代表",
        "授权委托人",
        "委托代理人",
        "被授权人",
        "法定代表人",
    )

    # 错别字检查停用词（含这些词的句子才检查，避免大量无关内容）
    TYPO_STOPWORDS = {
        "项目",
        "服务",
        "系统",
        "平台",
        "技术",
        "管理",
        "方案",
        "支持",
        "实施",
        "运维",
        "团队",
        "人员",
        "有限公司",
        "公司",
        "电话",
        "邮箱",
        "身份证",
        "社会保险",
        "缴费人名称",
        "工作内容",
        "项目经历",
        "学历",
        "年龄",
        "岗位",
        "职位",
        "姓名",
    }
    # 人名占位符（非真实姓名）
    PERSON_NAME_PLACEHOLDERS = {
        "已签字",
        "已盖章",
        "已签章",
        "已签名",
        "签字",
        "盖章",
        "签章",
        "签名",
        "已签",
    }
    # 已知常见错别字映射
    KNOWN_TYPO_MAP = {
        "釆用": "采用",
        "釆购": "采购",
        "按装": "安装",
        "部暑": "部署",
        "侯选": "候选",
        "现厂": "现场",
        "録入": "录入",
        "录相": "录像",
        "迳行": "径行",
        "拚装": "拼装",
        "优於": "优于",
        "缐上": "线上",
        "萤幕": "屏幕",
    }
    SKIP_REASON_MISSING_CONTENT = "missing_or_unusable_ocr_content"

    @classmethod
    def _get_language_tool_instance(cls) -> tuple[Any | None, str | None]:
        """延迟初始化 LanguageTool，失败时回退到词典模式。"""
        global _LANGUAGE_TOOL_INSTANCE, _LANGUAGE_TOOL_INIT_ATTEMPTED, _LANGUAGE_TOOL_INIT_ERROR

        if not settings.TYPO_LANGUAGETOOL_ENABLED:
            return None, "disabled_by_setting"
        if language_tool_python is None:
            return None, "package_not_installed"
        if _LANGUAGE_TOOL_INSTANCE is not None:
            return _LANGUAGE_TOOL_INSTANCE, None
        if _LANGUAGE_TOOL_INIT_ATTEMPTED:
            return None, _LANGUAGE_TOOL_INIT_ERROR or "initialization_failed"

        with _LANGUAGE_TOOL_INIT_LOCK:
            if _LANGUAGE_TOOL_INSTANCE is not None:
                return _LANGUAGE_TOOL_INSTANCE, None
            if _LANGUAGE_TOOL_INIT_ATTEMPTED:
                return None, _LANGUAGE_TOOL_INIT_ERROR or "initialization_failed"
            try:
                _LANGUAGE_TOOL_INSTANCE = language_tool_python.LanguageTool(
                    settings.TYPO_LANGUAGETOOL_LANGUAGE
                )
                _LANGUAGE_TOOL_INIT_ERROR = None
            except Exception as exc:  # pragma: no cover - environment dependent
                _LANGUAGE_TOOL_INIT_ERROR = f"{type(exc).__name__}: {exc}"
                logger.warning("LanguageTool init failed, fallback to dictionary mode: %s", exc)
            finally:
                _LANGUAGE_TOOL_INIT_ATTEMPTED = True

        return _LANGUAGE_TOOL_INSTANCE, _LANGUAGE_TOOL_INIT_ERROR

    @staticmethod
    def _typo_engine_name(language_tool_enabled: bool) -> str:
        return "languagetool_hybrid" if language_tool_enabled else "rule_based"

    def _build_typo_check_notes(
        self,
        *,
        language_tool_enabled: bool,
        language_tool_error: str | None,
    ) -> list[str]:
        notes: list[str] = []
        if language_tool_enabled:
            notes.append(
                f"已默认启用 LanguageTool（{settings.TYPO_LANGUAGETOOL_LANGUAGE}），并结合内置错别字词典补充识别。"
            )
        else:
            reason = language_tool_error or "unknown_reason"
            notes.append(
                f"LanguageTool 当前不可用（{reason}），本次回退为内置错别字词典识别。"
            )
        return notes

    def _prepare_document_groups(
        self,
        *,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
    ) -> tuple[list[str], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
        """按文档类型准备可分析文档，供错别字和人员复用独立复用。"""
        requested_types = self._normalize_requested_types(document_types)
        prepared_groups: dict[str, list[dict[str, Any]]] = {
            item: [] for item in requested_types
        }
        skipped_groups: dict[str, list[dict[str, Any]]] = {
            item: [] for item in requested_types
        }
        dedupe_keys: set[str] = set()

        for record in document_records:
            role = self._normalize_document_role(
                record.get("relation_role") or record.get("document_type")
            )
            if role not in requested_types:
                continue

            identifier_id = str(record.get("identifier_id") or "").strip()
            dedupe_key = f"{role}:{identifier_id}"
            if not identifier_id or dedupe_key in dedupe_keys:
                continue
            dedupe_keys.add(dedupe_key)

            prepared, skip_reason = self._prepare_document(record)
            if prepared is None:
                skipped_groups[role].append(
                    {
                        "identifier_id": identifier_id,
                        "relation_id": record.get("relation_id"),
                        "file_name": record.get("file_name"),
                        "reason": skip_reason or self.SKIP_REASON_MISSING_CONTENT,
                    }
                )
                continue
            prepared["document_type"] = role
            prepared_groups[role].append(prepared)

        return list(requested_types), prepared_groups, skipped_groups

    @staticmethod
    def _document_summaries(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "identifier_id": item["identifier_id"],
                "relation_id": item.get("relation_id"),
                "file_name": item.get("file_name"),
                "page_count": item.get("page_count", 0),
                "layout_section_count": item.get("layout_section_count", 0),
                "table_count": item.get("table_count", 0),
                "personnel_entry_count": len(item.get("personnel_entries") or []),
            }
            for item in documents
        ]

    # 错别字服务入口：只执行错别字检查
    def check_project_typos(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
    ) -> dict[str, Any]:
        requested_types, prepared_groups, skipped_groups = self._prepare_document_groups(
            document_records=document_records,
            document_types=document_types,
        )
        groups: dict[str, Any] = {}
        total_document_count = 0
        total_skipped_document_count = 0
        total_typo_issue_count = 0
        total_shared_typo_issue_count = 0
        total_suspicious_typo_document_count = 0
        language_tool, language_tool_error = self._get_language_tool_instance()
        language_tool_enabled = language_tool is not None

        for role in requested_types:
            prepared_documents = prepared_groups[role]
            skipped_documents = skipped_groups[role]
            typo_check = self._run_typo_check(
                prepared_documents,
                language_tool=language_tool,
                language_tool_error=language_tool_error,
            )
            group_document_count = len(prepared_documents)
            group_skipped_count = len(skipped_documents)
            group_typo_issue_count = int(typo_check.get("issue_count") or 0)
            group_shared_typo_issue_count = int(typo_check.get("shared_issue_count") or 0)
            group_suspicious_document_count = int(typo_check.get("suspicious_document_count") or 0)

            groups[role] = {
                "documents": self._document_summaries(prepared_documents),
                "skipped_documents": skipped_documents,
                "typo_check": typo_check,
                "summary": {
                    "document_count": group_document_count,
                    "skipped_document_count": group_skipped_count,
                    "typo_issue_count": group_typo_issue_count,
                    "shared_typo_issue_count": group_shared_typo_issue_count,
                    "suspicious_typo_document_count": group_suspicious_document_count,
                    "suspicious": bool(group_typo_issue_count),
                },
            }

            total_document_count += group_document_count
            total_skipped_document_count += group_skipped_count
            total_typo_issue_count += group_typo_issue_count
            total_shared_typo_issue_count += group_shared_typo_issue_count
            total_suspicious_typo_document_count += group_suspicious_document_count

        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "document_types": list(requested_types),
                "typo_detection_engine": self._typo_engine_name(language_tool_enabled),
                "typo_languagetool_enabled": language_tool_enabled,
                "typo_languagetool_language": settings.TYPO_LANGUAGETOOL_LANGUAGE,
                "typo_stopword_dictionary_enabled": True,
                "typo_known_typo_dictionary_enabled": settings.TYPO_KNOWN_DICTIONARY_ENABLED,
            },
            "groups": groups,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": total_document_count,
                "skipped_document_count": total_skipped_document_count,
                "typo_issue_count": total_typo_issue_count,
                "shared_typo_issue_count": total_shared_typo_issue_count,
                "suspicious_typo_document_count": total_suspicious_typo_document_count,
                "suspicious": bool(total_typo_issue_count),
            },
        }

    # 人员复用服务入口：只执行人员复用检查
    def check_project_personnel_reuse(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
    ) -> dict[str, Any]:
        requested_types, prepared_groups, skipped_groups = self._prepare_document_groups(
            document_records=document_records,
            document_types=document_types,
        )
        groups: dict[str, Any] = {}
        total_document_count = 0
        total_skipped_document_count = 0
        total_personnel_count = 0
        total_reused_name_count = 0

        for role in requested_types:
            prepared_documents = prepared_groups[role]
            skipped_documents = skipped_groups[role]
            personnel_reuse_check = self._run_personnel_reuse_check(prepared_documents)
            group_document_count = len(prepared_documents)
            group_skipped_count = len(skipped_documents)
            group_personnel_count = int(personnel_reuse_check.get("personnel_count") or 0)
            group_reused_name_count = int(personnel_reuse_check.get("reused_name_count") or 0)

            groups[role] = {
                "documents": self._document_summaries(prepared_documents),
                "skipped_documents": skipped_documents,
                "personnel_reuse_check": personnel_reuse_check,
                "summary": {
                    "document_count": group_document_count,
                    "skipped_document_count": group_skipped_count,
                    "personnel_count": group_personnel_count,
                    "reused_name_count": group_reused_name_count,
                    "suspicious": bool(group_reused_name_count),
                },
            }

            total_document_count += group_document_count
            total_skipped_document_count += group_skipped_count
            total_personnel_count += group_personnel_count
            total_reused_name_count += group_reused_name_count

        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "document_types": list(requested_types),
                "personnel_reuse_scope": "per_document_type",
            },
            "groups": groups,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": total_document_count,
                "skipped_document_count": total_skipped_document_count,
                "personnel_count": total_personnel_count,
                "reused_name_count": total_reused_name_count,
                "suspicious": bool(total_reused_name_count),
            },
        }

    # 文档预处理：提取人员信息、统计基础信息
    def _prepare_document(
        self,
        record: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        payload = self._coerce_payload(record.get("content"))
        sections = self._sections(payload)
        tables = self._native_tables(payload)
        if not sections and not tables:
            return None, self.SKIP_REASON_MISSING_CONTENT

        personnel_section_pages = self._detect_personnel_section_pages(sections)
        personnel_entries = self._extract_personnel_entries(
            record=record,
            sections=sections,
            tables=tables,
        )
        personnel_pages = set(personnel_section_pages)
        personnel_pages.update(
            int(item["page"])
            for item in personnel_entries
            if isinstance(item.get("page"), int)
        )

        return {
            "identifier_id": str(record.get("identifier_id") or "").strip(),
            "relation_id": record.get("relation_id"),
            "file_name": record.get("file_name"),
            "page_count": self._page_count(sections, tables),
            "layout_section_count": len(sections),
            "table_count": len(tables),
            "sections": sections,
            "tables": tables,
            "personnel_entries": personnel_entries,
            "personnel_pages": personnel_pages,
        }, None

    # 错别字检查：基于内置词典扫描文档内容
    def _run_typo_check(
        self,
        documents: list[dict[str, Any]],
        *,
        language_tool: Any | None = None,
        language_tool_error: str | None = None,
    ) -> dict[str, Any]:
        issue_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        document_issue_items: list[dict[str, Any]] = []
        total_issue_count = 0
        language_tool_enabled = language_tool is not None

        for document in documents:
            document_issues = self._extract_document_typo_issues(
                document,
                language_tool=language_tool,
            )
            total_issue_count += len(document_issues)
            if document_issues:
                document_issue_items.append(
                    {
                        "identifier_id": document["identifier_id"],
                        "relation_id": document.get("relation_id"),
                        "file_name": document.get("file_name"),
                        "issue_count": len(document_issues),
                        "issues": document_issues,
                    }
                )
            for issue in document_issues:
                issue_groups[str(issue.get("issue_key") or "")].append(issue)

        # 统计跨文档的共同错别字
        shared_issues: list[dict[str, Any]] = []
        for issue_key, items in issue_groups.items():
            document_ids = {
                str(item.get("document_identifier_id") or "").strip()
                for item in items
                if str(item.get("document_identifier_id") or "").strip()
            }
            if len(document_ids) < 2:
                continue

            first = items[0]
            shared_issues.append(
                {
                    "issue_key": issue_key,
                    "issue_type": first.get("issue_type"),
                    "matched_text": first.get("matched_text"),
                    "suggestion": first.get("suggestion"),
                    "document_count": len(document_ids),
                    "occurrence_count": len(items),
                    "locations": collect_locations(items),
                    "occurrences": items,
                }
            )

        shared_issues.sort(
            key=lambda item: (
                int(item.get("document_count") or 0),
                int(item.get("occurrence_count") or 0),
                str(item.get("issue_key") or ""),
            ),
            reverse=True,
        )
        document_issue_items.sort(
            key=lambda item: (
                int(item.get("issue_count") or 0),
                str(item.get("identifier_id") or ""),
            ),
            reverse=True,
        )

        return {
            "document_count": len(documents),
            "issue_count": total_issue_count,
            "shared_issue_count": len(shared_issues),
            "suspicious_document_count": len(document_issue_items),
            "engine": self._typo_engine_name(language_tool_enabled),
            "documents": document_issue_items,
            "shared_issues": shared_issues,
            "notes": self._build_typo_check_notes(
                language_tool_enabled=language_tool_enabled,
                language_tool_error=language_tool_error,
            ),
        }

    # 人员复用分析：聚合同名人员信息
    def _run_personnel_reuse_check(self, documents: list[dict[str, Any]]) -> dict[str, Any]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        total_personnel_count = 0
        document_summaries: list[dict[str, Any]] = []

        for document in documents:
            entries = list(document.get("personnel_entries") or [])
            total_personnel_count += len(entries)
            document_summaries.append(
                {
                    "identifier_id": document["identifier_id"],
                    "relation_id": document.get("relation_id"),
                    "file_name": document.get("file_name"),
                    "personnel_count": len(entries),
                }
            )
            for entry in entries:
                name = str(entry.get("name") or "").strip()
                if not name:
                    continue
                grouped[name].append(entry)

        reused_names: list[dict[str, Any]] = []
        for name, items in grouped.items():
            document_ids = {
                str(item.get("document_identifier_id") or "").strip()
                for item in items
                if str(item.get("document_identifier_id") or "").strip()
            }
            if len(document_ids) < 2:
                continue

            roles = sorted(
                {
                    str(item.get("role") or "").strip()
                    for item in items
                    if str(item.get("role") or "").strip()
                }
            )
            reused_names.append(
                {
                    "name": name,
                    "document_count": len(document_ids),
                    "occurrence_count": len(items),
                    "roles": roles,
                    "risk_level": self._personnel_reuse_risk_level(roles, len(document_ids)),
                    "locations": collect_locations(items),
                    "occurrences": items,
                }
            )

        reused_names.sort(
            key=lambda item: (
                int(item.get("document_count") or 0),
                int(item.get("occurrence_count") or 0),
                str(item.get("name") or ""),
            ),
            reverse=True,
        )
        document_summaries.sort(
            key=lambda item: (
                int(item.get("personnel_count") or 0),
                str(item.get("identifier_id") or ""),
            ),
            reverse=True,
        )

        return {
            "document_count": len(documents),
            "personnel_count": total_personnel_count,
            "reused_name_count": len(reused_names),
            "documents": document_summaries,
            "issues": reused_names,
            "notes": [
                "同名人员跨不同技术标重复出现时标记为疑似一人多用，建议结合原文页码与框选位置人工复核。",
            ],
        }

    def _check_language_tool_sentence(self, language_tool: Any, text: str) -> list[Any]:
        """串行调用共享的 LanguageTool 实例，避免并发冲突。"""
        try:
            with _LANGUAGE_TOOL_CHECK_LOCK:
                return list(language_tool.check(text))
        except Exception as exc:  # pragma: no cover - runtime/environment dependent
            logger.warning("LanguageTool sentence check failed: %s", exc)
            return []

    def _build_language_tool_issue(
        self,
        *,
        match: Any,
        sentence: dict[str, Any],
        document: dict[str, Any],
    ) -> dict[str, Any] | None:
        """将 LanguageTool 的 Match 转成项目内统一的错别字条目。"""
        issue_type = str(getattr(match, "rule_issue_type", "") or "").strip().lower()
        if issue_type not in {"misspelling", "typographical"}:
            return None

        text = str(sentence.get("text") or "")
        matched_text = str(getattr(match, "matched_text", "") or "").strip()
        if not matched_text:
            offset = int(getattr(match, "offset", 0) or 0)
            error_length = int(getattr(match, "error_length", 0) or 0)
            matched_text = text[offset: offset + error_length].strip()
        if not matched_text:
            return None

        replacements = list(getattr(match, "replacements", []) or [])
        suggestion = str(replacements[0]).strip() if replacements else ""
        if not suggestion or self._compact(suggestion) == self._compact(matched_text):
            return None

        rule_id = str(getattr(match, "rule_id", "") or "").strip() or "languagetool"
        issue = {
            "issue_type": "languagetool",
            "issue_key": rule_id,
            "matched_text": matched_text,
            "suggestion": suggestion,
            "page": sentence.get("page"),
            "bbox": sentence.get("bbox"),
            "text": text,
            "message": str(getattr(match, "message", "") or "").strip(),
            "document_identifier_id": document["identifier_id"],
            "relation_id": document.get("relation_id"),
            "file_name": document.get("file_name"),
        }
        issue["locations"] = [
            location for location in [
                make_location(
                    document_identifier_id=document["identifier_id"],
                    file_name=document.get("file_name"),
                    page=sentence.get("page"),
                    bbox=sentence.get("bbox"),
                    text=matched_text or text,
                )
            ] if location
        ]
        return issue

    def _find_language_tool_matches(
        self,
        sentence: dict[str, Any],
        document: dict[str, Any],
        language_tool: Any,
    ) -> list[dict[str, Any]]:
        """在句子中提取 LanguageTool 返回的拼写/笔误问题。"""
        text = str(sentence.get("text") or "").strip()
        if not text:
            return []

        issues: list[dict[str, Any]] = []
        for match in self._check_language_tool_sentence(language_tool, text):
            issue = self._build_language_tool_issue(
                match=match,
                sentence=sentence,
                document=document,
            )
            if issue:
                issues.append(issue)
        return issues

    # 错别字问题提取：对单个文档逐句扫描已知错误词
    def _extract_document_typo_issues(
        self,
        document: dict[str, Any],
        *,
        language_tool: Any | None = None,
    ) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()

        for sentence in self._iter_typo_sentences(document):
            sentence_issues: list[dict[str, Any]] = []
            if language_tool is not None:
                sentence_issues.extend(
                    self._find_language_tool_matches(sentence, document, language_tool)
                )
            if settings.TYPO_KNOWN_DICTIONARY_ENABLED:
                sentence_issues.extend(self._find_known_typo_matches(sentence, document))

            for issue in sentence_issues:
                if self._is_ignored_typo_issue(issue):
                    continue
                key = (
                    issue.get("page"),
                    issue.get("matched_text"),
                    issue.get("suggestion"),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                issues.append(issue)

        issues.sort(
            key=lambda item: (
                int(item.get("page") or 0),
                str(item.get("issue_type") or ""),
                str(item.get("matched_text") or ""),
            )
        )
        return issues

    def _is_ignored_typo_issue(self, issue: dict[str, Any]) -> bool:
        """过滤业务上接受的用字差异，避免把“帐/账”当错别字。"""
        matched_text = self._compact(issue.get("matched_text"))
        suggestion = self._compact(issue.get("suggestion"))
        if not matched_text or not suggestion or matched_text == suggestion:
            return False
        normalized_matched = matched_text.replace("帐", "账")
        normalized_suggestion = suggestion.replace("帐", "账")
        return normalized_matched == normalized_suggestion

    # 遍历文档中可用于错别字检查的句子
    def _iter_typo_sentences(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        personnel_pages = set(document.get("personnel_pages") or set())

        for section in document.get("sections") or []:
            section_type = str(section.get("type") or "").strip().lower()
            if section_type in {"seal", "signature"}:
                continue
            page = section.get("page")
            if isinstance(page, int) and page in personnel_pages:
                continue

            raw_text = str(section.get("text") or "").strip()
            if not raw_text:
                continue

            for sentence in self.SENTENCE_SPLIT_PATTERN.split(raw_text):
                normalized = self._normalize_text(sentence)
                compact = self._compact(normalized)
                if len(compact) < 10 or self._should_skip_typo_sentence(normalized):
                    continue
                items.append(
                    {
                        "page": page,
                        "bbox": section.get("bbox"),
                        "text": normalized,
                    }
                )
        return items

    def _should_skip_typo_sentence(self, text: str) -> bool:
        """判断句子是否应跳过错别字检查（过短、无中文、主要是数字/URL等）。"""
        compact = self._compact(text)
        if not compact:
            return True
        if len(re.findall(r"[\u4e00-\u9fa5]", compact)) < 4:
            return True
        if any(token in compact for token in self.TYPO_STOPWORDS):
            return False
        digit_count = sum(ch.isdigit() for ch in compact)
        if digit_count >= max(6, len(compact) // 2):
            return True
        if re.search(r"(https?://|www\.|@[A-Za-z0-9_]+)", compact):
            return True
        if re.fullmatch(r"[A-Za-z0-9\-_/.:]+", compact):
            return True
        return False

    def _find_known_typo_matches(
        self,
        sentence: dict[str, Any],
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """在句子中查找已知错别字并生成问题条目。"""
        issues: list[dict[str, Any]] = []
        text = str(sentence.get("text") or "")
        compact = self._compact(text)
        for typo_form, suggestion in self.KNOWN_TYPO_MAP.items():
            if typo_form not in compact:
                continue
            issue = {
                "issue_type": "known_typo",
                "issue_key": typo_form,
                "matched_text": typo_form,
                "suggestion": suggestion,
                "page": sentence.get("page"),
                "bbox": sentence.get("bbox"),
                "text": text,
                "document_identifier_id": document["identifier_id"],
                "relation_id": document.get("relation_id"),
                "file_name": document.get("file_name"),
            }
            issue["locations"] = [
                location for location in [
                    make_location(
                        document_identifier_id=document["identifier_id"],
                        file_name=document.get("file_name"),
                        page=sentence.get("page"),
                        bbox=sentence.get("bbox"),
                        text=typo_form,
                    )
                ] if location
            ]
            issues.append(issue)
        return issues

    # 人员信息提取：从表格和段落中找出人名及岗位
    def _extract_personnel_entries(
        self,
        *,
        record: dict[str, Any],
        sections: list[dict[str, Any]],
        tables: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []

        for table in tables:
            entries.extend(self._extract_personnel_entries_from_table(record, table))

        for section in sections:
            section_entries = self._extract_personnel_entries_from_section(record, section)
            if section_entries:
                entries.extend(section_entries)

        # 同一文档中同一姓名+角色仅保留质量最高的一条证据，避免多规则重复命中。
        selected: dict[tuple[Any, ...], dict[str, Any]] = {}
        for entry in entries:
            key = (
                entry.get("document_identifier_id"),
                entry.get("name"),
                entry.get("role"),
            )
            existing = selected.get(key)
            if existing is None or self._personnel_entry_priority(entry) > self._personnel_entry_priority(existing):
                selected[key] = entry
            elif existing is not None and self._personnel_entry_priority(entry) == self._personnel_entry_priority(existing):
                existing_page = int(existing.get("page") or 10**9)
                entry_page = int(entry.get("page") or 10**9)
                if entry_page < existing_page:
                    selected[key] = entry

        deduped = list(selected.values())

        deduped.sort(
            key=lambda item: (
                str(item.get("name") or ""),
                int(item.get("page") or 0),
                str(item.get("role") or ""),
            )
        )
        return deduped

    def _extract_personnel_entries_from_table(
        self,
        record: dict[str, Any],
        table: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从表格中按姓名列提取人员。"""
        rows = self._parse_html_table_rows(table)
        if not rows:
            return []

        entries: list[dict[str, Any]] = []
        header = [self._compact(cell) for cell in rows[0]]
        name_index = self._find_header_index(header, ("姓名",))
        role_index = self._find_header_index(header, self.ROLE_HEADER_HINTS)

        if name_index is not None and (role_index is not None or self._looks_like_personnel_header(header)):
            for row in rows[1:]:
                name = self._clean_person_name(self._safe_list_get(row, name_index))
                if not name:
                    continue
                role = self._normalize_role(
                    self._safe_list_get(row, role_index) if role_index is not None else ""
                )
                evidence_text = " | ".join(cell for cell in row if cell)
                entries.append(
                    self._build_personnel_entry(
                        record=record,
                        name=name,
                        role=role,
                        page=table.get("page"),
                        bbox=table.get("bbox"),
                        evidence_text=evidence_text,
                        source_type="personnel_table",
                    )
                )
        entries.extend(self._extract_personnel_entries_from_key_value_table(record, table, rows))
        return entries

    def _extract_personnel_entries_from_section(
        self,
        record: dict[str, Any],
        section: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从段落中通过正则提取职务-姓名对（如法定代表人：张三）。"""
        text = str(section.get("text") or "").strip()
        if not text:
            return []
        evidence_text = self._normalize_personnel_evidence_text(text)
        compact = self._compact(evidence_text)
        if not any(token in compact for token in self.PERSONNEL_CONTEXT_HINTS):
            return []

        entries: list[dict[str, Any]] = []
        for match in re.finditer(
            r"(?P<role>法定代表人|授权代表|授权委托人|委托代理人|被授权人)[^：:\n]{0,8}[：:]\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role=match.group("role"),
                evidence_text=evidence_text,
                source_type="personnel_line",
            )

        for match in re.finditer(
            r"(?P<role>法定代表人|授权代表|授权委托人|委托代理人|被授权人)\s*(?:为|是)?\s*(?P<name>[\u4e00-\u9fa5]{2,4})(?=$|[\s，,。；;、])",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role=match.group("role"),
                evidence_text=evidence_text,
                source_type="personnel_inline_role",
            )

        for match in re.finditer(
            r"(?:^|[\s（(：:])(?P<name>[\u4e00-\u9fa5]{2,4})\s+(?P<role>法定代表人|授权代表|授权委托人|委托代理人|被授权人)(?=$|[\s，,。；;、])",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role=match.group("role"),
                evidence_text=evidence_text,
                source_type="personnel_reverse_role",
            )

        for match in re.finditer(
            r"兹证明\s*[（(]?\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})\s*[）)]?[^。\n]{0,120}?系本公司法定代表人",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role="法定代表人",
                evidence_text=evidence_text,
                source_type="personnel_certificate",
            )

        for match in re.finditer(
            r"下面签字的\s*[（(]\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})\s*[、,，][^）)]{0,30}[）)]\s*代表本公司授权",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role="法定代表人",
                evidence_text=evidence_text,
                source_type="personnel_authorizer",
            )

        for match in re.finditer(
            r"授权下面签字的\s*[（(]\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})\s*[、,，]",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role="授权委托人",
                evidence_text=evidence_text,
                source_type="personnel_authorized_agent",
            )
        return entries

    def _extract_personnel_entries_from_key_value_table(
        self,
        record: dict[str, Any],
        table: dict[str, Any],
        rows: list[list[str]],
    ) -> list[dict[str, Any]]:
        """兼容营业执照/基本情况表这类“标签-值”表格中的人员字段。"""
        entries: list[dict[str, Any]] = []
        for row in rows:
            if len(row) < 2:
                continue
            for index in range(len(row) - 1):
                role = self._normalize_personnel_role_label(row[index])
                if not role:
                    continue
                name = self._clean_person_name(row[index + 1])
                if not name:
                    continue
                evidence_text = " | ".join(cell for cell in row if cell)
                entries.append(
                    self._build_personnel_entry(
                        record=record,
                        name=name,
                        role=role,
                        page=table.get("page"),
                        bbox=table.get("bbox"),
                        evidence_text=evidence_text,
                        source_type="personnel_key_value_table",
                    )
                )
        return entries

    def _append_personnel_match_entry(
        self,
        *,
        entries: list[dict[str, Any]],
        record: dict[str, Any],
        section: dict[str, Any],
        name: Any,
        role: Any,
        evidence_text: str,
        source_type: str,
    ) -> None:
        cleaned_name = self._clean_person_name(name)
        normalized_role = self._normalize_role(role)
        if (
            not cleaned_name
            or not normalized_role
            or self._person_name_conflicts_with_role(cleaned_name, normalized_role)
        ):
            return
        entries.append(
            self._build_personnel_entry(
                record=record,
                name=cleaned_name,
                role=normalized_role,
                page=section.get("page"),
                bbox=section.get("bbox"),
                evidence_text=evidence_text,
                source_type=source_type,
            )
        )

    def _build_personnel_entry(
        self,
        *,
        record: dict[str, Any],
        name: str,
        role: str,
        page: Any,
        bbox: Any,
        evidence_text: str,
        source_type: str,
    ) -> dict[str, Any]:
        """构造一条人员信息字典。"""
        entry = {
            "name": name,
            "role": role,
            "page": int(page) if isinstance(page, int) else None,
            "bbox": self._normalize_bbox(bbox),
            "text": self._normalize_text(evidence_text),
            "source_type": source_type,
            "document_identifier_id": str(record.get("identifier_id") or "").strip(),
            "relation_id": record.get("relation_id"),
            "file_name": record.get("file_name"),
        }
        entry["locations"] = [
            location for location in [
                make_location(
                    document_identifier_id=entry["document_identifier_id"],
                    file_name=entry.get("file_name"),
                    page=page,
                    bbox=bbox,
                    text=entry.get("text"),
                )
            ] if location
        ]
        return entry

    # HTML 表格行解析
    def _parse_html_table_rows(self, table: dict[str, Any]) -> list[list[str]]:
        """解析表格的 HTML 或纯文本，返回展开 rowspan/colspan 的二维字符串列表。"""
        block_content = str(table.get("block_content") or table.get("html") or "").strip()
        if "<table" not in block_content.lower():
            fallback_text = self._normalize_text(table.get("text") or table.get("raw_text") or "")
            if not fallback_text:
                return []
            return [[item] for item in fallback_text.splitlines() if item.strip()]

        parser = _TableHTMLParser()
        parser.feed(block_content)
        raw_rows = parser.rows
        if not raw_rows:
            return []

        active_spans: dict[int, dict[str, Any]] = {}
        expanded_rows: list[list[str]] = []
        max_columns = 0

        for raw_row in raw_rows:
            row: list[str] = []
            column_index = 0

            def extend_active_spans() -> None:
                nonlocal column_index
                while column_index in active_spans:
                    span_info = active_spans[column_index]
                    row.append(span_info["text"])
                    span_info["remaining"] -= 1
                    if span_info["remaining"] <= 0:
                        del active_spans[column_index]
                    column_index += 1

            extend_active_spans()
            for cell in raw_row:
                extend_active_spans()
                text = str(cell.get("text") or "").strip()
                rowspan = max(1, int(cell.get("rowspan") or 1))
                colspan = max(1, int(cell.get("colspan") or 1))
                for offset in range(colspan):
                    row.append(text)
                    if rowspan > 1:
                        active_spans[column_index + offset] = {
                            "text": text,
                            "remaining": rowspan - 1,
                        }
                column_index += colspan

            extend_active_spans()
            max_columns = max(max_columns, len(row))
            expanded_rows.append(row)

        for row in expanded_rows:
            while len(row) < max_columns:
                row.append("")
        return expanded_rows

    def _looks_like_personnel_header(self, header: list[str]) -> bool:
        """检查表头是否包含人员信息（同时存在'姓名'和岗位列）。"""
        compact_header = "".join(header)
        return "姓名" in compact_header and any(token in compact_header for token in self.ROLE_HEADER_HINTS)

    def _detect_personnel_section_pages(self, sections: list[dict[str, Any]]) -> set[int]:
        """从 heading 类型区段中识别人员章节所在页码集合。"""
        pages: set[int] = set()
        for section in sections:
            if str(section.get("type") or "").strip().lower() != "heading":
                continue
            compact = self._compact(section.get("text") or "")
            if compact and any(token in compact for token in self.PERSONNEL_SECTION_HINTS):
                page = section.get("page")
                if isinstance(page, int):
                    pages.add(page)
        return pages

    # 工具方法
    def _personnel_reuse_risk_level(self, roles: list[str], document_count: int) -> str:
        """根据复用文档数和关键岗位判断风险级别。"""
        if document_count >= 3:
            return "high"
        if any(role in {"项目经理", "项目负责人", "总负责人", "技术负责人"} for role in roles):
            return "high"
        return "medium"

    def _find_header_index(self, header: list[str], keywords: tuple[str, ...]) -> int | None:
        for index, value in enumerate(header):
            if any(keyword in value for keyword in keywords):
                return index
        return None

    def _safe_list_get(self, values: list[str], index: int | None) -> str:
        if index is None:
            return ""
        if 0 <= index < len(values):
            return str(values[index] or "")
        return ""

    def _normalize_role(self, value: Any) -> str:
        text = self._normalize_text(value)
        if not text:
            return ""
        compact = self._compact(text)
        for role in self.ROLE_TEXT_HINTS:
            if role in compact:
                return role
        if len(compact) <= 24:
            return compact
        return text[:24]

    def _normalize_personnel_role_label(self, value: Any) -> str:
        compact = self._compact(value)
        if not compact:
            return ""
        for role in self.PERSONNEL_ROLE_HINTS:
            if role in compact:
                return self._normalize_role(role)
        return ""

    def _personnel_entry_priority(self, entry: dict[str, Any]) -> int:
        source_type = str(entry.get("source_type") or "").strip()
        return int(self.PERSONNEL_SOURCE_PRIORITY.get(source_type, 0))

    def _clean_person_name(self, value: Any) -> str | None:
        """清洗人名，过滤掉占位符及明显非人名的字符串。"""
        text = self._normalize_text(value)
        if not text:
            return None

        text = re.sub(r"[\[\]【】()（）<>《》“”\"'·•]", "", text)
        text = re.sub(r"[\s:：,，;；/\\]+", "", text)
        if not text or len(text) > 20 or re.search(r"\d", text):
            return None
        if text in self.PERSON_NAME_PLACEHOLDERS:
            return None

        blocked = (
            "公司",
            "项目",
            "经理",
            "响应",
            "负责人",
            "工程师",
            "主管",
            "简历",
            "姓名",
            "岗位",
            "职位",
            "职务",
            "电话",
            "邮箱",
            "日期",
            "单位",
            "地址",
            "服务",
            "社会保险",
            "证明",
            "证明书",
            "身份证明",
            "委托书",
            "授权书",
            "签字",
            "盖章",
            "签章",
            "性别",
            "身份证",
            "资格",
            "证件号码",
        )
        if any(token in text for token in blocked):
            return None
        if self._looks_like_role_text(text):
            return None

        if re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", text):
            return text
        if re.fullmatch(r"[A-Za-z]{2,20}", text):
            return text
        if re.fullmatch(r"[\u4e00-\u9fa5A-Za-z]{2,8}", text) and len(
            re.findall(r"[\u4e00-\u9fa5]", text)
        ) >= 2:
            return text
        return None

    def _looks_like_role_text(self, value: Any) -> bool:
        compact = self._compact(value)
        if not compact:
            return False
        for role in self.PERSONNEL_ROLE_HINTS + self.ROLE_TEXT_HINTS:
            role_compact = self._compact(role)
            if not role_compact:
                continue
            if compact == role_compact or compact in role_compact or role_compact in compact:
                return True
        return False

    def _person_name_conflicts_with_role(self, name: str, role: str) -> bool:
        name_compact = self._compact(name)
        role_compact = self._compact(role)
        if not name_compact:
            return True
        if self._looks_like_role_text(name_compact):
            return True
        return bool(role_compact) and name_compact == role_compact

    def _page_count(self, sections: list[dict[str, Any]], tables: list[dict[str, Any]]) -> int:
        pages = {
            int(item["page"])
            for item in sections + tables
            if isinstance(item.get("page"), int)
        }
        return len(pages)

    def _normalize_requested_types(self, document_types: list[str] | None) -> tuple[str, ...]:
        if not document_types:
            return self.SUPPORTED_DOCUMENT_TYPES

        normalized: list[str] = []
        for item in document_types:
            role = self._normalize_document_role(item)
            if role not in self.SUPPORTED_DOCUMENT_TYPES:
                raise ValueError(f"Unsupported review document type: {item}")
            if role not in normalized:
                normalized.append(role)
        return tuple(normalized) if normalized else self.SUPPORTED_DOCUMENT_TYPES

    def _normalize_document_role(self, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"business", "business_bid"}:
            return DOCUMENT_TYPE_BUSINESS_BID
        if normalized in {"technical", "technical_bid"}:
            return DOCUMENT_TYPE_TECHNICAL_BID
        return normalized

    # 数据提取辅助（安全的 JSON 解析、标准化等）
    def _coerce_payload(self, payload: Any) -> dict[str, Any]:
        """将可能为字符串的 JSON 转为字典。"""
        if isinstance(payload, dict):
            return payload
        if not isinstance(payload, str):
            return {}
        text = payload.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _container(self, payload: dict[str, Any]) -> dict[str, Any]:
        """解包可能包裹在 'data' 字段中的真实数据。"""
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _sections(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """从 OCR 结果中提取标准化的版面区段列表。"""
        container = self._container(payload)
        raw_sections = container.get("layout_sections")
        if not isinstance(raw_sections, list):
            return []

        items: list[dict[str, Any]] = []
        for index, section in enumerate(raw_sections):
            if not isinstance(section, dict):
                continue
            text = self._normalize_text(section.get("text") or section.get("raw_text"))
            if not text:
                continue
            item = {
                "index": index,
                "page": int(section["page"]) if isinstance(section.get("page"), int) else None,
                "type": str(section.get("type") or "text").strip().lower() or "text",
                "text": text,
            }
            bbox = normalize_location_bbox(section.get("bbox") or section.get("box"))
            if bbox is not None:
                item["bbox"] = bbox
            items.append(item)
        return items

    def _native_tables(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """从 OCR 结果中提取标准化的表格列表。"""
        container = self._container(payload)
        raw_tables = container.get("native_tables") or container.get("logical_tables")
        if not isinstance(raw_tables, list):
            return []

        items: list[dict[str, Any]] = []
        for index, table in enumerate(raw_tables):
            if not isinstance(table, dict):
                continue
            item = {
                "index": index,
                "page": int(table["page"]) if isinstance(table.get("page"), int) else None,
                "block_content": str(table.get("block_content") or table.get("html") or ""),
                "text": self._normalize_text(table.get("raw_text") or table.get("text") or ""),
            }
            bbox = normalize_location_bbox(
                table.get("block_bbox")
                or table.get("bbox")
                or table.get("box")
                or table.get("block_polygon_points")
            )
            if bbox is not None:
                item["bbox"] = bbox
            items.append(item)
        return items

    def _normalize_text(self, value: Any) -> str:
        """文本归一化：反转义、替换全角空格、统一换行。"""
        text = html.unescape(str(value or ""))
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _normalize_personnel_evidence_text(self, value: Any) -> str:
        """清理 OCR 中的公式/下划线标记，便于抽取授权书和证明书中的姓名。"""
        text = self._normalize_text(value)
        text = re.sub(r"\\underline\s*\{\s*\\text\s*\{", "", text)
        text = re.sub(r"\\text\s*\{", "", text)
        text = re.sub(r"\\underline\s*\{", "", text)
        text = text.replace("$", " ")
        text = text.replace("{", "").replace("}", "")
        text = text.replace("\\", "")
        return self._normalize_text(text)

    def _compact(self, value: Any) -> str:
        """去除所有空白字符，用于关键词匹配。"""
        return re.sub(r"\s+", "", self._normalize_text(value))

    def _normalize_bbox(self, value: Any) -> list[int] | None:
        """将各种 bbox 格式统一为 [x, y, w, h] 整数列表。"""
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            if len(value) >= 4 and all(isinstance(item, (int, float)) for item in value[:4]):
                left, top, third, fourth = [int(round(float(item))) for item in value[:4]]
                if third >= left and fourth >= top:
                    return [left, top, max(third - left, 0), max(fourth - top, 0)]
                if third >= 0 and fourth >= 0:
                    return [left, top, third, fourth]
                return [min(left, third), min(top, fourth), abs(third - left), abs(fourth - top)]
            if value and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and all(isinstance(part, (int, float)) for part in item[:2])
                for item in value
            ):
                xs = [float(item[0]) for item in value]
                ys = [float(item[1]) for item in value]
                left = int(round(min(xs)))
                top = int(round(min(ys)))
                right = int(round(max(xs)))
                bottom = int(round(max(ys)))
                return [left, top, max(right - left, 0), max(bottom - top, 0)]
        return None

    def _bbox_top(self, bbox: Any) -> int:
        """获取 bbox 的顶部 y 坐标，用于排序。"""
        normalized = self._normalize_bbox(bbox)
        return normalized[1] if normalized else 0
