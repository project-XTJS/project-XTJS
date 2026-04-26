from __future__ import annotations

import html
import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from html.parser import HTMLParser
from typing import Any

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)


class _TableHTMLParser(HTMLParser):
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
    SUPPORTED_DOCUMENT_TYPES = (
        DOCUMENT_TYPE_BUSINESS_BID,
        DOCUMENT_TYPE_TECHNICAL_BID,
    )
    SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[。！？；;.!?])|[\r\n]+")
    NUMBERED_NAME_HEADING_PATTERN = re.compile(r"^\s*\d+\s*[.、]?\s*([\u4e00-\u9fa5A-Za-z]{2,20})\s*$")
    FIELD_NAME_PATTERN = re.compile(
        r"(?:姓名|缴费人名称|法定代表人|授权代表|授权委托人|委托代理人|被授权人)[^：:\n]{0,12}[：:]\s*([A-Za-z\u4e00-\u9fa5\[\]【】()（）]{2,30})"
    )
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
    ROLE_HEADER_HINTS = ("岗位", "职位", "职务", "职责")
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
        "法定代表人",
    )
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
    KNOWN_TYPO_MAP = {    
        "釆用": "采用",
        "釆购": "采购",
        "按装": "安装",
        "部暑": "部署",
        "侯选": "候选",
        "帐户": "账户",
        "帐号": "账号",
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

    def check_project_documents(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
    ) -> dict[str, Any]:
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

        groups: dict[str, Any] = {}
        total_document_count = 0
        total_skipped_document_count = 0
        total_typo_issue_count = 0
        total_shared_typo_issue_count = 0
        total_suspicious_typo_document_count = 0
        total_personnel_count = 0
        total_reused_name_count = 0

        for role in requested_types:
            prepared_documents = prepared_groups[role]
            skipped_documents = skipped_groups[role]
            typo_check = self._run_typo_check(prepared_documents)
            personnel_reuse_check = self._run_personnel_reuse_check(prepared_documents)
            group_summary = {
                "document_count": len(prepared_documents),
                "skipped_document_count": len(skipped_documents),
                "typo_issue_count": typo_check["issue_count"],
                "shared_typo_issue_count": typo_check["shared_issue_count"],
                "suspicious_typo_document_count": typo_check["suspicious_document_count"],
                "personnel_count": personnel_reuse_check["personnel_count"],
                "reused_name_count": personnel_reuse_check["reused_name_count"],
                "suspicious": bool(
                    typo_check["issue_count"] or personnel_reuse_check["reused_name_count"]
                ),
            }
            groups[role] = {
                "documents": [
                    {
                        "identifier_id": item["identifier_id"],
                        "relation_id": item.get("relation_id"),
                        "file_name": item.get("file_name"),
                        "page_count": item.get("page_count", 0),
                        "layout_section_count": item.get("layout_section_count", 0),
                        "table_count": item.get("table_count", 0),
                        "personnel_entry_count": len(item.get("personnel_entries") or []),
                    }
                    for item in prepared_documents
                ],
                "skipped_documents": skipped_documents,
                "typo_check": typo_check,
                "personnel_reuse_check": personnel_reuse_check,
                "summary": group_summary,
            }

            total_document_count += group_summary["document_count"]
            total_skipped_document_count += group_summary["skipped_document_count"]
            total_typo_issue_count += group_summary["typo_issue_count"]
            total_shared_typo_issue_count += group_summary["shared_typo_issue_count"]
            total_suspicious_typo_document_count += group_summary["suspicious_typo_document_count"]
            total_personnel_count += group_summary["personnel_count"]
            total_reused_name_count += group_summary["reused_name_count"]

        result = {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "document_types": list(requested_types),
                "typo_detection_engine": "rule_based",
                "typo_stopword_dictionary_enabled": True,
                "personnel_reuse_scope": "per_document_type",
            },
            "groups": groups,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": total_document_count,
                "skipped_document_count": total_skipped_document_count,
                "typo_issue_count": total_typo_issue_count,
                "shared_typo_issue_count": total_shared_typo_issue_count,
                "suspicious_typo_document_count": total_suspicious_typo_document_count,
                "personnel_count": total_personnel_count,
                "reused_name_count": total_reused_name_count,
                "suspicious": bool(
                    total_typo_issue_count or total_reused_name_count
                ),
            },
        }
        if len(requested_types) == 1:
            single_group = groups[requested_types[0]]
            result["documents"] = single_group["documents"]
            result["skipped_documents"] = single_group["skipped_documents"]
            result["typo_check"] = single_group["typo_check"]
            result["personnel_reuse_check"] = single_group["personnel_reuse_check"]
        return result

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

    def _run_typo_check(self, documents: list[dict[str, Any]]) -> dict[str, Any]:
        issue_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        document_issue_items: list[dict[str, Any]] = []
        total_issue_count = 0

        for document in documents:
            document_issues = self._extract_document_typo_issues(document)
            total_issue_count += len(document_issues)
            if document_issues:
                document_issue_items.append(
                    {
                        "identifier_id": document["identifier_id"],
                        "relation_id": document.get("relation_id"),
                        "file_name": document.get("file_name"),
                        "issue_count": len(document_issues),
                        "items": document_issues,
                    }
                )
            for issue in document_issues:
                issue_groups[str(issue.get("issue_key") or "")].append(issue)

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
                    "items": items,
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
            "engine": "rule_based",
            "documents": document_issue_items,
            "shared_issues": shared_issues,
            "notes": [
                "当前环境未启用 LanguageTool，本次使用内置错别字词典和人员姓名不一致规则识别。",
            ],
        }

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
                    "items": items,
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
            "items": reused_names,
            "notes": [
                "同名人员跨不同技术标重复出现时标记为疑似一人多用，建议结合原文页码与框选位置人工复核。",
            ],
        }

    def _extract_document_typo_issues(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()

        for sentence in self._iter_typo_sentences(document):
            for issue in self._find_known_typo_matches(sentence, document):
                key = (
                    issue.get("issue_type"),
                    issue.get("issue_key"),
                    issue.get("page"),
                    issue.get("matched_text"),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                issues.append(issue)

        for issue in self._find_person_name_mismatch_issues(document):
            key = (
                issue.get("issue_type"),
                issue.get("issue_key"),
                issue.get("page"),
                issue.get("matched_text"),
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
        issues: list[dict[str, Any]] = []
        text = str(sentence.get("text") or "")
        compact = self._compact(text)
        for typo_form, suggestion in self.KNOWN_TYPO_MAP.items():
            if typo_form not in compact:
                continue
            issues.append(
                {
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
            )
        return issues

    def _find_person_name_mismatch_issues(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        events = self._build_person_name_events(document)
        issues: list[dict[str, Any]] = []
        current_heading: dict[str, Any] | None = None

        for event in events:
            if event["event_type"] == "heading_name":
                current_heading = event
                continue
            if current_heading is None:
                continue
            if int(event.get("page") or 0) - int(current_heading.get("page") or 0) > 2:
                continue

            heading_name = str(current_heading.get("name") or "").strip()
            observed_name = str(event.get("name") or "").strip()
            if not self._is_name_mismatch_candidate(heading_name, observed_name):
                continue

            issues.append(
                {
                    "issue_type": "person_name_mismatch",
                    "issue_key": f"{heading_name}->{observed_name}",
                    "matched_text": observed_name,
                    "suggestion": heading_name,
                    "page": event.get("page"),
                    "bbox": event.get("bbox"),
                    "text": str(event.get("text") or ""),
                    "source": event.get("event_type"),
                    "reference_page": current_heading.get("page"),
                    "reference_text": current_heading.get("text"),
                    "document_identifier_id": document["identifier_id"],
                    "relation_id": document.get("relation_id"),
                    "file_name": document.get("file_name"),
                }
            )
        return issues

    def _build_person_name_events(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []

        for section in document.get("sections") or []:
            text = str(section.get("text") or "").strip()
            if not text:
                continue

            heading_match = self.NUMBERED_NAME_HEADING_PATTERN.match(text)
            if heading_match:
                name = self._clean_person_name(heading_match.group(1))
                if name:
                    events.append(
                        {
                            "event_type": "heading_name",
                            "name": name,
                            "page": section.get("page"),
                            "bbox": section.get("bbox"),
                            "text": text,
                            "sort_y": self._bbox_top(section.get("bbox")),
                        }
                    )

            for match in self.FIELD_NAME_PATTERN.finditer(text):
                name = self._clean_person_name(match.group(1))
                if not name:
                    continue
                events.append(
                    {
                        "event_type": "field_name",
                        "name": name,
                        "page": section.get("page"),
                        "bbox": section.get("bbox"),
                        "text": text,
                        "sort_y": self._bbox_top(section.get("bbox")),
                    }
                )

        for table in document.get("tables") or []:
            extracted = self._extract_resume_identity_from_table(table)
            if not extracted.get("name"):
                continue
            events.append(
                {
                    "event_type": "resume_table_name",
                    "name": extracted["name"],
                    "page": table.get("page"),
                    "bbox": table.get("bbox"),
                    "text": extracted.get("text") or "",
                    "sort_y": self._bbox_top(table.get("bbox")),
                }
            )

        events.sort(
            key=lambda item: (
                int(item.get("page") or 0),
                int(item.get("sort_y") or 0),
                0 if item.get("event_type") == "heading_name" else 1,
            )
        )
        return events

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

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for entry in entries:
            key = (
                entry.get("document_identifier_id"),
                entry.get("name"),
                entry.get("role"),
                entry.get("page"),
                tuple(entry.get("bbox") or []),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(entry)

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
            return entries

        identity = self._extract_resume_identity_from_table(table)
        if identity.get("name"):
            entries.append(
                self._build_personnel_entry(
                    record=record,
                    name=identity["name"],
                    role=self._normalize_role(identity.get("role") or ""),
                    page=table.get("page"),
                    bbox=table.get("bbox"),
                    evidence_text=identity.get("text") or "",
                    source_type="resume_table",
                )
            )
        return entries

    def _extract_personnel_entries_from_section(
        self,
        record: dict[str, Any],
        section: dict[str, Any],
    ) -> list[dict[str, Any]]:
        text = str(section.get("text") or "").strip()
        if not text:
            return []

        entries: list[dict[str, Any]] = []
        for match in re.finditer(
            r"(?P<role>法定代表人|授权代表|授权委托人|委托代理人|被授权人)[^：:\n]{0,8}[：:]\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})",
            text,
        ):
            name = self._clean_person_name(match.group("name"))
            if not name:
                continue
            entries.append(
                self._build_personnel_entry(
                    record=record,
                    name=name,
                    role=self._normalize_role(match.group("role")),
                    page=section.get("page"),
                    bbox=section.get("bbox"),
                    evidence_text=text,
                    source_type="personnel_line",
                )
            )
        return entries

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
        return {
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

    def _extract_resume_identity_from_table(self, table: dict[str, Any]) -> dict[str, str]:
        rows = self._parse_html_table_rows(table)
        if not rows:
            return {}

        name = None
        role = None
        for row in rows[:6]:
            for index, cell in enumerate(row):
                compact = self._compact(cell)
                if not compact:
                    continue
                if "姓名" in compact and name is None:
                    name = self._clean_person_name(self._safe_list_get(row, index + 1))
                if ("职位" in compact or any(token in compact for token in self.ROLE_HEADER_HINTS)) and role is None:
                    role = self._normalize_role(self._safe_list_get(row, index + 1))

        if not name:
            return {}

        flat_text = " | ".join(cell for row in rows[:6] for cell in row if cell)
        return {"name": name, "role": role or "", "text": flat_text}

    def _parse_html_table_rows(self, table: dict[str, Any]) -> list[list[str]]:
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
        compact_header = "".join(header)
        return "姓名" in compact_header and any(token in compact_header for token in self.ROLE_HEADER_HINTS)

    def _detect_personnel_section_pages(self, sections: list[dict[str, Any]]) -> set[int]:
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

    def _personnel_reuse_risk_level(self, roles: list[str], document_count: int) -> str:
        if document_count >= 3:
            return "high"
        if any(role in {"项目经理", "项目负责人", "总负责人", "技术负责人"} for role in roles):
            return "high"
        return "medium"

    def _is_name_mismatch_candidate(self, expected: str, observed: str) -> bool:
        if not expected or not observed or expected == observed:
            return False
        if len(expected) != len(observed):
            return False
        if expected[0] != observed[0]:
            return False
        difference_count = sum(1 for left, right in zip(expected, observed) if left != right)
        if difference_count <= 0:
              return False
        similarity = SequenceMatcher(None, expected, observed).ratio()
        return difference_count <= 2 and similarity >= 0.3

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

    def _clean_person_name(self, value: Any) -> str | None:
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
        )
        if any(token in text for token in blocked):
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

    def _coerce_payload(self, payload: Any) -> dict[str, Any]:
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
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _sections(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
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
            bbox = self._normalize_bbox(section.get("bbox") or section.get("box"))
            if bbox is not None:
                item["bbox"] = bbox
            items.append(item)
        return items

    def _native_tables(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
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
            bbox = self._normalize_bbox(
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
        text = html.unescape(str(value or ""))
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _compact(self, value: Any) -> str:
        return re.sub(r"\s+", "", self._normalize_text(value))

    def _normalize_bbox(self, value: Any) -> list[int] | None:
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
        normalized = self._normalize_bbox(bbox)
        return normalized[1] if normalized else 0
