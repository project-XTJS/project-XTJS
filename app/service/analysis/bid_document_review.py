from __future__ import annotations

import html
import json
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher
from html.parser import HTMLParser
from typing import Any

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
from .integrity import IntegrityChecker


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


class _ChinesePersonNER:
    DEFAULT_MODEL_NAME = os.getenv("BID_REVIEW_PERSON_NER_MODEL", "")
    DEFAULT_NER_TASK = os.getenv("BID_REVIEW_PERSON_NER_TASK", "ner/msra")
    DEFAULT_TOK_TASK = os.getenv("BID_REVIEW_PERSON_NER_TOK_TASK", "tok/fine")
    _pipeline = None
    _load_error: str | None = None

    @classmethod
    def is_available(cls) -> bool:
        try:
            cls._get_pipeline()
        except Exception:
            return False
        return True

    @classmethod
    def load_error(cls) -> str | None:
        return cls._load_error

    @classmethod
    def extract_names(cls, text: str) -> list[dict[str, Any]]:
        normalized_text = str(text or "").strip()
        if not normalized_text:
            return []

        try:
            pipeline = cls._get_pipeline()
        except Exception:
            return []

        document = pipeline(normalized_text, tasks=cls.DEFAULT_NER_TASK)
        raw_entities = list(document.get(cls.DEFAULT_NER_TASK) or [])
        tokens = list(document.get(cls.DEFAULT_TOK_TASK) or [])
        token_offsets = cls._build_token_offsets(normalized_text, tokens)

        entities: list[dict[str, Any]] = []
        for entity in raw_entities:
            if not isinstance(entity, (list, tuple)) or len(entity) < 4:
                continue
            _, label, start_token, end_token = entity[:4]
            if str(label).upper() != "PERSON":
                continue

            try:
                start_index = int(start_token)
                end_index = int(end_token)
            except (TypeError, ValueError):
                continue
            if start_index < 0 or end_index <= start_index:
                continue

            span = cls._resolve_token_span(
                normalized_text,
                token_offsets,
                start_index,
                end_index,
            )
            if not span:
                continue
            start, end = span
            text = normalized_text[start:end]
            if not text:
                continue
            entities.append(
                {
                    "start": start,
                    "end": end,
                    "score": 1.0,
                    "text": text,
                }
            )
        return entities

    @classmethod
    def _build_token_offsets(
        cls,
        text: str,
        tokens: list[Any],
    ) -> list[tuple[int, int]]:
        offsets: list[tuple[int, int]] = []
        cursor = 0
        for token in tokens:
            token_text = str(token or "")
            if not token_text:
                offsets.append((cursor, cursor))
                continue
            start = text.find(token_text, cursor)
            if start < 0:
                start = cursor
            end = start + len(token_text)
            offsets.append((start, end))
            cursor = end
        return offsets

    @classmethod
    def _resolve_token_span(
        cls,
        text: str,
        token_offsets: list[tuple[int, int]],
        start_index: int,
        end_index: int,
    ) -> tuple[int, int] | None:
        if token_offsets and end_index <= len(token_offsets):
            start = token_offsets[start_index][0]
            end = token_offsets[end_index - 1][1]
            if 0 <= start < end <= len(text):
                return start, end
        return None

    @classmethod
    def _get_pipeline(cls):
        if cls._pipeline is not None:
            return cls._pipeline

        try:
            import hanlp
            import hanlp.pretrained.mtl as hanlp_mtl

            model_name = cls.DEFAULT_MODEL_NAME or hanlp_mtl.CLOSE_TOK_POS_NER_SRL_DEP_SDP_CON_ELECTRA_SMALL_ZH
            cls._pipeline = hanlp.load(model_name)
            cls._load_error = None
            return cls._pipeline
        except Exception as exc:  # pragma: no cover - depends on runtime model availability
            cls._load_error = f"{type(exc).__name__}: {exc}"
            raise


class _ChineseTypoCorrector:
    DEFAULT_MODEL_NAME = os.getenv(
        "BID_REVIEW_TYPO_MODEL",
        "shibing624/macbert4csc-base-chinese",
    )
    DEFAULT_THRESHOLD = float(os.getenv("BID_REVIEW_TYPO_THRESHOLD", "0.9"))
    DEFAULT_BATCH_SIZE = int(os.getenv("BID_REVIEW_TYPO_BATCH_SIZE", "16"))
    DEFAULT_MAX_LENGTH = int(os.getenv("BID_REVIEW_TYPO_MAX_LENGTH", "128"))
    _corrector = None
    _load_error: str | None = None

    @classmethod
    def is_available(cls) -> bool:
        try:
            cls._get_corrector()
        except Exception:
            return False
        return True

    @classmethod
    def load_error(cls) -> str | None:
        return cls._load_error

    @classmethod
    def correct_batch(
        cls,
        sentences: list[str],
        *,
        threshold: float | None = None,
        batch_size: int | None = None,
        max_length: int | None = None,
    ) -> list[dict[str, Any]]:
        normalized = [str(item or "").strip() for item in sentences]
        if not normalized:
            return []

        try:
            corrector = cls._get_corrector()
        except Exception:
            return []

        return list(
            corrector.correct_batch(
                normalized,
                threshold=threshold if threshold is not None else cls.DEFAULT_THRESHOLD,
                batch_size=batch_size if batch_size is not None else cls.DEFAULT_BATCH_SIZE,
                max_length=max_length if max_length is not None else cls.DEFAULT_MAX_LENGTH,
                silent=True,
            )
            or []
        )

    @classmethod
    def _get_corrector(cls):
        if cls._corrector is not None:
            return cls._corrector

        try:
            from pycorrector.macbert.macbert_corrector import MacBertCorrector

            cls._corrector = MacBertCorrector(cls.DEFAULT_MODEL_NAME)
            cls._load_error = None
            return cls._corrector
        except Exception as exc:  # pragma: no cover - depends on runtime model availability
            cls._load_error = f"{type(exc).__name__}: {exc}"
            raise


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
    PERSONNEL_TEXT_TARGET_ATTACHMENT_HINTS = (
        ("法定代表人", "授权委托书"),
        ("法定代表人", "资格证明书"),
    )
    TENDER_PERSONNEL_REQUIREMENT_HINTS = (
        ("法定代表人", "证明书"),
        ("法定代表人", "资格证明书"),
        ("法定代表人", "授权委托书"),
        ("单位负责人", "证明书"),
        ("单位负责人", "授权委托书"),
        ("负责人", "证明书"),
        ("负责人", "授权委托书"),
    )
    PERSONNEL_SCOPE_FALLBACK_TITLES = (
        "附件 7-1 法定代表人资格证明书",
        "附件 7-2 法定代表人授权委托书",
        "法定代表人资格证明书",
        "法定代表人授权委托书",
    )
    ID_CARD_STRONG_HINTS = (
        "中华人民共和国居民身份证",
        "居民身份证",
        "公民身份号码",
        "签发机关",
        "有效期限",
    )
    ATTACHMENT_BODY_TEXT_HINTS = (
        "兹证明",
        "本授权书声明",
    )
    ATTACHMENT_SIGNATURE_TEXT_HINTS = (
        "签字或盖章",
        "签字盖章",
        "单位名称（盖公章）",
        "单位名称(盖公章)",
        "单位名称盖公章",
        "投标人（加盖公章）",
        "投标人(加盖公章)",
        "投标人名称（加盖公章）",
        "投标人名称(加盖公章)",
        "粘贴被授权人",
        "粘贴法定代表人",
        "身份证复印件",
        "身份证原件",
        "仅限投标使用",
    )
    TYPO_NATURAL_LANGUAGE_HINTS = (
        "的",
        "了",
        "和",
        "与",
        "并",
        "且",
        "为",
        "对",
        "在",
        "将",
        "按",
        "根据",
        "以及",
        "或者",
        "如果",
        "应",
        "需",
        "须",
        "请",
        "本项目",
        "本次",
        "文件",
        "方案",
        "服务",
        "系统",
        "采购",
        "投标",
    )
    TYPO_PROTECTED_SPAN_PATTERNS = (
        re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)+"),
        re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:/[A-Za-z0-9_./-]+)+"),
        re.compile(r"\b[A-Z][A-Z0-9_-]{1,}\b"),
        re.compile(r"(?:人民币|¥|￥)?\s*\d[\d,，.]*\s*(?:元|万元|亿元|%|％)"),
        re.compile(r"\b\d{6,}[0-9XxA-Za-z-]*\b"),
        re.compile(
            r"[\u4e00-\u9fa5A-Za-z0-9()（）·\-.]{2,60}"
            r"(?:公司|集团|银行|医院|大学|学院|研究院|研究所|中心|分公司|事务所|委员会|厂|院|所)"
        ),
        re.compile(r"《[^》]{2,80}》"),
        re.compile(r"(?:API|api|接口|Interface|interface)[A-Za-z0-9_./-]*"),
    )
    TYPO_PROTECTED_LABEL_PATTERNS = (
        re.compile(r"(?:项目名称|采购项目名称|项目编号|标段名称|包件名称)\s*[:：]\s*([^\n]{2,80})"),
        re.compile(r"(?:投标人|供应商|申请人|单位名称|公司名称)\s*[:：]\s*([^\n]{2,80})"),
        re.compile(r"(?:接口名称|接口名|API名称|API)\s*[:：]\s*([^\n]{2,80})"),
    )
    TYPO_REJECT_REPLACEMENT_PAIRS = {
        ("余", "馀"),
        ("记", "纪"),
        ("场", "厂"),
        ("台", "合"),
        ("作", "做"),
        ("声", "申"),
        ("覆", "复"),
        ("机", "即"),
        ("新", "心"),
        ("璃", "境"),
        ("工", "物"),
        ("备", "各"),
        ("在", "再"),
        ("演", "转"),
        ("职", "执"),
        ("及", "即"),
        ("须", "需"),
        ("法", "运"),
    }
    TYPO_MAX_REPLACEMENT_SPAN = 4
    TYPO_MAX_CHANGED_SPAN_COUNT = 1
    TYPO_MAX_CHANGED_CHAR_COUNT = 2
    TABLE_HEADER_SCAN_LIMIT = 6
    TABLE_NAME_HEADER_CORE = "姓名"
    TABLE_NAME_HEADER_BLOCK_TOKENS = (
        "签名",
        "盖章",
        "姓名章",
        "姓名拼音",
        "用户名",
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
    PERSON_NAME_TEMPLATE_BLOCKLIST = {
        "\u76d6\u516c\u7ae0",
        "\u7b7e\u5b57\u6216\u76d6\u7ae0",
        "\u88ab\u6388\u6743\u4eba\u7b7e\u5b57",
        "\u88ab\u6388\u6743\u4eba\u7b7e\u5b57\u6216\u76d6\u7ae0",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba\u7b7e\u5b57",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba\u7b7e\u5b57\u6216\u76d6\u7ae0",
        "\u6388\u6743\u4ee3\u8868\u7b7e\u5b57",
        "\u6388\u6743\u4ee3\u8868\u7b7e\u5b57\u6216\u76d6\u7ae0",
        "\u5355\u4f4d\u540d\u79f0\u76d6\u516c\u7ae0",
        "\u5355\u4f4d\u540d\u79f0\uff08\u76d6\u516c\u7ae0\uff09",
    }
    PERSON_NAME_TEMPLATE_BLOCK_TOKENS = (
        "\u76d6\u516c\u7ae0",
        "\u7b7e\u5b57\u6216\u76d6\u7ae0",
        "\u7b7e\u5b57\u5e76\u76d6\u7ae0",
        "\u7b7e\u5b57\u76d6\u7ae0",
        "\u7b7e\u7ae0",
        "\u516c\u7ae0",
        "\u5355\u4f4d\u540d\u79f0",
        "\u88ab\u6388\u6743\u4eba\u7b7e\u5b57",
        "\u6cd5\u5b9a\u4ee3\u8868\u4eba\u7b7e\u5b57",
        "\u6388\u6743\u4ee3\u8868\u7b7e\u5b57",
    )
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

    def __init__(self) -> None:
        self._integrity_checker = IntegrityChecker()

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
                "typo_detection_engine": "rule_filter_plus_macbert",
                "typo_model_name": _ChineseTypoCorrector.DEFAULT_MODEL_NAME,
                "typo_model_threshold": _ChineseTypoCorrector.DEFAULT_THRESHOLD,
                "typo_stopword_dictionary_enabled": True,
                "personnel_reuse_scope": "per_document_type",
                "personnel_table_extraction_engine": "required_attachment_name_column",
                "personnel_text_extraction_engine": "attachment_hanlp_ner",
                "business_bid_personnel_scope": "required_attachments_only",
                "technical_bid_personnel_scope": "disabled",
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
        role = self._normalize_document_role(
            record.get("relation_role") or record.get("document_type")
        )
        sections = self._sections(payload)
        tables = self._native_tables(payload)
        if not sections and not tables:
            return None, self.SKIP_REASON_MISSING_CONTENT

        personnel_sections: list[dict[str, Any]] = []
        personnel_tables: list[dict[str, Any]] = []
        personnel_scope_mode = "disabled"
        personnel_scope_titles: list[str] = []
        target_attachment_pages: set[int] = set()
        if role == DOCUMENT_TYPE_BUSINESS_BID:
            personnel_sections, personnel_tables, personnel_scope_titles = self._scope_business_required_attachments(
                record=record,
                payload=payload,
                sections=sections,
                tables=tables,
            )
            personnel_scope_mode = "required_attachments_only"
            target_attachment_pages = self._detect_target_attachment_pages(personnel_sections)

        personnel_section_pages = self._detect_personnel_section_pages(personnel_sections)
        personnel_entries = self._extract_personnel_entries(
            record=record,
            sections=personnel_sections,
            tables=personnel_tables,
            target_attachment_pages=target_attachment_pages,
        )
        personnel_pages = set(personnel_section_pages)
        personnel_pages.update(target_attachment_pages)
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
            "personnel_scope_mode": personnel_scope_mode,
            "personnel_scope_titles": personnel_scope_titles,
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
        model_available = _ChineseTypoCorrector.is_available()
        notes = [
            "Typo detection runs as rule-filtered MacBERT candidate review.",
            "Only single-char or same-length replacements are accepted, and protected spans stay blocked.",
        ]
        if model_available:
            notes.append(
                f"Enabled pycorrector + MacBERT model: {_ChineseTypoCorrector.DEFAULT_MODEL_NAME}"
            )
        else:
            notes.append("MacBERT is unavailable, fallback to rule-based typo detection.")
            if _ChineseTypoCorrector.load_error():
                notes.append(f"Model load error: {_ChineseTypoCorrector.load_error()}")

        return {
            "document_count": len(documents),
            "issue_count": total_issue_count,
            "shared_issue_count": len(shared_issues),
            "suspicious_document_count": len(document_issue_items),
            "engine": "rule_filter_plus_macbert" if model_available else "rule_based_fallback",
            "documents": document_issue_items,
            "shared_issues": shared_issues,
            "notes": notes,
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

            reused_names.append(
                {
                    "name": name,
                    "document_count": len(document_ids),
                    "occurrence_count": len(items),
                    "risk_level": self._personnel_reuse_risk_level(len(document_ids)),
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
                "先按招标文件要求锁定商务标附件范围，再只在这些附件的表格里按“姓名”列向下提取成员姓名。",
                "正文继续仅在法定代表人授权委托书、法定代表人资格证明书中使用 HanLP 识别人名，并排除身份证与签字盖章区域。",
                "同名人员跨不同文档重复出现时标记为疑似一人多用，建议结合原文页码与框选位置人工复核。",
            ],
        }

    def _extract_document_typo_issues(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()
        typo_sentences = self._iter_typo_sentences(document)

        for sentence in typo_sentences:
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

        for issue in self._find_model_typo_matches(typo_sentences, document):
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
        skip_pages = self._detect_typo_skip_pages(document)

        for section in document.get("sections") or []:
            section_type = str(section.get("type") or "").strip().lower()
            if section_type in {"seal", "signature"}:
                continue
            page = section.get("page")
            if isinstance(page, int) and page in skip_pages:
                continue

            raw_text = str(section.get("text") or "").strip()
            if not raw_text:
                continue

            for sentence in self.SENTENCE_SPLIT_PATTERN.split(raw_text):
                normalized = self._normalize_text(sentence)
                compact = self._compact(normalized)
                if len(compact) < 10 or self._should_skip_typo_sentence(normalized):
                    continue
                if not self._looks_like_natural_language_sentence(normalized):
                    continue
                items.append(
                    {
                        "page": page,
                        "bbox": section.get("bbox"),
                        "text": normalized,
                    }
                )
        return items

    def _detect_typo_skip_pages(self, document: dict[str, Any]) -> set[int]:
        skip_pages = set(document.get("personnel_pages") or set())
        page_stats: dict[int, dict[str, int]] = defaultdict(lambda: {"section_count": 0, "dense_count": 0})

        for section in document.get("sections") or []:
            page = section.get("page")
            if not isinstance(page, int):
                continue

            text = str(section.get("text") or "").strip()
            compact = self._compact(text)
            section_type = str(section.get("type") or "").strip().lower()
            if section_type in {"seal", "signature"}:
                skip_pages.add(page)
                continue
            if not compact:
                continue
            if self._looks_like_identity_card_text(compact):
                skip_pages.add(page)
                continue
            if self._looks_like_attachment_signature_text(compact):
                skip_pages.add(page)
                continue

            stats = page_stats[page]
            stats["section_count"] += 1
            if self._looks_like_dense_number_text(compact):
                stats["dense_count"] += 1

        for page, stats in page_stats.items():
            section_count = int(stats.get("section_count") or 0)
            dense_count = int(stats.get("dense_count") or 0)
            if dense_count >= 2 and dense_count >= max(2, section_count // 2):
                skip_pages.add(page)
        return skip_pages

    def _looks_like_dense_number_text(self, compact_text: str) -> bool:
        if not compact_text:
            return False
        digit_count = sum(ch.isdigit() for ch in compact_text)
        if digit_count >= max(10, len(compact_text) // 3):
            return True
        long_number_count = len(re.findall(r"\d{6,}", compact_text))
        return long_number_count >= 2

    def _looks_like_natural_language_sentence(self, text: str) -> bool:
        compact = self._compact(text)
        if not compact:
            return False

        chinese_count = len(re.findall(r"[\u4e00-\u9fa5]", compact))
        if chinese_count < 6:
            return False
        if re.fullmatch(r"[\u4e00-\u9fa5A-Za-z0-9\s:：/_.-]+", compact) and "，" not in text and "。" not in text:
            if not any(token in compact for token in self.TYPO_NATURAL_LANGUAGE_HINTS):
                return False
        return any(token in compact for token in self.TYPO_NATURAL_LANGUAGE_HINTS) or any(
            mark in text for mark in ("，", "。", "；", "：")
        )

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

    def _find_model_typo_matches(
        self,
        sentences: list[dict[str, Any]],
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if not sentences or not _ChineseTypoCorrector.is_available():
            return []

        source_sentences = [str(item.get("text") or "") for item in sentences]
        results = _ChineseTypoCorrector.correct_batch(source_sentences)
        if not results:
            return []

        issues: list[dict[str, Any]] = []
        for sentence, result in zip(sentences, results):
            source_text = str(result.get("source") or sentence.get("text") or "")
            target_text = str(result.get("target") or source_text)
            if not source_text or source_text == target_text:
                continue

            changes = self._extract_high_confidence_typo_changes(source_text, target_text)
            for change in changes:
                issues.append(
                    {
                        "issue_type": "model_typo",
                        "issue_key": f"{change['source']}->{change['target']}",
                        "matched_text": change["source"],
                        "suggestion": change["target"],
                        "page": sentence.get("page"),
                        "bbox": sentence.get("bbox"),
                        "text": source_text,
                        "corrected_text": target_text,
                        "change_start": change["start"],
                        "change_end": change["end"],
                        "document_identifier_id": document["identifier_id"],
                        "relation_id": document.get("relation_id"),
                        "file_name": document.get("file_name"),
                    }
                )
        return issues

    def _extract_high_confidence_typo_changes(
        self,
        source_text: str,
        target_text: str,
    ) -> list[dict[str, Any]]:
        if not source_text or not target_text or source_text == target_text:
            return []

        protected_spans = self._collect_typo_protected_spans(source_text)
        changes: list[dict[str, Any]] = []
        total_changed_char_count = 0
        opcodes = SequenceMatcher(None, source_text, target_text).get_opcodes()
        for tag, i1, i2, j1, j2 in opcodes:
            if tag == "equal":
                continue
            if tag != "replace":
                return []

            source_span = source_text[i1:i2]
            target_span = target_text[j1:j2]
            if not self._is_allowed_model_typo_span(
                source_text,
                source_span,
                target_span,
                start=i1,
                end=i2,
                protected_spans=protected_spans,
            ):
                return []

            total_changed_char_count += len(source_span)
            changes.append(
                {
                    "start": i1,
                    "end": i2,
                    "source": source_span,
                    "target": target_span,
                }
            )

        if not changes:
            return []
        if len(changes) > self.TYPO_MAX_CHANGED_SPAN_COUNT:
            return []
        if total_changed_char_count > self.TYPO_MAX_CHANGED_CHAR_COUNT:
            return []
        return changes

    def _is_allowed_model_typo_span(
        self,
        sentence: str,
        source_span: str,
        target_span: str,
        *,
        start: int,
        end: int,
        protected_spans: list[tuple[int, int]],
    ) -> bool:
        if not source_span or not target_span or source_span == target_span:
            return False
        if len(source_span) != len(target_span):
            return False
        if len(source_span) > self.TYPO_MAX_REPLACEMENT_SPAN:
            return False
        if (source_span, target_span) in self.TYPO_REJECT_REPLACEMENT_PAIRS:
            return False
        if not re.fullmatch(r"[\u4e00-\u9fa5]+", source_span):
            return False
        if not re.fullmatch(r"[\u4e00-\u9fa5]+", target_span):
            return False
        if self._span_overlaps_protected(start, end, protected_spans):
            return False

        left_context = sentence[max(0, start - 1):start]
        right_context = sentence[end:min(len(sentence), end + 1)]
        context = f"{left_context}{source_span}{right_context}"
        if re.search(r"[A-Za-z0-9]", context):
            return False
        return True

    def _collect_typo_protected_spans(self, text: str) -> list[tuple[int, int]]:
        spans: list[tuple[int, int]] = []
        normalized = str(text or "")
        if not normalized:
            return spans

        for pattern in self.TYPO_PROTECTED_SPAN_PATTERNS:
            for match in pattern.finditer(normalized):
                start, end = match.span()
                if end > start:
                    spans.append((start, end))

        for pattern in self.TYPO_PROTECTED_LABEL_PATTERNS:
            for match in pattern.finditer(normalized):
                start, end = match.span(1)
                if end > start:
                    spans.append((start, end))

        return self._merge_spans(spans)

    def _merge_spans(self, spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
        if not spans:
            return []
        ordered = sorted(spans)
        merged: list[tuple[int, int]] = [ordered[0]]
        for start, end in ordered[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
                continue
            merged.append((start, end))
        return merged

    def _span_overlaps_protected(
        self,
        start: int,
        end: int,
        protected_spans: list[tuple[int, int]],
    ) -> bool:
        for protected_start, protected_end in protected_spans:
            if start < protected_end and end > protected_start:
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
        target_attachment_pages: set[int],
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []

        for table in tables:
            entries.extend(self._extract_personnel_entries_from_table(record, table))

        for section in sections:
            section_entries = self._extract_personnel_entries_from_section(
                record,
                section,
                target_attachment_pages=target_attachment_pages,
            )
            if section_entries:
                entries.extend(section_entries)

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for entry in entries:
            key = (
                entry.get("document_identifier_id"),
                entry.get("name"),
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
            )
        )
        return deduped

    def _extract_personnel_entries_from_table(
        self,
        record: dict[str, Any],
        table: dict[str, Any],
    ) -> list[dict[str, Any]]:
        return self._extract_name_column_entries_from_table(record, table)

    def _extract_personnel_entries_from_section(
        self,
        record: dict[str, Any],
        section: dict[str, Any],
        *,
        target_attachment_pages: set[int],
    ) -> list[dict[str, Any]]:
        text = str(section.get("text") or "").strip()
        if not text:
            return []
        if not self._should_extract_attachment_text_ner(
            section,
            target_attachment_pages=target_attachment_pages,
        ):
            return []

        entries: list[dict[str, Any]] = []
        for name, candidate_text in self._extract_attachment_names_from_text(text):
            entries.append(
                self._build_personnel_entry(
                    record=record,
                    name=name,
                    page=section.get("page"),
                    bbox=section.get("bbox"),
                    evidence_text=candidate_text,
                    source_type="attachment_ner",
                )
            )
        return entries

    def _should_extract_attachment_text_ner(
        self,
        section: dict[str, Any],
        *,
        target_attachment_pages: set[int],
    ) -> bool:
        section_type = str(section.get("type") or "").strip().lower()
        if section_type != "text":
            return False

        page = section.get("page")
        if not isinstance(page, int) or page not in target_attachment_pages:
            return False

        compact = self._compact(section.get("text") or "")
        if not compact or len(compact) < 2:
            return False
        if self._looks_like_identity_card_text(compact):
            return False
        if self._looks_like_attachment_signature_text(compact):
            return False
        return self._looks_like_attachment_body_text(compact)

    def _extract_attachment_names_from_text(self, text: str) -> list[tuple[str, str]]:
        entries: list[tuple[str, str]] = []
        seen: set[str] = set()
        normalized = self._normalize_text(text)
        if not normalized:
            return entries

        for entity in _ChinesePersonNER.extract_names(normalized):
            name = self._clean_person_name(entity.get("text"))
            if not name or name in seen:
                continue
            seen.add(name)
            entries.append((name, normalized))
        return entries

    def _looks_like_identity_card_text(self, compact_text: str) -> bool:
        if not compact_text:
            return False
        if any(token in compact_text for token in self.ID_CARD_STRONG_HINTS):
            return True

        field_tokens = (
            "姓名",
            "性别",
            "民族",
            "出生",
            "住址",
            "公民身份号码",
            "身份证号码",
            "签发机关",
            "有效期限",
        )
        field_hits = sum(1 for token in field_tokens if token in compact_text)
        return field_hits >= 3

    def _looks_like_attachment_signature_text(self, compact_text: str) -> bool:
        if not compact_text:
            return False
        return any(token in compact_text for token in self.ATTACHMENT_SIGNATURE_TEXT_HINTS)

    def _looks_like_attachment_body_text(self, compact_text: str) -> bool:
        if not compact_text:
            return False
        return any(token in compact_text for token in self.ATTACHMENT_BODY_TEXT_HINTS)

    def _scope_business_required_attachments(
        self,
        *,
        record: dict[str, Any],
        payload: dict[str, Any],
        sections: list[dict[str, Any]],
        tables: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        tender_payload = self._coerce_payload(record.get("tender_content"))
        if not tender_payload:
            return [], [], []

        try:
            integrity_raw = self._integrity_checker.check_integrity(tender_payload, payload)
        except Exception:
            return [], [], []

        details = integrity_raw.get("details", {}) if isinstance(integrity_raw, dict) else {}
        target_titles: list[str] = []
        for item_name, detail in details.items():
            if not isinstance(detail, dict) or not detail.get("scored", True):
                continue
            target_titles.extend(self._collect_integrity_scope_titles(item_name, detail))
        target_titles.extend(self._collect_personnel_scope_titles_from_tender(tender_payload))

        if not target_titles:
            return [], [], []

        normalized_targets = {
            self._normalize_attachment_scope_title(title)
            for title in target_titles
            if self._normalize_attachment_scope_title(title)
        }
        if not normalized_targets:
            return [], [], []

        heading_indexes = [
            index
            for index, section in enumerate(sections)
            if str(section.get("type") or "").strip().lower() == "heading"
            and self._matches_required_attachment_heading(section.get("text") or "", normalized_targets)
        ]
        if not heading_indexes:
            return [], [], sorted({title for title in target_titles if title})

        scoped_sections: list[dict[str, Any]] = []
        scoped_pages: set[int] = set()
        all_heading_indexes = [
            index
            for index, section in enumerate(sections)
            if str(section.get("type") or "").strip().lower() == "heading"
        ]
        for start_index in heading_indexes:
            next_heading_index = len(sections)
            for candidate in all_heading_indexes:
                if candidate > start_index:
                    next_heading_index = candidate
                    break
            for section in sections[start_index:next_heading_index]:
                scoped_sections.append(section)
                if isinstance(section.get("page"), int):
                    scoped_pages.add(int(section["page"]))

        deduped_scoped_sections: list[dict[str, Any]] = []
        seen_section_keys: set[tuple[Any, ...]] = set()
        for section in scoped_sections:
            key = (
                section.get("index"),
                section.get("page"),
                section.get("type"),
                section.get("text"),
            )
            if key in seen_section_keys:
                continue
            seen_section_keys.add(key)
            deduped_scoped_sections.append(section)

        scoped_tables = [
            table
            for table in tables
            if isinstance(table.get("page"), int) and int(table["page"]) in scoped_pages
        ]
        return deduped_scoped_sections, scoped_tables, sorted({title for title in target_titles if title})

    def _collect_integrity_scope_titles(
        self,
        item_name: Any,
        detail: dict[str, Any],
    ) -> list[str]:
        titles: list[str] = []
        item_text = str(item_name or "").strip()
        if item_text:
            titles.append(item_text)

        preview = str(detail.get("preview") or "").strip()
        if preview and preview != "-":
            titles.extend(
                part.strip()
                for part in re.split(r"\s*[|｜；;]\s*", preview)
                if part.strip() and part.strip() != "-"
            )

        normalized_item = self._normalize_attachment_scope_title(item_text)
        if "法定代表人" in normalized_item and "证明书" in normalized_item and "授权委托书" in normalized_item:
            titles.extend(
                (
                    "附件 7-1 法定代表人资格证明书",
                    "附件 7-2 法定代表人授权委托书",
                    "法定代表人资格证明书",
                    "法定代表人授权委托书",
                )
            )

        deduped: list[str] = []
        seen: set[str] = set()
        for title in titles:
            normalized_title = self._normalize_attachment_scope_title(title)
            if not normalized_title or normalized_title in seen:
                continue
            seen.add(normalized_title)
            deduped.append(title)
        return deduped

    def _collect_personnel_scope_titles_from_tender(
        self,
        tender_payload: dict[str, Any],
    ) -> list[str]:
        sections = self._sections(tender_payload)
        if not sections:
            return []

        normalized_texts = [
            self._compact(section.get("text") or "")
            for section in sections
            if self._compact(section.get("text") or "")
        ]
        if not normalized_texts:
            return []

        if not any(
            all(token in text for token in token_group)
            for text in normalized_texts
            for token_group in self.TENDER_PERSONNEL_REQUIREMENT_HINTS
        ):
            return []

        return list(self.PERSONNEL_SCOPE_FALLBACK_TITLES)

    def _matches_required_attachment_heading(
        self,
        heading_text: Any,
        normalized_targets: set[str],
    ) -> bool:
        normalized_heading = self._normalize_attachment_scope_title(heading_text)
        if not normalized_heading:
            return False
        return any(
            normalized_heading == target
            or normalized_heading in target
            or target in normalized_heading
            for target in normalized_targets
        )

    def _normalize_attachment_scope_title(self, value: Any) -> str:
        text = self._normalize_text(value)
        if not text:
            return ""
        text = re.sub(r"^\s*(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*[、.)）．]?\s*", "", text)
        text = re.sub(r"^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*", "", text)
        text = re.sub(r"^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*", "", text)
        text = re.sub(r"\s+", "", text)
        return "".join(ch for ch in text if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")

    def _build_personnel_entry(
        self,
        *,
        record: dict[str, Any],
        name: str,
        page: Any,
        bbox: Any,
        evidence_text: str,
        source_type: str,
    ) -> dict[str, Any]:
        return {
            "name": name,
            "page": int(page) if isinstance(page, int) else None,
            "bbox": self._normalize_bbox(bbox),
            "text": self._normalize_text(evidence_text),
            "source_type": source_type,
            "document_identifier_id": str(record.get("identifier_id") or "").strip(),
            "relation_id": record.get("relation_id"),
            "file_name": record.get("file_name"),
        }

    def _extract_name_column_entries_from_table(
        self,
        record: dict[str, Any],
        table: dict[str, Any],
    ) -> list[dict[str, Any]]:
        rows = self._parse_html_table_rows(table)
        if not rows:
            return []

        header_match = self._locate_name_column_header(rows)
        if header_match is None:
            return []

        header_row_index, name_indexes = header_match

        entries: list[dict[str, Any]] = []
        for row in rows[header_row_index + 1:]:
            evidence_text = " | ".join(cell for cell in row if str(cell or "").strip())
            if not evidence_text:
                continue

            for name_index in name_indexes:
                name = self._clean_person_name(self._safe_list_get(row, name_index))
                if not name:
                    continue
                entries.append(
                    self._build_personnel_entry(
                        record=record,
                        name=name,
                        page=table.get("page"),
                        bbox=table.get("bbox"),
                        evidence_text=evidence_text,
                        source_type="name_column_table",
                    )
                )
        return entries

    def _extract_resume_identity_from_table(self, table: dict[str, Any]) -> dict[str, str]:
        rows = self._parse_html_table_rows(table)
        if not rows:
            return {}

        header_match = self._locate_name_column_header(rows)
        if header_match is None:
            return {}

        header_row_index, name_indexes = header_match
        for row in rows[header_row_index + 1:]:
            evidence_text = " | ".join(cell for cell in row if str(cell or "").strip())
            if not evidence_text:
                continue
            for name_index in name_indexes:
                name = self._clean_person_name(self._safe_list_get(row, name_index))
                if name:
                    return {"name": name, "text": evidence_text}
        return {}

    def _locate_name_column_header(
        self,
        rows: list[list[str]],
    ) -> tuple[int, list[int]] | None:
        scan_limit = min(len(rows), self.TABLE_HEADER_SCAN_LIMIT)
        for row_index in range(scan_limit):
            compact_row = [self._compact(cell) for cell in rows[row_index]]
            name_indexes = [
                index
                for index, compact in enumerate(compact_row)
                if self._is_name_column_header(compact)
            ]
            if name_indexes:
                return row_index, name_indexes
        return None

    def _is_name_column_header(self, compact_text: str) -> bool:
        if not compact_text:
            return False
        if any(token in compact_text for token in self.TABLE_NAME_HEADER_BLOCK_TOKENS):
            return False
        if compact_text == self.TABLE_NAME_HEADER_CORE:
            return True
        return (
            self.TABLE_NAME_HEADER_CORE in compact_text
            and compact_text.endswith(self.TABLE_NAME_HEADER_CORE)
            and len(compact_text) <= 6
        )

    def _parse_html_table_rows(self, table: dict[str, Any]) -> list[list[str]]:
        rows = table.get("rows")
        if isinstance(rows, list) and rows:
            parsed_rows: list[list[str]] = []
            for row in rows:
                if isinstance(row, dict):
                    cells = [self._normalize_text(value) for value in row.values()]
                elif isinstance(row, list):
                    cells = [self._normalize_text(value) for value in row]
                else:
                    cells = [self._normalize_text(row)]
                normalized_cells = [cell for cell in cells if cell]
                if normalized_cells:
                    parsed_rows.append(normalized_cells)
            if parsed_rows:
                return parsed_rows

        records = table.get("records")
        if isinstance(records, list) and records:
            record_rows: list[list[str]] = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                cells = [self._normalize_text(value) for value in record.values()]
                normalized_cells = [cell for cell in cells if cell]
                if normalized_cells:
                    record_rows.append(normalized_cells)
            if record_rows:
                return record_rows

        block_content = str(table.get("block_content") or table.get("html") or "").strip()
        if "<table" not in block_content.lower():
            fallback_text = self._normalize_text(
                table.get("text") or table.get("raw_text") or table.get("block_content") or ""
            )
            if not fallback_text:
                return []
            rows: list[list[str]] = []
            for line in fallback_text.splitlines():
                normalized_line = self._normalize_text(line)
                if not normalized_line:
                    continue
                if "|" in normalized_line:
                    cells = [self._normalize_text(item) for item in normalized_line.split("|")]
                elif "\t" in normalized_line:
                    cells = [self._normalize_text(item) for item in re.split(r"\t+", normalized_line)]
                else:
                    cells = [self._normalize_text(item) for item in re.split(r"\s{2,}", normalized_line)]
                normalized_cells = [cell for cell in cells if cell]
                if normalized_cells:
                    rows.append(normalized_cells)
            return rows

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

    def _detect_target_attachment_pages(self, sections: list[dict[str, Any]]) -> set[int]:
        heading_indexes = [
            index
            for index, section in enumerate(sections)
            if self._matches_personnel_text_attachment_heading(section)
        ]
        if not heading_indexes:
            return set()

        attachment_pages: set[int] = set()
        all_heading_indexes = [
            index
            for index, section in enumerate(sections)
            if str(section.get("type") or "").strip().lower() == "heading"
        ]
        for start_index in heading_indexes:
            next_heading_index = len(sections)
            for candidate in all_heading_indexes:
                if candidate > start_index:
                    next_heading_index = candidate
                    break
            for section in sections[start_index:next_heading_index]:
                page = section.get("page")
                if isinstance(page, int):
                    attachment_pages.add(page)
        return attachment_pages

    def _matches_personnel_text_attachment_heading(self, section: dict[str, Any]) -> bool:
        section_type = str(section.get("type") or "").strip().lower()
        if section_type != "heading":
            return False

        normalized_heading = self._normalize_attachment_scope_title(section.get("text") or "")
        if not normalized_heading:
            return False

        return any(
            all(token in normalized_heading for token in token_group)
            for token_group in self.PERSONNEL_TEXT_TARGET_ATTACHMENT_HINTS
        )

    def _personnel_reuse_risk_level(self, document_count: int) -> str:
        if document_count >= 3:
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
        if text in self.PERSON_NAME_TEMPLATE_BLOCKLIST:
            return None
        if any(token in text for token in self.PERSON_NAME_TEMPLATE_BLOCK_TOKENS):
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
            "证件",
            "号码",
            "缴费",
            "基数",
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
        source_items: list[tuple[str, int, dict[str, Any]]] = []
        for source_key in ("native_tables", "logical_tables", "table_sections"):
            raw_tables = container.get(source_key)
            if not isinstance(raw_tables, list):
                continue
            for index, table in enumerate(raw_tables):
                if isinstance(table, dict):
                    source_items.append((source_key, index, table))
        if not source_items:
            return []

        items: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()
        for source_key, index, table in source_items:
            item = {
                "index": index,
                "source_key": source_key,
                "page": int(table["page"]) if isinstance(table.get("page"), int) else None,
                "block_content": str(table.get("block_content") or table.get("html") or ""),
                "text": self._normalize_text(table.get("raw_text") or table.get("text") or ""),
            }
            rows = table.get("rows")
            if isinstance(rows, list):
                item["rows"] = rows
            records = table.get("records")
            if isinstance(records, list):
                item["records"] = records
            bbox = self._normalize_bbox(
                table.get("block_bbox")
                or table.get("bbox")
                or table.get("box")
                or table.get("block_polygon_points")
            )
            if bbox is not None:
                item["bbox"] = bbox
            dedupe_key = (
                item.get("page"),
                tuple(item.get("bbox") or []),
                self._compact(item.get("text") or ""),
                self._compact(item.get("block_content") or ""),
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
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
