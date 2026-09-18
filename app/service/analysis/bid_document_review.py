# -*- coding: utf-8 -*-
"""
投标文件审查服务。

提供人员信息提取及跨文档人员复用分析功能，
主要用于商务标/技术标的自动化审查。
"""

from __future__ import annotations

import html
import json
import logging
import re
from collections import Counter, defaultdict
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
from app.service.manual_review.working_copy import (
    MANUAL_EXTRACTIONS_KEY,
    PERSONNEL_REUSE_CHECK_KEY,
)

logger = logging.getLogger(__name__)

# 人名 NER(LAC)单例：作为规则抽取的“补漏”，懒加载、失败回退
_PERSONNEL_NER: Any | None = None
_PERSONNEL_NER_SIGNATURE: tuple[Any, ...] | None = None
_PERSONNEL_NER_INIT_LOCK = Lock()
_PERSONNEL_NER_DISABLED = False  # 初始化失败后置位，避免反复重试拖慢主流程




class _PaddleNlpLacNer:
    """PaddleNLP LAC 人名抽取（动态图驱动，绕开 paddle 3.x 静态导出不兼容）。

    背景：paddle 3.x 把推理模型导出为 inference.json，而 paddlenlp 2.6 仍按
    inference.pdmodel 查找，导致 Taskflow 的静态预测器一律加载失败。这里：
      · monkey-patch 掉“构建静态预测器”，只保留“构建动态 BiGruCrf 模型”；
      · 推理时用动态前向替代静态 predictor，再走任务自带的后处理拿 (词, 词性)；
      · 只收 词性为 PER 的词作为人名。
    """

    def __init__(self, *, task: str, mode: str, device_id: int) -> None:
        import paddle
        from paddlenlp import Taskflow
        from paddlenlp.taskflow.named_entity_recognition import NERLACTask

        self._paddle = paddle
        # 仅构建动态模型、跳过静态预测器加载（静态文件在 paddle3 下名不对、必失败）
        original = NERLACTask._get_inference_model
        NERLACTask._get_inference_model = lambda task_self: task_self._construct_model(task_self.model)
        try:
            tf = Taskflow(
                "ner", mode="fast", entity_only=False,
                device_id=device_id, batch_size=16,
            )
        finally:
            NERLACTask._get_inference_model = original
        self._task = tf.task_instance

    def names(self, texts: list[str]) -> list[list[str]]:
        if not texts:
            return []
        task = self._task
        inputs = task._preprocess(list(texts))
        results: list[Any] = []
        lens: list[Any] = []
        for batch in inputs["data_loader"]:
            input_ids, seq_len = batch
            with self._paddle.no_grad():
                preds = task._model(input_ids, seq_len)  # 动态前向，返回 viterbi 标签id
            results.extend(preds.numpy().tolist())
            lens.extend(seq_len.numpy().tolist())
        inputs["result"] = results
        inputs["lens"] = lens
        parsed = task._postprocess(inputs)  # list[list[(词,词性)]]，单条时为 list[(词,词性)]
        if parsed and isinstance(parsed[0], tuple):
            parsed = [parsed]
        out: list[list[str]] = []
        for item in parsed or []:
            names: list[str] = []
            for unit in item or []:
                if isinstance(unit, (list, tuple)) and len(unit) >= 2 and str(unit[1]) == "PER":
                    names.append(str(unit[0]))
            out.append(names)
        while len(out) < len(texts):
            out.append([])
        return out


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
        "personnel_self_declaration": 98,
        "personnel_authorizer": 95,
        "personnel_authorized_agent": 95,
        "personnel_key_value_table": 90,
        "personnel_table": 80,
        "personnel_reverse_role": 70,
        "personnel_line": 65,
        "personnel_inline_role": 60,
        "personnel_ner": 50,
        "personnel_public_credit_table": 30,
    }
    PERSONNEL_DISPLAY_SOURCE_PRIORITY = {
        "personnel_table": 110,
        "personnel_certificate": 100,
        "personnel_self_declaration": 98,
        "personnel_authorizer": 95,
        "personnel_authorized_agent": 95,
        "personnel_key_value_table": 85,
        "personnel_reverse_role": 75,
        "personnel_line": 70,
        "personnel_inline_role": 65,
        "personnel_ner": 55,
        "personnel_public_credit_table": 20,
    }
    PUBLIC_CREDIT_PERSONNEL_HINTS = (
        "统一社会信用代码",
        "信用信息概要",
        "报告生成日期",
        "报告出具单位",
        "国家公共信用信息中心",
    )
    # 不计入项目人员的来源板块：营业执照/统一社会信用代码信息表里的法定代表人等
    # 并非项目投入人员，自动抽取阶段直接剔除，避免污染名单与跨文件重名查重。
    PERSONNEL_EXCLUDED_SOURCE_TYPES = frozenset(
        {
            "personnel_public_credit_table",
        }
    )

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
    SKIP_REASON_MISSING_CONTENT = "missing_or_unusable_ocr_content"






    @classmethod
    def _resolve_personnel_ner_device_id(cls) -> int:
        """把 PERSONNEL_NER_DEVICE 解析成 Taskflow 的 device_id（-1=CPU）。"""
        requested = str(getattr(settings, "PERSONNEL_NER_DEVICE", "auto") or "auto").strip().lower()
        if requested in ("", "auto"):
            try:
                import paddle

                match = re.fullmatch(r".+:(\d+)", str(paddle.get_device()))
                return int(match.group(1)) if match else -1
            except Exception:
                return -1
        if requested == "cpu":
            return -1
        match = re.fullmatch(r"(?:gpu|cuda|xpu|npu):(\d+)", requested)
        return int(match.group(1)) if match else -1

    @classmethod
    def _get_personnel_ner(cls) -> Any:
        """延迟初始化人名 NER(LAC)；失败置位禁用并向上抛出，由调用方回退纯规则。"""
        global _PERSONNEL_NER, _PERSONNEL_NER_SIGNATURE, _PERSONNEL_NER_DISABLED

        if _PERSONNEL_NER_DISABLED:
            raise RuntimeError("personnel NER previously failed to initialize")

        task = str(getattr(settings, "PERSONNEL_NER_TASK", "ner") or "ner")
        mode = str(getattr(settings, "PERSONNEL_NER_MODE", "fast") or "fast")
        device_id = cls._resolve_personnel_ner_device_id()
        signature = (task, mode, device_id)

        if _PERSONNEL_NER is not None and _PERSONNEL_NER_SIGNATURE == signature:
            return _PERSONNEL_NER

        with _PERSONNEL_NER_INIT_LOCK:
            if _PERSONNEL_NER is not None and _PERSONNEL_NER_SIGNATURE == signature:
                return _PERSONNEL_NER
            try:
                logger.info("Initializing personnel NER(LAC) task=%s mode=%s device_id=%s", task, mode, device_id)
                _PERSONNEL_NER = _PaddleNlpLacNer(task=task, mode=mode, device_id=device_id)
                _PERSONNEL_NER_SIGNATURE = signature
            except Exception as exc:  # pragma: no cover - 依赖模型下载/运行环境
                _PERSONNEL_NER_DISABLED = True
                raise RuntimeError(f"personnel NER init failed: {exc}") from exc
        return _PERSONNEL_NER


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
    def _document_summaries(
        documents: list[dict[str, Any]],
        *,
        include_personnel_details: bool = False,
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for item in documents:
            entries = list(item.get("personnel_entries") or [])
            summary = {
                "identifier_id": item["identifier_id"],
                "relation_id": item.get("relation_id"),
                "file_name": item.get("file_name"),
                "page_count": item.get("page_count", 0),
                "layout_section_count": item.get("layout_section_count", 0),
                "table_count": item.get("table_count", 0),
                "personnel_entry_count": len(entries),
            }
            if include_personnel_details:
                summary["names"] = sorted(
                    {
                        str(entry.get("name") or "").strip()
                        for entry in entries
                        if str(entry.get("name") or "").strip()
                    }
                )
                summary["personnel_entries"] = entries
            summaries.append(summary)
        return summaries

    # 错别字服务入口：只执行错别字检查


    # 人员复用服务入口：只执行人员复用检查
    def check_project_personnel_reuse(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
        confirmed_names: list[Any] | None = None,
    ) -> dict[str, Any]:
        requested_types, prepared_groups, skipped_groups = self._prepare_document_groups(
            document_records=document_records,
            document_types=document_types,
        )
        normalized_confirmed_names = self._normalize_confirmed_personnel_names(confirmed_names)
        confirmation_status = "pending" if normalized_confirmed_names is None else "confirmed"
        include_reuse_issues = confirmation_status == "confirmed"
        if normalized_confirmed_names is not None:
            for documents in prepared_groups.values():
                self._apply_confirmed_personnel_names(
                    documents,
                    confirmed_names=normalized_confirmed_names,
                )
        groups: dict[str, Any] = {}
        total_document_count = 0
        total_skipped_document_count = 0
        total_personnel_count = 0
        total_reused_name_count = 0
        all_prepared_documents: list[dict[str, Any]] = []

        for role in requested_types:
            prepared_documents = prepared_groups[role]
            all_prepared_documents.extend(prepared_documents)
            skipped_documents = skipped_groups[role]
            personnel_reuse_check = self._run_personnel_reuse_check(
                prepared_documents,
                include_reuse_issues=include_reuse_issues,
            )
            group_document_count = len(prepared_documents)
            group_skipped_count = len(skipped_documents)
            group_personnel_count = int(personnel_reuse_check.get("personnel_count") or 0)
            group_reused_name_count = int(personnel_reuse_check.get("reused_name_count") or 0)

            groups[role] = {
                "documents": self._document_summaries(
                    prepared_documents,
                    include_personnel_details=True,
                ),
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
                "confirmation_required": normalized_confirmed_names is None,
                "confirmation_status": confirmation_status,
                "confirmed_names": normalized_confirmed_names or [],
            },
            "groups": groups,
            "personnel_extraction": self._build_personnel_extraction_summary(
                all_prepared_documents,
                confirmation_status=confirmation_status,
            ),
            "combined_personnel_reuse_check": self._run_personnel_reuse_check(
                all_prepared_documents,
                include_reuse_issues=include_reuse_issues,
            ),
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": total_document_count,
                "skipped_document_count": total_skipped_document_count,
                "personnel_count": total_personnel_count,
                "reused_name_count": total_reused_name_count,
                "suspicious": bool(total_reused_name_count),
            },
        }

    def build_personnel_reuse_from_draft(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        documents: list[dict[str, Any]],
        confirmation_status: str,
    ) -> dict[str, Any]:
        """Build personnel reuse result directly from frontend-edited per-document draft."""
        prepared_documents = [
            self._normalize_personnel_draft_document(document)
            for document in documents
            if isinstance(document, dict)
        ]
        prepared_documents = [document for document in prepared_documents if document is not None]

        requested_types: list[str] = []
        for document in prepared_documents:
            document_type = str(document.get("document_type") or DOCUMENT_TYPE_BUSINESS_BID).strip()
            if document_type not in requested_types:
                requested_types.append(document_type)
        if not requested_types:
            requested_types = [DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID]

        groups: dict[str, Any] = {}
        all_prepared_documents: list[dict[str, Any]] = []
        total_personnel_count = 0
        for role in requested_types:
            role_documents = [
                document for document in prepared_documents
                if str(document.get("document_type") or DOCUMENT_TYPE_BUSINESS_BID).strip() == role
            ]
            all_prepared_documents.extend(role_documents)
            personnel_reuse_check = self._run_personnel_reuse_check(
                role_documents,
                include_reuse_issues=confirmation_status == "confirmed",
            )
            group_personnel_count = int(personnel_reuse_check.get("personnel_count") or 0)
            total_personnel_count += group_personnel_count
            groups[role] = {
                "documents": self._document_summaries(
                    role_documents,
                    include_personnel_details=True,
                ),
                "skipped_documents": [],
                "personnel_reuse_check": personnel_reuse_check,
                "summary": {
                    "document_count": len(role_documents),
                    "skipped_document_count": 0,
                    "personnel_count": group_personnel_count,
                    "reused_name_count": int(personnel_reuse_check.get("reused_name_count") or 0),
                    "suspicious": bool(int(personnel_reuse_check.get("reused_name_count") or 0)),
                },
            }

        combined_check = self._run_personnel_reuse_check(
            all_prepared_documents,
            include_reuse_issues=confirmation_status == "confirmed",
        )
        combined_reused_count = int(combined_check.get("reused_name_count") or 0)
        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "document_types": list(requested_types),
                "personnel_reuse_scope": "all_bid_documents",
                "confirmation_required": confirmation_status != "confirmed",
                "confirmation_status": confirmation_status,
                "draft_source": "frontend_review",
                "confirmed_names": list(combined_check.get("names") or []) if confirmation_status == "confirmed" else [],
            },
            "groups": groups,
            "personnel_extraction": self._build_personnel_extraction_summary(
                all_prepared_documents,
                confirmation_status=confirmation_status,
            ),
            "combined_personnel_reuse_check": combined_check,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": len(all_prepared_documents),
                "skipped_document_count": 0,
                "personnel_count": total_personnel_count,
                "reused_name_count": combined_reused_count,
                "suspicious": bool(combined_reused_count),
            },
        }

    def _normalize_personnel_draft_document(self, document: dict[str, Any]) -> dict[str, Any] | None:
        identifier_id = str(
            document.get("document_identifier_id") or
            document.get("identifier_id") or
            document.get("doc_id") or
            ""
        ).strip()
        file_name = str(document.get("file_name") or document.get("fileName") or identifier_id or "").strip()
        if not identifier_id and not file_name:
            return None

        document_type = str(
            document.get("document_type") or
            document.get("relation_role") or
            DOCUMENT_TYPE_BUSINESS_BID
        ).strip() or DOCUMENT_TYPE_BUSINESS_BID
        entries: list[dict[str, Any]] = []
        for entry in document.get("personnel_entries") or []:
            normalized = self._normalize_personnel_draft_entry(
                entry,
                document_identifier_id=identifier_id,
                relation_id=document.get("relation_id"),
                file_name=file_name,
                document_type=document_type,
            )
            if normalized:
                entries.append(normalized)
        entries = self._dedupe_personnel_entries_within_document(entries)

        pages = {
            int(entry["page"])
            for entry in entries
            if isinstance(entry.get("page"), int)
        }
        return {
            "identifier_id": identifier_id,
            "relation_id": document.get("relation_id"),
            "file_name": file_name,
            "document_type": document_type,
            "page_count": int(document.get("page_count") or 0),
            "layout_section_count": int(document.get("layout_section_count") or 0),
            "table_count": int(document.get("table_count") or 0),
            "sections": [],
            "tables": [],
            "personnel_entries": entries,
            "personnel_pages": pages,
        }

    def _normalize_personnel_draft_entry(
        self,
        entry: Any,
        *,
        document_identifier_id: str,
        relation_id: Any,
        file_name: str,
        document_type: str = "",
    ) -> dict[str, Any] | None:
        if not isinstance(entry, dict):
            return None
        name = self._clean_person_name(entry.get("name") or entry.get("person_name") or entry.get("personnel_name"))
        if not name:
            return None
        role = self._normalize_role(entry.get("role") or entry.get("person_role") or entry.get("position") or "待确认") or "待确认"
        page_value = entry.get("page") or entry.get("page_no") or entry.get("page_number")
        try:
            page = int(page_value) if page_value not in (None, "") else None
        except (TypeError, ValueError):
            page = None
        if page is not None and page <= 0:
            page = None
        bbox = self._normalize_bbox(entry.get("bbox") or entry.get("box"))
        text = self._normalize_text(entry.get("text") or entry.get("evidence_text") or entry.get("note") or name)
        normalized = {
            "name": name,
            "role": role,
            "page": page,
            "bbox": bbox,
            "text": text,
            "source_type": str(entry.get("source_type") or "frontend_personnel_draft"),
            "document_identifier_id": document_identifier_id,
            "document_type": str(entry.get("document_type") or document_type or "").strip(),
            "relation_id": entry.get("relation_id", relation_id),
            "file_name": entry.get("file_name") or file_name,
        }
        normalized["locations"] = [
            location for location in [
                make_location(
                    document_identifier_id=document_identifier_id,
                    file_name=normalized.get("file_name"),
                    page=page,
                    bbox=bbox,
                    text=text or name,
                )
            ] if location
        ]
        if entry.get("note"):
            normalized["note"] = str(entry.get("note") or "").strip()
        return normalized

    def _manual_personnel_entries_override(
        self,
        record: dict[str, Any],
        payload: dict[str, Any],
    ) -> tuple[bool, list[dict[str, Any]]]:
        manual_payload = self._manual_personnel_payload(record, payload)
        if manual_payload is None:
            return False, []

        raw_entries = self._manual_personnel_raw_entries(manual_payload)
        identifier_id = str(
            record.get("identifier_id")
            or record.get("document_identifier_id")
            or ""
        ).strip()
        file_name = str(record.get("file_name") or identifier_id or "").strip()
        entries: list[dict[str, Any]] = []
        for raw_entry in raw_entries:
            normalized = self._normalize_personnel_draft_entry(
                raw_entry,
                document_identifier_id=identifier_id,
                relation_id=record.get("relation_id"),
                file_name=file_name,
            )
            if normalized:
                normalized["source_type"] = str(
                    normalized.get("source_type") or "manual_personnel_review"
                )
                entries.append(normalized)
        return True, entries

    @classmethod
    def _manual_personnel_payload(
        cls,
        record: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        direct_payload = cls._as_dict(record.get("manual_personnel_reuse_check"))
        if direct_payload:
            return direct_payload

        for source in (
            payload,
            cls._as_dict(cls._as_dict(record.get("review_content")).get("effective_content")),
        ):
            manual_extractions = cls._as_dict(source.get(MANUAL_EXTRACTIONS_KEY))
            manual_payload = cls._as_dict(manual_extractions.get(PERSONNEL_REUSE_CHECK_KEY))
            if manual_payload:
                return manual_payload

        review_inputs = cls._as_dict(cls._as_dict(record.get("review_content")).get("inputs"))
        legacy_payload = cls._as_dict(review_inputs.get(PERSONNEL_REUSE_CHECK_KEY))
        return legacy_payload if legacy_payload else None

    @classmethod
    def _manual_personnel_raw_entries(cls, manual_payload: dict[str, Any]) -> list[dict[str, Any]]:
        entries = cls._dict_items(manual_payload.get("personnel_entries"))
        if entries:
            return entries

        documents = manual_payload.get("documents")
        if isinstance(documents, list):
            collected: list[dict[str, Any]] = []
            for document in documents:
                if isinstance(document, dict):
                    collected.extend(cls._dict_items(document.get("personnel_entries")))
            return collected

        document = manual_payload.get("document")
        if isinstance(document, dict):
            return cls._dict_items(document.get("personnel_entries"))

        confirmed_names = manual_payload.get("confirmed_names")
        if isinstance(confirmed_names, list):
            collected = []
            for item in confirmed_names:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("person_name") or item.get("personnel_name")
                    raw = dict(item)
                    raw["name"] = name
                    collected.append(raw)
                elif str(item or "").strip():
                    collected.append({"name": str(item).strip(), "role": "manual_confirmed"})
            return collected
        return []

    @staticmethod
    def _as_dict(value: Any) -> dict[str, Any]:
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _dict_items(value: Any) -> list[dict[str, Any]]:
        return [dict(item) for item in value or [] if isinstance(item, dict)]

    def _normalize_confirmed_personnel_names(self, values: list[Any] | None) -> list[str] | None:
        """Normalize user-confirmed personnel names; None means not confirmed yet."""
        if values is None:
            return None
        names: list[str] = []
        for value in values:
            raw_name = value.get("name") if isinstance(value, dict) else value
            name = self._clean_person_name(raw_name)
            if name and name not in names:
                names.append(name)
        return names

    def _apply_confirmed_personnel_names(
        self,
        documents: list[dict[str, Any]],
        *,
        confirmed_names: list[str],
    ) -> None:
        """Keep only confirmed names and backfill manually added names from OCR text."""
        confirmed_set = set(confirmed_names)
        for document in documents:
            existing_entries = [
                entry
                for entry in list(document.get("personnel_entries") or [])
                if str(entry.get("name") or "").strip() in confirmed_set
            ]
            existing_keys = {
                (
                    str(entry.get("name") or "").strip(),
                    entry.get("page"),
                    str(entry.get("text") or ""),
                )
                for entry in existing_entries
            }
            for name in confirmed_names:
                for entry in self._find_person_name_occurrences(document, name):
                    key = (str(entry.get("name") or "").strip(), entry.get("page"), str(entry.get("text") or ""))
                    if key in existing_keys:
                        continue
                    existing_entries.append(entry)
                    existing_keys.add(key)
            document["personnel_entries"] = existing_entries
            document["personnel_pages"] = {
                int(entry["page"])
                for entry in existing_entries
                if isinstance(entry.get("page"), int)
            }

    def _find_person_name_occurrences(self, document: dict[str, Any], name: str) -> list[dict[str, Any]]:
        """Find manually confirmed names in document sections and tables."""
        entries: list[dict[str, Any]] = []
        if not name:
            return entries
        sources = list(document.get("sections") or []) + list(document.get("tables") or [])
        for source in sources:
            text = self._normalize_text(source.get("text") or source.get("raw_text") or source.get("block_content"))
            if not text or name not in text:
                continue
            entries.append(
                {
                    "name": name,
                    "role": "待确认",
                    "page": source.get("page") if isinstance(source.get("page"), int) else None,
                    "bbox": self._normalize_bbox(source.get("bbox")),
                    "text": text[:240],
                    "source_type": "personnel_confirmed_name_search",
                    "document_identifier_id": document["identifier_id"],
                    "relation_id": document.get("relation_id"),
                    "file_name": document.get("file_name"),
                    "locations": [
                        location for location in [
                            make_location(
                                document_identifier_id=document["identifier_id"],
                                file_name=document.get("file_name"),
                                page=source.get("page"),
                                bbox=source.get("bbox"),
                                text=name,
                            )
                        ] if location
                    ],
                }
            )
        return entries

    def _build_personnel_extraction_summary(
        self,
        documents: list[dict[str, Any]],
        *,
        confirmation_status: str,
    ) -> dict[str, Any]:
        """List all extracted/confirmed names for frontend confirmation."""
        entries: list[dict[str, Any]] = []
        for document in documents:
            entries.extend(list(document.get("personnel_entries") or []))
        names = sorted(
            {
                str(entry.get("name") or "").strip()
                for entry in entries
                if str(entry.get("name") or "").strip()
            }
        )
        return {
            "confirmation_status": confirmation_status,
            "name_count": len(names),
            "names": names,
            "personnel_entries": entries,
            "documents": self._document_summaries(
                documents,
                include_personnel_details=True,
            ),
        }

    # 文档预处理：提取人员信息、统计基础信息
    def _prepare_document(
        self,
        record: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        payload = self._coerce_payload(record.get("content"))
        sections = self._sections(payload)
        tables = self._native_tables(payload)
        manual_personnel_exists, manual_personnel_entries = self._manual_personnel_entries_override(
            record,
            payload,
        )
        if not sections and not tables and not manual_personnel_exists:
            return None, self.SKIP_REASON_MISSING_CONTENT

        personnel_section_pages = self._detect_personnel_section_pages(sections)
        personnel_entries = (
            manual_personnel_entries
            if manual_personnel_exists
            else self._extract_personnel_entries(
                record=record,
                sections=sections,
                tables=tables,
            )
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
            "document_type": str(
                record.get("document_type")
                or record.get("relation_role")
                or ""
            ).strip(),
            "page_count": self._page_count(sections, tables),
            "layout_section_count": len(sections),
            "table_count": len(tables),
            "sections": sections,
            "tables": tables,
            "personnel_entries": personnel_entries,
            "personnel_pages": personnel_pages,
        }, None


    # 人员复用分析：聚合同名人员信息
    def _run_personnel_reuse_check(
        self,
        documents: list[dict[str, Any]],
        *,
        include_reuse_issues: bool = True,
    ) -> dict[str, Any]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        total_personnel_count = 0
        document_summaries: list[dict[str, Any]] = []
        all_entries: list[dict[str, Any]] = []

        for document in documents:
            entries = list(document.get("personnel_entries") or [])
            total_personnel_count += len(entries)
            all_entries.extend(entries)
            names = sorted(
                {
                    str(entry.get("name") or "").strip()
                    for entry in entries
                    if str(entry.get("name") or "").strip()
                }
            )
            document_summaries.append(
                {
                    "identifier_id": document["identifier_id"],
                    "relation_id": document.get("relation_id"),
                    "file_name": document.get("file_name"),
                    "document_type": document.get("document_type"),
                    "personnel_count": len(entries),
                    "names": names,
                    "personnel_entries": entries,
                }
            )
            for entry in entries:
                name = str(entry.get("name") or "").strip()
                if not name:
                    continue
                grouped[name].append(entry)

        reused_names: list[dict[str, Any]] = []
        for name, items in grouped.items():
            if not include_reuse_issues:
                continue
            bidder_keys = {
                self._personnel_reuse_bidder_key(item)
                for item in items
                if self._personnel_reuse_bidder_key(item)
            }
            if len(bidder_keys) < 2:
                continue
            document_ids = {
                str(item.get("document_identifier_id") or "").strip()
                for item in items
                if str(item.get("document_identifier_id") or "").strip()
            }

            roles = sorted(
                {
                    str(item.get("role") or "").strip()
                    for item in items
                    if str(item.get("role") or "").strip()
                }
            )
            pages = sorted(
                {
                    int(item["page"])
                    for item in items
                    if isinstance(item.get("page"), int)
                }
            )
            pages_by_file: dict[str, list[int]] = {}
            for item in items:
                file_name = str(item.get("file_name") or item.get("document_identifier_id") or "").strip()
                page = item.get("page")
                if not file_name or not isinstance(page, int):
                    continue
                pages_by_file.setdefault(file_name, [])
                if page not in pages_by_file[file_name]:
                    pages_by_file[file_name].append(page)
            for file_pages in pages_by_file.values():
                file_pages.sort()
            reused_names.append(
                {
                    "name": name,
                    "document_count": len(document_ids),
                    "bidder_count": len(bidder_keys),
                    "bidder_keys": sorted(bidder_keys),
                    "occurrence_count": len(items),
                    "roles": roles,
                    "pages": pages,
                    "pages_by_file": dict(sorted(pages_by_file.items())),
                    "risk_level": self._personnel_reuse_risk_level(roles, len(bidder_keys)),
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
            "names": sorted(
                {
                    str(entry.get("name") or "").strip()
                    for entry in all_entries
                    if str(entry.get("name") or "").strip()
                }
            ),
            "personnel_entries": all_entries,
            "documents": document_summaries,
            "issues": reused_names,
            "notes": [
                "同名人员跨不同技术标重复出现时标记为疑似一人多用，建议结合原文页码与框选位置人工复核。",
            ],
        }

    def _personnel_reuse_bidder_key(self, item: dict[str, Any]) -> str:
        relation_id = item.get("relation_id")
        if relation_id not in (None, ""):
            return f"relation:{relation_id}"
        for key in ("bidder_key", "bidder_name", "company_name"):
            text = str(item.get(key) or "").strip()
            if text:
                return f"{key}:{text}"
        document_id = str(item.get("document_identifier_id") or "").strip()
        if document_id:
            return f"document:{document_id}"
        file_name = str(item.get("file_name") or "").strip()
        return f"file:{file_name}" if file_name else ""















    def _is_cjk_text(self, value: str) -> bool:
        text = str(value or "")
        return bool(text and all("\u4e00" <= char <= "\u9fff" for char in text))


    # 遍历文档中可用于错别字检查的句子









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

        # NER(LAC)补漏：把规则没命中的人名从“人员相关段落(目标页)”里补出来；
        # 任何异常都吞掉、回退纯规则，绝不影响主流程。
        try:
            entries.extend(self._extract_personnel_names_via_ner(record, sections))
        except Exception as exc:  # pragma: no cover - 模型/环境相关
            logger.warning("人名 NER 抽取异常，已跳过(回退纯规则)：%s", exc)

        # 在去重前剔除营业执照/信用信息表等非项目人员来源：只来自这些板块的人名会被
        # 完全移除，同时出现在正式人员表的人名因正式来源仍在而保留。
        entries = [
            entry
            for entry in entries
            if str(entry.get("source_type") or "")
            not in self.PERSONNEL_EXCLUDED_SOURCE_TYPES
        ]

        return self._dedupe_personnel_entries_within_document(entries)

    def _extract_personnel_names_via_ner(
        self,
        record: dict[str, Any],
        sections: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """在“人员相关段落(目标页)”上用 LAC 抽人名，作为规则的补漏。

        只抽人名、不判角色(角色置“待确认”)；输出 {人名, 页码} 形式的人员条目，
        交给既有的去重与跨文档复用比对。为降噪与控成本：
          · 只在含人员上下文/人员章节的段落上跑；
          · 跳过营业执照/公共信用信息表区块(避免抽到非项目法人)；
          · 复用 _clean_person_name 过滤占位/非人名。
        """
        if not bool(getattr(settings, "PERSONNEL_NER_ENABLED", False)):
            return []

        targets: list[dict[str, Any]] = []
        for section in sections:
            text = str(section.get("text") or "").strip()
            if not text:
                continue
            compact = self._compact(self._normalize_personnel_evidence_text(text))
            if not compact:
                continue
            if any(hint in compact for hint in self.PUBLIC_CREDIT_PERSONNEL_HINTS):
                continue  # 营业执照/公共信用表：非项目人员，跳过
            title = str(section.get("title") or "")
            is_target = any(hint in compact for hint in self.PERSONNEL_CONTEXT_HINTS) or any(
                hint in title for hint in self.PERSONNEL_SECTION_HINTS
            )
            if is_target:
                targets.append(section)
        if not targets:
            return []

        engine = self._get_personnel_ner()  # 失败抛出，由上层 try 回退
        max_chars = int(getattr(settings, "PERSONNEL_NER_MAX_CHARS", 1500) or 1500)
        batch_size = max(1, int(getattr(settings, "PERSONNEL_NER_BATCH_SIZE", 16) or 16))

        entries: list[dict[str, Any]] = []
        for start in range(0, len(targets), batch_size):
            chunk = targets[start:start + batch_size]
            texts = [
                self._normalize_personnel_evidence_text(s.get("text"))[:max_chars]
                for s in chunk
            ]
            try:
                names_per_text = engine.names(texts)
            except Exception as exc:  # pragma: no cover - 推理相关
                logger.warning("人名 NER 推理失败，跳过该批：%s", exc)
                continue
            for section, raw_names in zip(chunk, names_per_text):
                seen: set[str] = set()
                for raw in raw_names:
                    name = self._clean_person_name(raw)
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    entries.append(
                        self._build_personnel_entry(
                            record=record,
                            name=name,
                            role="待确认",
                            page=section.get("page"),
                            bbox=section.get("bbox"),
                            evidence_text=self._normalize_personnel_evidence_text(
                                section.get("text")
                            )[:200],
                            source_type="personnel_ner",
                        )
                    )
        return entries

    def _dedupe_personnel_entries_within_document(
        self,
        entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """同一 PDF 内同名人员仅保留一条；不同 PDF 的同名人员不在抽取阶段合并。"""
        selected: dict[tuple[Any, ...], dict[str, Any]] = {}
        for entry in entries:
            document_key = (
                entry.get("document_identifier_id")
                or entry.get("file_name")
                or ""
            )
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            key = (
                document_key,
                name,
            )
            existing = selected.get(key)
            entry_priority = self._personnel_entry_display_priority(entry)
            existing_priority = (
                self._personnel_entry_display_priority(existing)
                if existing is not None
                else -1
            )
            if existing is None or entry_priority > existing_priority:
                selected[key] = entry
            elif existing is not None and entry_priority == existing_priority:
                existing_role = str(existing.get("role") or "").strip()
                entry_role = str(entry.get("role") or "").strip()
                if existing_role in ("", "待确认") and entry_role not in ("", "待确认"):
                    selected[key] = entry
                    continue
                existing_page = int(existing.get("page") or 10**9)
                entry_page = int(entry.get("page") or 10**9)
                if entry_page < existing_page:
                    selected[key] = entry

        deduped = list(selected.values())

        deduped.sort(
            key=lambda item: (
                -self._personnel_entry_display_priority(item),
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
            r"我\s*(?:[（(]\s*姓名\s*[）)]\s*)?(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})\s*(?:[（(]\s*姓名\s*[）)]\s*)?\s*(?:系|为)[^。\n]{0,120}?法定代表人",
            evidence_text,
        ):
            self._append_personnel_match_entry(
                entries=entries,
                record=record,
                section=section,
                name=match.group("name"),
                role="法定代表人",
                evidence_text=evidence_text,
                source_type="personnel_self_declaration",
            )

        for match in re.finditer(
            r"现授权委托(?:本单位在职职工)?\s*(?P<name>[A-Za-z\u4e00-\u9fa5]{2,20})\s*(?:[（(]\s*姓名\s*[）)]\s*)?\s*(?:为|作为)全权代表",
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
                source_type = self._personnel_key_value_source_type(row, evidence_text)
                entries.append(
                    self._build_personnel_entry(
                        record=record,
                        name=name,
                        role=role,
                        page=table.get("page"),
                        bbox=table.get("bbox"),
                        evidence_text=evidence_text,
                        source_type=source_type,
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
            "document_type": str(record.get("document_type") or record.get("relation_role") or "").strip(),
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


    def _personnel_entry_display_priority(self, entry: dict[str, Any]) -> int:
        source_type = str(entry.get("source_type") or "").strip()
        return int(self.PERSONNEL_DISPLAY_SOURCE_PRIORITY.get(source_type, 0))

    def _personnel_key_value_source_type(self, row: list[str], evidence_text: str) -> str:
        compact = self._compact(evidence_text or " ".join(str(cell or "") for cell in row))
        if compact and any(token in compact for token in self.PUBLIC_CREDIT_PERSONNEL_HINTS):
            return "personnel_public_credit_table"
        return "personnel_key_value_table"

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
            "银行",
            "分行",
            "开户银行",
            "其他内容",
            "其它内容",
            "其他资质情况",
            "基础信息",
            "对外海报",
            "工作年限",
            "或授权",
            "所学专业",
            "税号",
            "时间",
            "年月日",
            "公章",
            "用章",
            "联系",
            "联系人",
            "联系电话",
            "联系电",
            "销售",
            "无偏离",
            "代表人",
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
