# -*- coding: utf-8 -*-
"""
文档查重服务模块。

对投标文件（商务标/技术标）执行内容查重分析，包括精确匹配、基于句子/段落/表格的相似度检测、
图片重复识别，并支持从分项报价和偏离表中提取查重范围，排除招标文件模板内容。
"""

from __future__ import annotations

from difflib import SequenceMatcher
import hashlib
import html
import io
import json
import re
from html.parser import HTMLParser
from itertools import combinations
from typing import Any

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID
from app.service.minio_service import MinioService

from .deviation import DeviationChecker
from .itemized_pricing import ItemizedPricingChecker
from .template_extractor import SectionClassifier


class _TableHTMLParser(HTMLParser):
    """简易 HTML 表格解析器，用于提取表格的行文本。"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized == "tr":
            self._current_row = []
            return
        if normalized in {"td", "th"} and self._current_row is not None:
            self._current_cell_parts = []
            return
        if normalized == "br" and self._current_cell_parts is not None:
            self._current_cell_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"td", "th"} and self._current_row is not None and self._current_cell_parts is not None:
            text = "".join(self._current_cell_parts)
            text = re.sub(r"\s+", " ", html.unescape(text)).strip()
            self._current_row.append(text)
            self._current_cell_parts = None
            return
        if normalized == "tr" and self._current_row is not None:
            if any(cell for cell in self._current_row):
                self.rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        if self._current_cell_parts is not None:
            self._current_cell_parts.append(data)


class DuplicateCheckService:
    """文档查重服务，支持精确匹配和基于相似度的内容分析。"""

    # 支持的文档类型
    SUPPORTED_DOCUMENT_TYPES = (DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID)

    # 各种预编译的正则和常量
    PAGE_NUMBER_PATTERN = re.compile(r"^\d+$")
    SPLIT_LINE_PATTERN = re.compile(r"[\r\n]+")
    SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[。！？!?；;])|(?<=\.)(?=\s|$)")

    # 文档被跳过的原因标识
    BUSINESS_SCOPE_SKIP_REASON = "missing_business_duplicate_scope_content"
    TEMPLATE_EXCLUDED_SKIP_REASON = "content_fully_covered_by_tender_template"

    # 文本处理阈值
    MIN_SENTENCE_COMPACT_LENGTH = 10           # 最小句子紧凑长度
    BUSINESS_SIMILARITY_MIN_KEY_LENGTH = 8     # 相似度匹配的最小键长度

    # 相似度阈值
    BUSINESS_BLOCK_SIMILARITY_THRESHOLD = 0.78
    BUSINESS_SECTION_SIMILARITY_THRESHOLD = 0.72
    BUSINESS_TABLE_SIMILARITY_THRESHOLD = 0.72

    # 查重范围中需要过滤的常见表头关键词
    COMMON_DUPLICATE_HEADER_TOKENS = (
        "序号", "项目名称", "招标编号", "项目编号", "招标文件", "采购文件",
        "投标文件", "响应文件", "采购规格", "响应规格", "偏离说明",
        "商务条款", "技术条款", "分项名称", "分项说明", "单价", "合计",
        "备注", "对应投标文件所在页",
    )
    # 招标文件中的固定要求模板词，不应作为查重内容
    COMMON_DUPLICATE_REQUIREMENT_TOKENS = (
        "提供复印件", "项目管理经验", "相关领域", "工程师认证证书",
        "认证证书", "毕业时间为准", "投标人送交", "第三方进行计量",
        "提供证书", "招标人提供", "培训相关费用", "合同总价",
        "正式验收", "现场初验收", "试运行及终验",
    )
    # 偏离响应中表示具体应答的关键词
    DEVIATION_RESPONSE_TOKENS = (
        "我方", "我公司", "响应", "偏离", "详见", "技术文件",
        "商务文件", "技术分册", "商务分册", "技术册", "商务册",
    )
    # 模板化行首的正则模式
    COMMON_DUPLICATE_TEMPLATE_PATTERNS = (
        re.compile(r"^(?:项目名称|项目编号|招标编号|采购编号|招标人|采购人|投标人|供应商)\s*[:：_]"),
        re.compile(r"^(?:GB|GJB|ISO|IEC|YD/T|SJ/T)[A-Z0-9./ -]*[;；。]?$", re.IGNORECASE),
    )

    def __init__(self) -> None:
        # 依赖的其他检查器用于提取范围
        self._itemized_checker = ItemizedPricingChecker()
        self._deviation_checker = DeviationChecker()
        # MinIO 服务延迟初始化
        self._minio_service: MinioService | None = None
        # 图片 hash 缓存，避免重复从 MinIO 下载
        self._document_image_cache: dict[str, list[dict[str, Any]]] = {}

    # ── 公共入口 ─────────────────────────────────

    def check_project_documents(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
        max_evidence_sections: int = 5,
        max_pairs_per_type: int = 0,
    ) -> dict[str, Any]:
        """主入口：对项目中的文档分组进行两两比较，返回查重分析结果。"""
        requested_types = self._normalize_requested_types(document_types)
        prepared_groups: dict[str, list[dict[str, Any]]] = {item: [] for item in requested_types}
        skipped_groups: dict[str, list[dict[str, Any]]] = {item: [] for item in requested_types}
        template_cache: dict[tuple[str, str], dict[str, Any] | None] = {}

        dedupe_keys: set[tuple[str, str]] = set()
        for record in document_records:
            role = self._normalize_document_role(
                record.get("relation_role") or record.get("document_type")
            )
            if role not in requested_types:
                continue

            identifier_id = str(record.get("identifier_id") or "").strip()
            if not identifier_id:
                skipped_groups[role].append(
                    {
                        "relation_id": record.get("relation_id"),
                        "file_name": record.get("file_name"),
                        "reason": "missing_identifier_id",
                    }
                )
                continue

            dedupe_key = (role, identifier_id)
            if dedupe_key in dedupe_keys:
                continue
            dedupe_keys.add(dedupe_key)

            # 准备模板上下文，用于排除招标文件中相同的内容
            template_context = self._get_tender_template_context(
                record, role=role, cache=template_cache,
            )
            prepared, skip_reason = self._prepare_document(
                record, role=role, template_context=template_context,
            )
            if prepared is None:
                skipped_groups[role].append(
                    {
                        "identifier_id": identifier_id,
                        "relation_id": record.get("relation_id"),
                        "file_name": record.get("file_name"),
                        "reason": skip_reason or "missing_or_unusable_ocr_content",
                    }
                )
                continue
            prepared["document_type"] = role
            prepared_groups[role].append(prepared)

        groups: dict[str, Any] = {}
        suspicious_pair_count = 0
        high_risk_pair_count = 0
        medium_risk_pair_count = 0

        for role in requested_types:
            documents = prepared_groups[role]
            # 对所有文档进行两两组合并比较
            pair_items = [
                self._compare_documents(left, right, role=role, max_evidence_sections=max_evidence_sections)
                for left, right in combinations(documents, 2)
            ]
            pair_items.sort(key=self._pair_sort_key, reverse=True)

            total_pair_count = len(pair_items)
            if max_pairs_per_type > 0:
                pair_items = pair_items[:max_pairs_per_type]

            suspicious = [
                item for item in pair_items if str(item.get("risk_level") or "none") != "none"
            ]
            high = [item for item in pair_items if item.get("risk_level") == "high"]
            medium = [item for item in pair_items if item.get("risk_level") == "medium"]

            suspicious_pair_count += len(suspicious)
            high_risk_pair_count += len(high)
            medium_risk_pair_count += len(medium)

            groups[role] = {
                "document_count": len(documents),
                "pair_count": total_pair_count,
                "reported_pair_count": len(pair_items),
                "suspicious_pair_count": len(suspicious),
                "high_risk_pair_count": len(high),
                "medium_risk_pair_count": len(medium),
                "documents": [
                    {
                        "identifier_id": item["identifier_id"],
                        "relation_id": item.get("relation_id"),
                        "file_name": item.get("file_name"),
                        "section_count": item.get("section_count", 0),
                        "block_count": item.get("block_count", 0),
                        "table_count": item.get("table_count", 0),
                        "image_count": item.get("image_count", 0),
                    }
                    for item in documents
                ],
                "skipped_documents": skipped_groups[role],
                "items": pair_items,
            }

        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "detection_mode": "exact_plus_similarity",
                "document_types": list(requested_types),
                "max_evidence_sections": int(max_evidence_sections),
                "max_pairs_per_type": int(max_pairs_per_type),
                "template_exclusion_enabled": True,
                "template_exclusion_source": "tender_document",
                "block_matching_unit": "sentence",
                "business_similarity_enabled": True,
            },
            "groups": groups,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": sum(groups[item]["document_count"] for item in groups),
                "pair_count": sum(groups[item]["pair_count"] for item in groups),
                "reported_pair_count": sum(groups[item]["reported_pair_count"] for item in groups),
                "suspicious_pair_count": suspicious_pair_count,
                "high_risk_pair_count": high_risk_pair_count,
                "medium_risk_pair_count": medium_risk_pair_count,
            },
        }

    # ── 文档预处理 ───────────────────────────────

    def _prepare_document(
        self,
        record: dict[str, Any],
        *,
        role: str,
        template_context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """对单条文档记录提取内容，排除模板，返回可用于比较的结构化对象。"""
        payload = self._coerce_payload(record.get("content"))
        ordered_blocks, table_entries, empty_reason = self._extract_document_content(payload, role=role)
        if not ordered_blocks:
            # 即使没有区块，仍尝试仅用表格构建文档，供表格重复检测
            prepared = self._build_prepared_document(record, [], table_entries, role=role)
            if prepared is not None:
                return prepared, None
            return None, empty_reason

        ordered_blocks, table_entries = self._exclude_template_content(
            ordered_blocks, table_entries, template_context=template_context,
        )
        if not ordered_blocks:
            prepared = self._build_prepared_document(record, [], table_entries, role=role)
            if prepared is not None:
                return prepared, None
            return None, self.TEMPLATE_EXCLUDED_SKIP_REASON

        prepared = self._build_prepared_document(record, ordered_blocks, table_entries, role=role)
        return prepared, None if prepared is not None else empty_reason

    def _extract_document_content(
        self,
        payload: dict[str, Any],
        *,
        role: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        """按文档角色提取可供查重的区块和表格，返回 (blocks, tables, skip_reason)。"""
        if role == DOCUMENT_TYPE_BUSINESS_BID:
            # 商务标需通过分项报价和偏离表提取查重范围
            scoped_segments = self._extract_business_duplicate_segments(payload)
            if not scoped_segments:
                return [], [], self.BUSINESS_SCOPE_SKIP_REASON
            ordered_blocks, table_entries = self._build_scoped_blocks_and_tables(scoped_segments)
            return ordered_blocks, table_entries, self.BUSINESS_SCOPE_SKIP_REASON

        container = self._container(payload)
        ordered_blocks, table_entries = self._extract_ordered_blocks(container)

        if not ordered_blocks:
            # 回退到纯文本全文
            fallback_text = self._fallback_text(container)
            if fallback_text:
                exact_key = self._compact_raw_text(fallback_text)
                ordered_blocks = [
                    {
                        "type": "text",
                        "page": 1,
                        "text": fallback_text,
                        "exact_key": exact_key,
                        "exact_hash": self._hash_text(exact_key),
                    }
                ]

        return ordered_blocks, table_entries, "missing_or_unusable_ocr_content"

    def _get_tender_template_context(
        self,
        record: dict[str, Any],
        *,
        role: str,
        cache: dict[tuple[str, str], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """获取招标文件的模板内容（用于排除投标文件中相同的部分）。"""
        tender_identifier = str(record.get("tender_identifier_id") or "").strip()
        if not tender_identifier:
            return None

        cache_key = (role, tender_identifier)
        if cache_key in cache:
            return cache[cache_key]

        tender_payload = self._coerce_payload(record.get("tender_content"))
        ordered_blocks, table_entries, _ = self._extract_document_content(tender_payload, role=role)
        if not ordered_blocks:
            cache[cache_key] = None
            return None

        cache[cache_key] = {
            "tender_identifier_id": tender_identifier,
            "block_hashes": {
                str(block.get("exact_hash") or "")
                for block in ordered_blocks
                if str(block.get("exact_hash") or "")
            },
            "table_hashes": {
                str(table.get("exact_hash") or "")
                for table in table_entries
                if str(table.get("exact_hash") or "")
            },
            "placeholder_patterns": self._build_template_placeholder_patterns(ordered_blocks),
        }
        return cache[cache_key]

    # ── 模板排除逻辑 ─────────────────────────────

    def _exclude_template_content(
        self,
        ordered_blocks: list[dict[str, Any]],
        table_entries: list[dict[str, Any]],
        *,
        template_context: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """根据招标模板上下文移除重复的固定文案区块和表格。"""
        if template_context is None:
            return ordered_blocks, table_entries

        block_hashes = set(template_context.get("block_hashes") or [])
        table_hashes = set(template_context.get("table_hashes") or [])
        placeholder_patterns = list(template_context.get("placeholder_patterns") or [])

        filtered_blocks = [
            block
            for block in ordered_blocks
            if str(block.get("exact_hash") or "") not in block_hashes
            and not self._matches_template_placeholder(block, placeholder_patterns)
        ]
        filtered_tables = [
            table
            for table in table_entries
            if str(table.get("exact_hash") or "") not in table_hashes
        ]
        return filtered_blocks, filtered_tables

    def _build_template_placeholder_patterns(
        self,
        blocks: list[dict[str, Any]],
    ) -> list[re.Pattern[str]]:
        """从招标文本中生成带占位符的正则模式，用于识别模板化语句。"""
        patterns: list[re.Pattern[str]] = []
        seen = set()
        for block in blocks:
            pattern = self._template_placeholder_pattern(str(block.get("text") or ""))
            if pattern is None:
                continue
            key = pattern.pattern
            if key in seen:
                continue
            seen.add(key)
            patterns.append(pattern)
        return patterns

    def _template_placeholder_pattern(self, text: str) -> re.Pattern[str] | None:
        """为包含下划线、省略号、格式占位符的文本生成正则模式。"""
        normalized = self._normalize_plain_text(text)
        changed = False
        format_tokens = re.findall(r"[（(][^）)]{0,80}格式[^）)]*[）)]", normalized)
        pattern_source = normalized
        for index, token in enumerate(format_tokens):
            pattern_source = pattern_source.replace(token, f"__FMT_TOKEN_{index}__", 1)
        escaped = re.escape(pattern_source)

        if re.search(r"_{2,}|…{2,}|\.{3,}", normalized):
            escaped = re.sub(r"_{2,}", ".+?", escaped)
            escaped = re.sub(r"…{2,}", ".+?", escaped)
            escaped = re.sub(r"(?:\\\.){3,}", ".+?", escaped)
            changed = True

        for index, token in enumerate(format_tokens):
            escaped = escaped.replace(re.escape(f"__FMT_TOKEN_{index}__"), rf"(?:{re.escape(token)})?")
            changed = True

        if not changed:
            return None

        escaped = escaped.replace(r"\ ", r"\s*")
        return re.compile(rf"^{escaped}$", re.IGNORECASE)

    def _matches_template_placeholder(
        self,
        block: dict[str, Any],
        patterns: list[re.Pattern[str]],
    ) -> bool:
        """判断文本块是否完全匹配模板占位符模式。"""
        if not patterns:
            return False
        text = self._normalize_plain_text(block.get("text") or "")
        return any(pattern.fullmatch(text) for pattern in patterns)

    # ── 文档结构化构建 ───────────────────────────

    def _build_prepared_document(
        self,
        record: dict[str, Any],
        ordered_blocks: list[dict[str, Any]],
        table_entries: list[dict[str, Any]],
        *,
        role: str,
    ) -> dict[str, Any] | None:
        """构建可供比较的内部文档结构，包含区块、区段、表格、图片等映射。"""
        sections = self._build_sections(ordered_blocks)
        full_text = "\n".join(block["text"] for block in ordered_blocks if block.get("text"))
        exact_key = self._compact_raw_text(full_text)
        image_entries = self._extract_document_images(record, role=role)
        exact_block_map = self._build_sentence_unit_map(ordered_blocks)
        exact_section_map = {
            section["exact_hash"]: section
            for section in sections
            if len(section["exact_key"]) >= 8
        }
        exact_table_map = {table["exact_hash"]: table for table in table_entries}
        exact_image_map = {
            str(item.get("exact_hash") or ""): item
            for item in image_entries
            if str(item.get("exact_hash") or "")
        }

        if len(exact_key) < 16 and not exact_table_map and not exact_image_map:
            return None

        exact_hash_source = exact_key or json.dumps(
            {
                "table_hashes": sorted(exact_table_map.keys()),
                "image_hashes": sorted(exact_image_map.keys()),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

        return {
            "identifier_id": str(record.get("identifier_id") or ""),
            "relation_id": record.get("relation_id"),
            "file_name": str(record.get("file_name") or ""),
            "full_text": full_text,
            "exact_key": exact_key,
            "exact_hash": self._hash_text(exact_hash_source),
            "blocks": ordered_blocks,
            "exact_block_hashes": set(exact_block_map.keys()),
            "exact_block_map": exact_block_map,
            "sections": sections,
            "section_count": len(sections),
            "exact_section_hashes": set(exact_section_map.keys()),
            "exact_section_map": exact_section_map,
            "block_count": len(ordered_blocks),
            "tables": table_entries,
            "table_count": len(table_entries),
            "exact_table_hashes": set(exact_table_map.keys()),
            "exact_table_map": exact_table_map,
            "images": image_entries,
            "image_count": len(image_entries),
            "exact_image_hashes": set(exact_image_map.keys()),
            "exact_image_map": exact_image_map,
        }

    def _build_sentence_unit_map(
        self,
        ordered_blocks: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """将区块划分为句子并建立精确哈希映射。"""
        sentence_map: dict[str, dict[str, Any]] = {}
        for block in ordered_blocks:
            if block.get("type") == "heading":
                continue
            for sentence in self._sentence_units_from_block(block):
                sentence_map.setdefault(sentence["exact_hash"], sentence)
        return sentence_map

    def _sentence_units_from_block(self, block: dict[str, Any]) -> list[dict[str, Any]]:
        """从文本块中拆分句子并计算哈希。"""
        text = self._normalize_plain_text(block.get("text") or "")
        if not text:
            return []

        items: list[dict[str, Any]] = []
        for sentence_text in self._split_sentences(text):
            exact_key = self._compact_raw_text(sentence_text)
            if len(exact_key) < self.MIN_SENTENCE_COMPACT_LENGTH:
                continue
            items.append(
                {
                    "page": block.get("page"),
                    "type": block.get("type"),
                    "text": sentence_text,
                    "exact_key": exact_key,
                    "exact_hash": self._hash_text(exact_key),
                }
            )
        return items

    def _split_sentences(self, text: str) -> list[str]:
        """按句子边界分割文本并去重。"""
        normalized = self._normalize_plain_text(text)
        if not normalized:
            return []

        sentences: list[str] = []
        for line in self.SPLIT_LINE_PATTERN.split(normalized):
            line = line.strip()
            if not line:
                continue

            parts = self.SENTENCE_BOUNDARY_PATTERN.split(line)
            buffer = ""
            for part in parts:
                fragment = str(part or "").strip()
                if not fragment:
                    continue
                buffer = f"{buffer}{fragment}".strip()
                if self.SENTENCE_BOUNDARY_PATTERN.search(fragment):
                    sentences.append(buffer)
                    buffer = ""
            if buffer:
                sentences.append(buffer)

        deduped: list[str] = []
        seen = set()
        for sentence in sentences:
            normalized_sentence = self._normalize_plain_text(sentence)
            if not normalized_sentence:
                continue
            key = self._compact_raw_text(normalized_sentence)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(normalized_sentence)
        return deduped

    # ── 商务标查重范围提取 ───────────────────────

    def _extract_business_duplicate_segments(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """从商务标中提取分项报价和偏离表相关段落作为查重范围。"""
        segments: list[dict[str, Any]] = []

        # 分项报价部分
        itemized_document = self._itemized_checker._prepare_document(payload)
        for section in itemized_document.get("item_sections") or []:
            segment = self._segment_from_itemized_section(section)
            if segment is not None:
                segments.append(segment)

        # 偏离表部分
        deviation_payload = self._deviation_checker._coerce_payload(payload)
        deviation_sections = self._deviation_checker._extract_bid_deviation_sections(deviation_payload)
        row_segments = self._segments_from_deviation_rows(deviation_sections)
        for segment in row_segments:
            segments.append(segment)

        # 补充未被行覆盖的偏离表章节
        covered_page_keys = {
            tuple(int(page) for page in (segment.get("pages") or []) if isinstance(page, int))
            for segment in row_segments
        }
        for section in (deviation_sections.get("business") or []) + (deviation_sections.get("technical") or []):
            section_pages = tuple(
                int(page)
                for page in ([section.get("page")] if isinstance(section.get("page"), int) else [])
                if isinstance(page, int)
            )
            if section_pages and section_pages in covered_page_keys:
                continue
            segment = self._segment_from_deviation_section(section)
            if segment is not None:
                segments.append(segment)

        deduped = self._dedupe_scoped_segments(segments)
        deduped.sort(key=self._scoped_segment_sort_key)
        return deduped

    def _segments_from_deviation_rows(
        self,
        deviation_sections: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从已解析的偏离行中构建查重段落。"""
        section_pages = {
            int(section.get("page"))
            for section in (deviation_sections.get("business") or []) + (deviation_sections.get("technical") or [])
            if isinstance(section, dict) and isinstance(section.get("page"), int)
        }
        grouped: dict[tuple[str, int], list[str]] = {}

        for row in deviation_sections.get("rows") or []:
            if not isinstance(row, dict):
                continue
            page = row.get("page")
            if not isinstance(page, int):
                continue
            title = str(row.get("title") or "").strip() or "偏离表"
            if "偏离" not in title and page not in section_pages:
                continue

            requirement = self._normalize_plain_text(row.get("requirement_text") or "")
            response = self._normalize_plain_text(row.get("response_text") or "")
            deviation = self._normalize_plain_text(row.get("deviation_text") or "")
            if not self._is_deviation_duplicate_row(requirement, response, deviation):
                continue
            joined = " | ".join(part for part in (requirement, response, deviation) if part).strip()
            if len(self._compact_raw_text(joined)) < 6:
                continue

            grouped.setdefault((title, page), []).append(joined)

        segments: list[dict[str, Any]] = []
        for (title, page), lines in grouped.items():
            deduped_lines: list[str] = []
            seen = set()
            for line in lines:
                key = self._compact_raw_text(line)
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped_lines.append(line)
            if not deduped_lines:
                continue
            segments.append(
                {
                    "title": title,
                    "pages": [page],
                    "kind": "table",
                    "source": "deviation_table",
                    "preserve_common_lines": True,
                    "lines": deduped_lines,
                }
            )
        return segments

    def _is_deviation_duplicate_row(
        self,
        requirement: str,
        response: str,
        deviation: str,
    ) -> bool:
        """判断偏离行是否具有重复检查意义。"""
        compact_requirement = self._compact_raw_text(requirement)
        compact_response = self._compact_raw_text(response)
        compact_deviation = self._compact_raw_text(deviation)
        joined = f"{compact_requirement}{compact_response}{compact_deviation}"
        if not joined:
            return False

        header_hits = sum(1 for token in self.COMMON_DUPLICATE_HEADER_TOKENS if token in joined)
        if header_hits >= 4:
            return False

        if compact_requirement and compact_requirement == compact_response and len(compact_requirement) >= 12:
            return False

        if compact_deviation:
            return True

        if not compact_response:
            return False

        if any(token in compact_response for token in ("响应", "相同", "满足", "符合", "偏离", "详见")):
            return True

        return False

    def _segment_from_itemized_section(self, section: dict[str, Any]) -> dict[str, Any] | None:
        """将分项报价区段标准化为查重段落。"""
        lines = self._normalize_scope_lines(section.get("lines") or [])
        if not lines:
            return None

        raw_pages = section.get("pages")
        pages = [page for page in raw_pages if isinstance(page, int)] if isinstance(raw_pages, list) else []
        if not pages and isinstance(section.get("page"), int):
            pages = [int(section["page"])]

        return {
            "title": str(section.get("anchor") or "分项报价表").strip() or "分项报价表",
            "pages": pages or [1],
            "kind": "table",
            "source": "itemized_pricing",
            "lines": lines,
        }

    def _segment_from_deviation_section(self, section: dict[str, Any]) -> dict[str, Any] | None:
        """将偏离表区段标准化为查重段落。"""
        raw_lines = section.get("lines")
        if not isinstance(raw_lines, list) or not raw_lines:
            raw_lines = self.SPLIT_LINE_PATTERN.split(str(section.get("text") or ""))
        lines = self._normalize_scope_lines(
            self._trim_deviation_section_lines(raw_lines),
            preserve_common_lines=True,
        )
        lines = [line for line in lines if self._is_deviation_response_line(line)]
        if not lines:
            return None

        pages: list[int] = []
        line_items = section.get("line_items")
        if isinstance(line_items, list):
            for item in line_items:
                if isinstance(item, dict) and isinstance(item.get("page"), int):
                    page = int(item["page"])
                    if page not in pages:
                        pages.append(page)
        if not pages and isinstance(section.get("page"), int):
            pages.append(int(section["page"]))

        title = str(section.get("title") or "").strip() or "偏离表"
        return {
            "title": title,
            "pages": pages or [1],
            "kind": "table",
            "source": "deviation_table",
            "preserve_common_lines": True,
            "lines": lines,
        }

    def _trim_deviation_section_lines(self, values: list[Any]) -> list[str]:
        """截断偏离表章节中超出边界的行。"""
        trimmed: list[str] = []
        for raw_value in values:
            text = self._normalize_plain_text(raw_value)
            if not text:
                continue
            if trimmed and self._is_deviation_scope_boundary(text):
                break
            trimmed.append(text)
        return trimmed

    def _is_deviation_scope_boundary(self, text: str) -> bool:
        """识别是否到达偏离表范围的边界。"""
        compact = self._compact_raw_text(text)
        if not compact:
            return False
        if "偏离" in compact:
            return False
        if re.match(r"^(附件|附表|附录)\s*[0-9一二三四五六七八九十]+", text):
            return True
        return any(
            token in compact
            for token in (
                "基本情况表",
                "资格证明",
                "资信证明",
                "业绩证明",
                "类似项目",
                "开标一览表",
                "报价一览表",
            )
        )

    # ── 文本清理与规范化工具 ──────────────────────

    def _normalize_scope_lines(
        self,
        values: list[Any],
        *,
        preserve_common_lines: bool = False,
    ) -> list[str]:
        """对范围内的文本行进行规范化并去重。"""
        normalized: list[str] = []
        seen = set()
        for value in values:
            text = self._normalize_plain_text(value)
            if not text:
                continue
            text = self._strip_scope_serial_prefix(text)
            if not text:
                continue
            if not preserve_common_lines and self._is_common_duplicate_scope_line(text):
                continue
            key = self._compact_raw_text(text)
            if not key or key in seen:
                continue
            seen.add(key)
            normalized.append(text)
        return normalized

    def _strip_scope_serial_prefix(self, text: str) -> str:
        """移除文本开头的序号前缀。"""
        normalized = self._normalize_plain_text(text)
        if not normalized:
            return ""
        stripped = re.sub(
            r"^\s*(?:[(（]?\d{1,4}[)）]?[.、:：]?|[一二三四五六七八九十百千]+[、.．])\s+",
            "",
            normalized,
        )
        return stripped.strip()

    def _is_common_duplicate_scope_line(self, text: str) -> bool:
        """判断是否为应忽略的公共模板行（如表头、固定提示语等）。"""
        compact = self._compact_raw_text(text)
        if not compact:
            return True

        for pattern in self.COMMON_DUPLICATE_TEMPLATE_PATTERNS:
            if pattern.search(text) or pattern.search(compact):
                return True

        token_hits = sum(1 for token in self.COMMON_DUPLICATE_HEADER_TOKENS if token in compact)
        if compact in {
            "投标文件的响应情况",
            "投标文件的响应",
            "响应情况",
            "偏离说明",
            "对应材料投标文件所在页",
        }:
            return True
        if "序号" in compact and token_hits >= 4:
            return True
        if token_hits >= 5 and len(compact) <= 80:
            return True
        if compact.endswith("偏离表") and len(compact) <= 30:
            return True

        if "无偏离" in compact and ("与招标文件" in compact or "与采购文件" in compact):
            return True
        if "与招标文件条款相同" in compact or "与采购文件条款相同" in compact:
            return True

        if any(token in compact for token in self.COMMON_DUPLICATE_REQUIREMENT_TOKENS):
            return True

        if 4 <= len(compact) <= 32 and "项目" in compact:
            return True

        if re.fullmatch(r"[（(]?\d+[）)]?[\u4e00-\u9fa5]{0,8}[;；。]?", compact):
            return True
        return False

    def _is_deviation_response_line(self, text: str) -> bool:
        """判断文本行是否为偏离表中的具体响应行。"""
        compact = self._compact_raw_text(text)
        if not compact:
            return False
        if any(token in compact for token in self.DEVIATION_RESPONSE_TOKENS):
            return True
        if re.search(r"(?:^|[^A-Za-z])P\d+", compact, re.IGNORECASE):
            return True
        if re.search(r"第\d+页", compact):
            return True
        return False

    def _dedupe_scoped_segments(self, segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """对查重段落进行去重。"""
        deduped: list[dict[str, Any]] = []
        seen = set()
        for segment in segments:
            joined = "\n".join(segment.get("lines") or [])
            key = self._compact_raw_text(
                f"{segment.get('source') or ''}\n{segment.get('title') or ''}\n{joined}"
            )
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(segment)
        return deduped

    def _scoped_segment_sort_key(self, segment: dict[str, Any]) -> tuple[int, int, str]:
        """定义查重段落的排序键。"""
        pages = [page for page in (segment.get("pages") or []) if isinstance(page, int)]
        first_page = min(pages) if pages else 1
        source = str(segment.get("source") or "")
        source_rank = 0 if source == "itemized_pricing" else 1
        title = str(segment.get("title") or "")
        return (first_page, source_rank, title)

    def _build_scoped_blocks_and_tables(
        self,
        segments: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """将查重段落转换为内部的区块和表格结构。"""
        blocks: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []

        for index, segment in enumerate(segments, start=1):
            lines = self._normalize_scope_lines(
                segment.get("lines") or [],
                preserve_common_lines=bool(segment.get("preserve_common_lines")),
            )
            if not lines:
                continue

            pages = [page for page in (segment.get("pages") or []) if isinstance(page, int)] or [1]
            title = self._normalize_plain_text(segment.get("title") or "") or f"scope_{index}"
            heading_key = self._compact_raw_text(title)
            if heading_key:
                blocks.append(
                    {
                        "type": "heading",
                        "page": pages[0],
                        "text": title,
                        "exact_key": heading_key,
                        "exact_hash": self._hash_text(heading_key),
                    }
                )

            block_type = str(segment.get("kind") or "text")
            for line in lines:
                exact_key = self._compact_raw_text(line)
                if block_type != "heading" and len(exact_key) < 8:
                    continue
                blocks.append(
                    {
                        "type": block_type,
                        "page": pages[0],
                        "text": line,
                        "exact_key": exact_key,
                        "exact_hash": self._hash_text(exact_key),
                    }
                )

            table_text = "\n".join(lines).strip()
            if table_text:
                tables.append(
                    {
                        "pages": pages,
                        "text": table_text,
                        "rows": lines,
                        "exact_hash": self._hash_text(self._compact_raw_text(table_text)),
                    }
                )

        return blocks, tables

    # ── 通用数据解析 ──────────────────────────────

    def _coerce_payload(self, value: Any) -> dict[str, Any]:
        """将可能的字符串 JSON 或纯文本转换为字典。"""
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    return {"text": value}
                return parsed if isinstance(parsed, dict) else {"text": value}
            return {"text": value}
        return {}

    def _container(self, payload: dict[str, Any]) -> dict[str, Any]:
        """提取 payload 中的实际内容容器（优先使用 data 字段）。"""
        if isinstance(payload.get("data"), dict):
            return payload["data"]
        return payload

    def _extract_ordered_blocks(
        self,
        container: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """从文档容器中提取顺序排列的区块和表格。"""
        table_queues, table_entries = self._build_table_queues(container)
        layout_sections = self._layout_sections(container)
        ordered_blocks: list[dict[str, Any]] = []

        for section in layout_sections:
            section_type = str(section.get("type") or "text").strip().lower()
            if section_type in {"seal", "signature"}:
                continue

            page = section.get("page") if isinstance(section.get("page"), int) else 1
            if section_type == "table":
                page_tables = table_queues.get(page) or []
                text = page_tables.pop(0) if page_tables else ""
                if not text:
                    text = self._normalize_plain_text(
                        section.get("raw_text") or section.get("text") or ""
                    )
            else:
                text = self._normalize_plain_text(section.get("text") or section.get("raw_text") or "")

            if self._is_noise_block(text, section_type):
                continue

            exact_key = self._compact_raw_text(text)
            if section_type != "heading" and len(exact_key) < 8:
                continue

            ordered_blocks.append(
                {
                    "type": section_type,
                    "page": page,
                    "text": text,
                    "exact_key": exact_key,
                    "exact_hash": self._hash_text(exact_key),
                }
            )

        return ordered_blocks, table_entries

    def _layout_sections(self, container: dict[str, Any]) -> list[dict[str, Any]]:
        """获取并按位置排序布局区段。"""
        items = container.get("layout_sections")
        if not isinstance(items, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            text = item.get("text") or item.get("raw_text")
            if not text:
                continue
            bbox = item.get("bbox") or item.get("bbox_ocr") or item.get("box")
            anchor = self._bbox_anchor(bbox)
            normalized.append(
                {
                    "page": item.get("page") if isinstance(item.get("page"), int) else 1,
                    "type": str(item.get("type") or "text").strip().lower() or "text",
                    "text": text,
                    "raw_text": item.get("raw_text"),
                    "_sort_y": anchor[1],
                    "_sort_x": anchor[0],
                }
            )

        normalized.sort(key=lambda item: (item["page"], item["_sort_y"], item["_sort_x"]))
        return normalized

    def _bbox_anchor(self, bbox: Any) -> tuple[int, int]:
        """获取 bbox 左上角坐标，用于排序。"""
        if isinstance(bbox, (list, tuple)) and len(bbox) >= 2:
            if all(isinstance(item, (int, float)) for item in bbox[:2]):
                return (int(round(float(bbox[0]))), int(round(float(bbox[1]))))
            if bbox and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and all(isinstance(value, (int, float)) for value in item[:2])
                for item in bbox
            ):
                xs = [int(round(float(item[0]))) for item in bbox]
                ys = [int(round(float(item[1]))) for item in bbox]
                return (min(xs), min(ys))
        return (10**9, 10**9)

    def _build_table_queues(
        self,
        container: dict[str, Any],
    ) -> tuple[dict[int, list[str]], list[dict[str, Any]]]:
        """构建按页码索引的表格文本队列和表格条目列表。"""
        candidates: list[Any] = []
        if isinstance(container.get("logical_tables"), list):
            candidates.extend(container.get("logical_tables") or [])
        if isinstance(container.get("table_sections"), list):
            candidates.extend(container.get("table_sections") or [])

        page_queues: dict[int, list[str]] = {}
        table_entries: list[dict[str, Any]] = []
        for raw_item in candidates:
            if not isinstance(raw_item, dict):
                continue

            lines = self._table_to_lines(raw_item)
            if not lines:
                continue

            pages = raw_item.get("pages")
            if isinstance(pages, list):
                normalized_pages = [page for page in pages if isinstance(page, int)]
            else:
                page = raw_item.get("page")
                normalized_pages = [page] if isinstance(page, int) else [1]

            text = "\n".join(lines).strip()
            if not text:
                continue

            for page in normalized_pages or [1]:
                page_queues.setdefault(page, []).append(text)

            table_entries.append(
                {
                    "pages": normalized_pages or [1],
                    "text": text,
                    "rows": lines,
                    "exact_hash": self._hash_text(self._compact_raw_text(text)),
                }
            )

        return page_queues, table_entries

    def _table_to_lines(self, table: dict[str, Any]) -> list[str]:
        """将表格的多种表示形式统一转换为行列表。"""
        rows = table.get("rows")
        if isinstance(rows, list) and rows:
            result = []
            for row in rows:
                if isinstance(row, dict):
                    values = [str(value).strip() for value in row.values() if str(value).strip()]
                elif isinstance(row, list):
                    values = [str(value).strip() for value in row if str(value).strip()]
                else:
                    values = [str(row).strip()]
                if values:
                    result.append(" | ".join(values))
            if result:
                return result

        records = table.get("records")
        if isinstance(records, list) and records:
            result = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                values = [str(value).strip() for value in record.values() if str(value).strip()]
                if values:
                    result.append(" | ".join(values))
            if result:
                return result

        for key in ("block_content", "html"):
            candidate = str(table.get(key) or "").strip()
            if "<table" in candidate.lower():
                parser = _TableHTMLParser()
                try:
                    parser.feed(candidate)
                    parser.close()
                except Exception:
                    parser = None
                if parser and parser.rows:
                    return [
                        " | ".join(cell for cell in row if cell)
                        for row in parser.rows
                        if any(cell for cell in row)
                    ]

        for key in ("raw_text", "text", "block_content"):
            candidate = self._normalize_plain_text(table.get(key) or "")
            if not candidate:
                continue
            lines = [line.strip() for line in self.SPLIT_LINE_PATTERN.split(candidate) if line.strip()]
            if lines:
                return lines
            return [candidate]

        return []

    # ── 文本规范化 ────────────────────────────────

    def _normalize_plain_text(self, value: Any) -> str:
        """基础文本规范化：反转义、统一空格和换行。"""
        text = html.unescape(str(value or ""))
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _compact_raw_text(self, text: str) -> str:
        """将文本中所有空白字符去除，用于精确哈希比较。"""
        normalized = self._normalize_plain_text(text)
        return re.sub(r"\s+", "", normalized)

    def _is_noise_block(self, text: str, section_type: str) -> bool:
        """判断文本块是否为噪声（页码、目录、过短等）。"""
        compact = self._compact_raw_text(text)
        if not compact:
            return True
        if self.PAGE_NUMBER_PATTERN.fullmatch(compact):
            return True
        if SectionClassifier.RE_TOC.search(text):
            return True
        if section_type != "heading" and len(compact) < 4:
            return True
        return False

    # ── 区段构建 ─────────────────────────────────

    def _build_sections(self, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """将区块按标题聚合为区段。"""
        sections: list[dict[str, Any]] = []
        current = self._new_section("document_prelude")

        for block in blocks:
            is_heading = block["type"] == "heading" or SectionClassifier.is_heading(block["text"])
            if is_heading and current["blocks"]:
                finalized = self._finalize_section(current)
                if finalized is not None:
                    sections.append(finalized)
                current = self._new_section(block["text"])
                current["pages"].add(int(block.get("page") or 1))
                continue

            current["blocks"].append(block)
            current["pages"].add(int(block.get("page") or 1))

        finalized = self._finalize_section(current)
        if finalized is not None:
            sections.append(finalized)

        if len(sections) <= 1:
            fallback_sections = self._build_fallback_sections(blocks)
            if fallback_sections:
                sections = fallback_sections

        return [section for section in sections if len(section.get("exact_key") or "") >= 8]

    def _new_section(self, title: str) -> dict[str, Any]:
        normalized_title = self._normalize_plain_text(title) or "document_prelude"
        return {"title": normalized_title, "pages": set(), "blocks": []}

    def _finalize_section(self, section: dict[str, Any]) -> dict[str, Any] | None:
        blocks = [block for block in section.get("blocks", []) if block.get("text")]
        if not blocks:
            return None

        text = "\n".join(block["text"] for block in blocks if block.get("text")).strip()
        if not text:
            return None

        exact_key = self._compact_raw_text(text)
        return {
            "title": section.get("title") or "document_prelude",
            "pages": sorted(page for page in section.get("pages", set()) if isinstance(page, int)),
            "text": text,
            "preview": self._clip(text, 160),
            "exact_key": exact_key,
            "exact_hash": self._hash_text(exact_key),
        }

    def _build_fallback_sections(
        self,
        blocks: list[dict[str, Any]],
        chunk_size: int = 10,
    ) -> list[dict[str, Any]]:
        """将内容块等距分组成区段，作为无法按标题聚合时的回退方案。"""
        content_blocks = [block for block in blocks if block["type"] != "heading"]
        if len(content_blocks) < 4:
            return []

        sections: list[dict[str, Any]] = []
        for index in range(0, len(content_blocks), chunk_size):
            chunk = content_blocks[index : index + chunk_size]
            if not chunk:
                continue

            text = "\n".join(block["text"] for block in chunk if block.get("text")).strip()
            if not text:
                continue

            exact_key = self._compact_raw_text(text)
            sections.append(
                {
                    "title": f"chunk_{len(sections) + 1}",
                    "pages": sorted(
                        {
                            int(block.get("page") or 1)
                            for block in chunk
                            if isinstance(block.get("page"), int) or block.get("page") is not None
                        }
                    ),
                    "text": text,
                    "preview": self._clip(text, 160),
                    "exact_key": exact_key,
                    "exact_hash": self._hash_text(exact_key),
                }
            )
        return sections

    # ── 相似度匹配辅助 ───────────────────────────

    def _business_similarity_key(self, value: Any) -> str:
        """将文本转换为适合相似度比较的规范化键（替换页码、数字等）。"""
        text = self._strip_scope_serial_prefix(self._normalize_plain_text(value))
        if not text:
            return ""
        text = re.sub(r"第\s*\d+\s*页", " <PAGE> ", text, flags=re.IGNORECASE)
        text = re.sub(r"(?:P|p)\s*\d+(?:\s*-\s*(?:P|p)?\s*\d+)?", " <PAGE> ", text)
        text = re.sub(r"[¥￥]?\d[\d,，.．]*", " <NUM> ", text)
        text = re.sub(r"[()（）【】\[\]{}<>《》:：;；,，、/\\|]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        text = re.sub(r"(?:<num>\s*){2,}", "<num> ", text)
        text = re.sub(r"(?:<page>\s*){2,}", "<page> ", text)
        return text.strip()

    def _similarity_ratio(self, left: str, right: str) -> float:
        """计算两段文本的相似度（0~1）。"""
        if not left or not right:
            return 0.0
        if left == right:
            return 1.0
        return SequenceMatcher(None, left, right).ratio()

    def _build_business_similarity_block_units(
        self,
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从文档中构建用于相似度比较的块单元。"""
        units: list[dict[str, Any]] = []
        for block in document.get("blocks") or []:
            if str(block.get("type") or "") == "heading":
                continue
            similarity_key = self._business_similarity_key(block.get("text") or "")
            if len(self._compact_raw_text(similarity_key)) < self.BUSINESS_SIMILARITY_MIN_KEY_LENGTH:
                continue
            units.append(
                {
                    "page": block.get("page"),
                    "type": block.get("type"),
                    "text": str(block.get("text") or ""),
                    "exact_hash": str(block.get("exact_hash") or ""),
                    "similarity_key": similarity_key,
                }
            )
        return units

    def _build_business_similarity_section_units(
        self,
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从文档中构建用于相似度比较的区段单元。"""
        units: list[dict[str, Any]] = []
        for section in document.get("sections") or []:
            similarity_key = self._business_similarity_key(section.get("text") or "")
            if len(self._compact_raw_text(similarity_key)) < self.BUSINESS_SIMILARITY_MIN_KEY_LENGTH:
                continue
            units.append(
                {
                    "title": str(section.get("title") or ""),
                    "pages": list(section.get("pages") or []),
                    "preview": str(section.get("preview") or ""),
                    "text": str(section.get("text") or ""),
                    "exact_hash": str(section.get("exact_hash") or ""),
                    "similarity_key": similarity_key,
                }
            )
        return units

    def _build_business_similarity_table_units(
        self,
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """从文档中构建用于相似度比较的表格单元。"""
        units: list[dict[str, Any]] = []
        for table in document.get("tables") or []:
            rows = self._normalize_scope_lines(table.get("rows") or [])
            if not rows:
                continue
            similarity_rows = [
                self._business_similarity_key(row)
                for row in rows
                if self._business_similarity_key(row)
            ]
            similarity_rows = [
                row for row in similarity_rows
                if len(self._compact_raw_text(row)) >= self.BUSINESS_SIMILARITY_MIN_KEY_LENGTH
            ]
            if not similarity_rows:
                continue
            units.append(
                {
                    "pages": list(table.get("pages") or []),
                    "rows": rows,
                    "exact_hash": str(table.get("exact_hash") or ""),
                    "similarity_rows": similarity_rows,
                    "similarity_key": "\n".join(similarity_rows),
                }
            )
        return units

    def _match_similarity_units(
        self,
        left_units: list[dict[str, Any]],
        right_units: list[dict[str, Any]],
        *,
        threshold: float,
        key_getter,
        exact_match_getter=None,
    ) -> list[tuple[float, dict[str, Any], dict[str, Any]]]:
        """
        通用相似单元匹配算法：返回 (分数, 左单元, 右单元) 列表，
        每个单元最多匹配一次。
        """
        candidates: list[tuple[float, int, int]] = []
        for left_index, left_unit in enumerate(left_units):
            left_key = str(key_getter(left_unit) or "")
            if not left_key:
                continue
            for right_index, right_unit in enumerate(right_units):
                if exact_match_getter and exact_match_getter(left_unit) == exact_match_getter(right_unit):
                    continue
                right_key = str(key_getter(right_unit) or "")
                if not right_key:
                    continue
                ratio = self._similarity_ratio(left_key, right_key)
                if ratio >= threshold:
                    candidates.append((ratio, left_index, right_index))

        candidates.sort(reverse=True)
        selected: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        used_left: set[int] = set()
        used_right: set[int] = set()
        for ratio, left_index, right_index in candidates:
            if left_index in used_left or right_index in used_right:
                continue
            used_left.add(left_index)
            used_right.add(right_index)
            selected.append((ratio, left_units[left_index], right_units[right_index]))
        return selected

    def _compare_business_similarity_blocks(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """比较两个文档的块级别相似度。"""
        left_units = self._build_business_similarity_block_units(left)
        right_units = self._build_business_similarity_block_units(right)
        matches = self._match_similarity_units(
            left_units,
            right_units,
            threshold=self.BUSINESS_BLOCK_SIMILARITY_THRESHOLD,
            key_getter=lambda item: item.get("similarity_key"),
            exact_match_getter=lambda item: item.get("exact_hash"),
        )
        matched_count = len(matches)
        overlap_ratio = matched_count / max(1, min(len(left_units), len(right_units)))
        items = []
        for ratio, left_unit, right_unit in matches[:max_evidence_sections]:
            items.append(
                {
                    "page": left_unit.get("page"),
                    "left_page": left_unit.get("page"),
                    "right_page": right_unit.get("page"),
                    "type": "similar_sentence",
                    "left_type": left_unit.get("type"),
                    "right_type": right_unit.get("type"),
                    "left_text": self._clip(left_unit.get("text") or "", 160),
                    "right_text": self._clip(right_unit.get("text") or "", 160),
                    "similarity": round(ratio, 4),
                }
            )
        return {
            "similar_overlap_ratio": overlap_ratio,
            "similar_shared_count": matched_count,
            "items": items,
        }

    def _compare_business_similarity_sections(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """比较两个文档的区段级别相似度。"""
        left_units = self._build_business_similarity_section_units(left)
        right_units = self._build_business_similarity_section_units(right)
        matches = self._match_similarity_units(
            left_units,
            right_units,
            threshold=self.BUSINESS_SECTION_SIMILARITY_THRESHOLD,
            key_getter=lambda item: item.get("similarity_key"),
            exact_match_getter=lambda item: item.get("exact_hash"),
        )
        matched_count = len(matches)
        overlap_ratio = matched_count / max(1, min(len(left_units), len(right_units)))
        items = []
        for ratio, left_unit, right_unit in matches[:max_evidence_sections]:
            items.append(
                {
                    "left_title": left_unit.get("title"),
                    "right_title": right_unit.get("title"),
                    "left_pages": left_unit.get("pages", []),
                    "right_pages": right_unit.get("pages", []),
                    "exact": False,
                    "similarity": round(ratio, 4),
                    "left_preview": left_unit.get("preview"),
                    "right_preview": right_unit.get("preview"),
                }
            )
        return {
            "similar_match_count": matched_count,
            "similar_match_ratio": overlap_ratio,
            "items": items,
        }

    def _table_similarity_ratio(self, left_rows: list[str], right_rows: list[str]) -> float:
        """计算两个表格行列表的相似度（基于行匹配比例）。"""
        if not left_rows or not right_rows:
            return 0.0
        matches = self._match_similarity_units(
            [{"similarity_key": row, "exact_hash": row} for row in left_rows],
            [{"similarity_key": row, "exact_hash": row} for row in right_rows],
            threshold=self.BUSINESS_BLOCK_SIMILARITY_THRESHOLD,
            key_getter=lambda item: item.get("similarity_key"),
            exact_match_getter=None,
        )
        return len(matches) / max(1, min(len(left_rows), len(right_rows)))

    def _compare_business_similarity_tables(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """比较两个文档的表格级别相似度。"""
        left_units = self._build_business_similarity_table_units(left)
        right_units = self._build_business_similarity_table_units(right)
        candidates: list[tuple[float, int, int]] = []
        for left_index, left_unit in enumerate(left_units):
            for right_index, right_unit in enumerate(right_units):
                if left_unit.get("exact_hash") == right_unit.get("exact_hash"):
                    continue
                text_ratio = self._similarity_ratio(
                    str(left_unit.get("similarity_key") or ""),
                    str(right_unit.get("similarity_key") or ""),
                )
                row_ratio = self._table_similarity_ratio(
                    list(left_unit.get("similarity_rows") or []),
                    list(right_unit.get("similarity_rows") or []),
                )
                score = max(text_ratio, row_ratio)
                if score >= self.BUSINESS_TABLE_SIMILARITY_THRESHOLD:
                    candidates.append((score, left_index, right_index))

        candidates.sort(reverse=True)
        selected: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        used_left: set[int] = set()
        used_right: set[int] = set()
        for score, left_index, right_index in candidates:
            if left_index in used_left or right_index in used_right:
                continue
            used_left.add(left_index)
            used_right.add(right_index)
            selected.append((score, left_units[left_index], right_units[right_index]))

        matched_count = len(selected)
        overlap_ratio = matched_count / max(1, min(len(left_units), len(right_units)))
        items = []
        for score, left_unit, right_unit in selected[:max_evidence_sections]:
            items.append(
                {
                    "left_pages": left_unit.get("pages", []),
                    "right_pages": right_unit.get("pages", []),
                    "exact": False,
                    "similarity": round(score, 4),
                    "left_rows": [self._clip(row, 200) for row in list(left_unit.get("rows") or [])],
                    "right_rows": [self._clip(row, 200) for row in list(right_unit.get("rows") or [])],
                    "left_sample_rows": [self._clip(row, 160) for row in list(left_unit.get("rows") or [])[:3]],
                    "right_sample_rows": [self._clip(row, 160) for row in list(right_unit.get("rows") or [])[:3]],
                }
            )
        return {
            "similar_match_count": matched_count,
            "similar_match_ratio": overlap_ratio,
            "items": items,
        }

    # ── 评分与风险判定 ───────────────────────────

    def _business_similarity_match_score(
        self,
        *,
        similar_block_overlap_ratio: float,
        similar_section_match_ratio: float,
        similar_table_match_ratio: float,
    ) -> float:
        """加权计算商务标相似度综合匹配分数（0~1）。"""
        score = (
            (0.45 * similar_section_match_ratio)
            + (0.35 * similar_block_overlap_ratio)
            + (0.20 * similar_table_match_ratio)
        )
        return min(round(score, 4), 0.9999)

    def _supports_similarity_matching(self, role: str) -> bool:
        """当前文档角色是否支持相似度匹配。"""
        return role in {DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID}

    def _business_risk_level(
        self,
        *,
        exact_duplicate: bool,
        exact_match_score: float,
        exact_block_count: int,
        exact_section_count: int,
        exact_table_count: int,
        exact_image_count: int,
        exact_block_overlap_ratio: float,
        similar_match_score: float,
        similar_block_count: int,
        similar_section_count: int,
        similar_table_count: int,
        similar_block_overlap_ratio: float,
        similar_section_overlap_ratio: float,
        similar_table_overlap_ratio: float,
    ) -> str:
        """综合精确匹配和相似度匹配判断商务标的最终风险等级。"""
        exact_risk = self._exact_risk_level(
            exact_duplicate=exact_duplicate,
            exact_match_score=exact_match_score,
            exact_block_count=exact_block_count,
            exact_section_count=exact_section_count,
            exact_table_count=exact_table_count,
            exact_image_count=exact_image_count,
            exact_block_overlap_ratio=exact_block_overlap_ratio,
        )
        if self._risk_rank(exact_risk) >= self._risk_rank("medium"):
            return exact_risk
        if similar_table_count >= 1 and similar_section_count >= 1:
            return "high"
        if similar_match_score >= 0.6 and similar_block_count >= 3:
            return "high"
        if similar_section_count >= 1 and similar_block_count >= 2:
            return "medium"
        if similar_table_count >= 1 or similar_block_overlap_ratio >= 0.45:
            return "medium"
        if similar_block_count >= 1 or similar_section_overlap_ratio >= 0.3 or similar_table_overlap_ratio >= 0.3:
            return "low"
        return exact_risk

    def _fallback_text(self, container: dict[str, Any]) -> str:
        """从容器中提取回退全文文本。"""
        for key in ("content", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return self._normalize_plain_text(value)

        pages = container.get("pages")
        if isinstance(pages, list):
            parts = []
            for item in pages:
                if isinstance(item, dict):
                    text = self._normalize_plain_text(item.get("text") or "")
                    if text:
                        parts.append(text)
            return "\n".join(parts).strip()

        return ""

    # ── 文档比较核心 ─────────────────────────────

    def _compare_documents(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        role: str,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """执行两个文档的完整比较（精确+相似度），返回结构化比较记录。"""
        block_metrics = self._compare_blocks(left, right, max_evidence_sections=max_evidence_sections)
        section_metrics = self._compare_sections(left, right, max_evidence_sections=max_evidence_sections)
        table_metrics = self._compare_tables(left, right, max_evidence_sections=max_evidence_sections)
        image_metrics = self._compare_images(left, right, max_evidence_sections=max_evidence_sections)

        exact_duplicate = bool(left["exact_hash"] == right["exact_hash"])
        exact_match_score = self._exact_match_score(
            exact_duplicate=exact_duplicate,
            exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
            exact_section_match_ratio=float(section_metrics["exact_match_ratio"]),
            exact_table_match_ratio=float(table_metrics["exact_match_ratio"]),
            exact_image_match_ratio=float(image_metrics["exact_match_ratio"]),
        )
        similar_block_metrics = {
            "similar_overlap_ratio": 0.0,
            "similar_shared_count": 0,
            "items": [],
        }
        similar_section_metrics = {
            "similar_match_ratio": 0.0,
            "similar_match_count": 0,
            "items": [],
        }
        similar_table_metrics = {
            "similar_match_ratio": 0.0,
            "similar_match_count": 0,
            "items": [],
        }
        similar_match_score = 0.0
        if self._supports_similarity_matching(role):
            similar_block_metrics = self._compare_business_similarity_blocks(
                left, right, max_evidence_sections=max_evidence_sections,
            )
            similar_section_metrics = self._compare_business_similarity_sections(
                left, right, max_evidence_sections=max_evidence_sections,
            )
            similar_table_metrics = self._compare_business_similarity_tables(
                left, right, max_evidence_sections=max_evidence_sections,
            )
            similar_match_score = self._business_similarity_match_score(
                similar_block_overlap_ratio=float(similar_block_metrics["similar_overlap_ratio"]),
                similar_section_match_ratio=float(similar_section_metrics["similar_match_ratio"]),
                similar_table_match_ratio=float(similar_table_metrics["similar_match_ratio"]),
            )
            risk_level = self._business_risk_level(
                exact_duplicate=exact_duplicate,
                exact_match_score=exact_match_score,
                exact_block_count=int(block_metrics["exact_shared_count"]),
                exact_section_count=int(section_metrics["exact_match_count"]),
                exact_table_count=int(table_metrics["exact_match_count"]),
                exact_image_count=int(image_metrics["exact_match_count"]),
                exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
                similar_match_score=similar_match_score,
                similar_block_count=int(similar_block_metrics["similar_shared_count"]),
                similar_section_count=int(similar_section_metrics["similar_match_count"]),
                similar_table_count=int(similar_table_metrics["similar_match_count"]),
                similar_block_overlap_ratio=float(similar_block_metrics["similar_overlap_ratio"]),
                similar_section_overlap_ratio=float(similar_section_metrics["similar_match_ratio"]),
                similar_table_overlap_ratio=float(similar_table_metrics["similar_match_ratio"]),
            )
        else:
            risk_level = self._exact_risk_level(
                exact_duplicate=exact_duplicate,
                exact_match_score=exact_match_score,
                exact_block_count=int(block_metrics["exact_shared_count"]),
                exact_section_count=int(section_metrics["exact_match_count"]),
                exact_table_count=int(table_metrics["exact_match_count"]),
                exact_image_count=int(image_metrics["exact_match_count"]),
                exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
            )

        match_score = max(exact_match_score, similar_match_score)

        notes = []
        if not left["tables"] or not right["tables"]:
            notes.append("at_least_one_document_has_no_structured_table_content")
        if not left["sections"] or not right["sections"]:
            notes.append("at_least_one_document_has_no_stable_section_structure")
        if not left["images"] or not right["images"]:
            notes.append("at_least_one_document_has_no_extractable_image_content")

        return {
            "left_document_identifier": left["identifier_id"],
            "right_document_identifier": right["identifier_id"],
            "left_relation_id": left.get("relation_id"),
            "right_relation_id": right.get("relation_id"),
            "left_file_name": left.get("file_name"),
            "right_file_name": right.get("file_name"),
            "document_type": role,
            "exact_duplicate": exact_duplicate,
            "exact_match_score": round(exact_match_score, 4),
            "similarity_match_score": round(similar_match_score, 4),
            "match_score": round(match_score, 4),
            "risk_level": risk_level,
            "suspicious": risk_level != "none",
            "metrics": {
                "exact_block_count": int(block_metrics["exact_shared_count"]),
                "exact_section_count": int(section_metrics["exact_match_count"]),
                "exact_table_count": int(table_metrics["exact_match_count"]),
                "exact_image_count": int(image_metrics["exact_match_count"]),
                "exact_block_overlap_ratio": round(float(block_metrics["exact_overlap_ratio"]), 4),
                "exact_section_overlap_ratio": round(float(section_metrics["exact_match_ratio"]), 4),
                "exact_table_overlap_ratio": round(float(table_metrics["exact_match_ratio"]), 4),
                "exact_image_overlap_ratio": round(float(image_metrics["exact_match_ratio"]), 4),
                "similar_block_count": int(similar_block_metrics["similar_shared_count"]),
                "similar_section_count": int(similar_section_metrics["similar_match_count"]),
                "similar_table_count": int(similar_table_metrics["similar_match_count"]),
                "similar_block_overlap_ratio": round(float(similar_block_metrics["similar_overlap_ratio"]), 4),
                "similar_section_overlap_ratio": round(float(similar_section_metrics["similar_match_ratio"]), 4),
                "similar_table_overlap_ratio": round(float(similar_table_metrics["similar_match_ratio"]), 4),
            },
            "duplicate_blocks": block_metrics["items"],
            "duplicate_sections": section_metrics["items"],
            "duplicate_tables": table_metrics["items"],
            "duplicate_images": image_metrics["items"],
            "similar_blocks": similar_block_metrics["items"],
            "similar_sections": similar_section_metrics["items"],
            "similar_tables": similar_table_metrics["items"],
            "notes": notes,
        }

    # ── 精确比较方法 ─────────────────────────────

    def _compare_blocks(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """基于句子哈希精确比较两个文档的区块重复度。"""
        common_hashes = left["exact_block_hashes"] & right["exact_block_hashes"]
        overlap_ratio = self._dice_ratio(left["exact_block_hashes"], right["exact_block_hashes"])

        items = []
        for block_hash in sorted(common_hashes):
            left_block = left["exact_block_map"].get(block_hash)
            right_block = right["exact_block_map"].get(block_hash)
            if not left_block or not right_block:
                continue
            items.append(
                {
                    "page": left_block.get("page"),
                    "left_page": left_block.get("page"),
                    "right_page": right_block.get("page"),
                    "type": "sentence",
                    "left_type": left_block.get("type"),
                    "right_type": right_block.get("type"),
                    "text": self._clip(left_block.get("text") or "", 160),
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_overlap_ratio": overlap_ratio,
            "exact_shared_count": len(common_hashes),
            "items": items,
        }

    def _compare_sections(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """基于区段哈希精确比较两个文档的段落重复度。"""
        common_hashes = left["exact_section_hashes"] & right["exact_section_hashes"]
        exact_match_ratio = len(common_hashes) / max(
            1,
            min(len(left["exact_section_hashes"]), len(right["exact_section_hashes"])),
        )

        items = []
        for section_hash in sorted(common_hashes):
            left_section = left["exact_section_map"].get(section_hash)
            right_section = right["exact_section_map"].get(section_hash)
            if not left_section or not right_section:
                continue
            items.append(
                {
                    "left_title": left_section["title"],
                    "right_title": right_section["title"],
                    "left_pages": left_section.get("pages", []),
                    "right_pages": right_section.get("pages", []),
                    "exact": True,
                    "left_preview": left_section.get("preview"),
                    "right_preview": right_section.get("preview"),
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_match_count": len(common_hashes),
            "exact_match_ratio": exact_match_ratio,
            "items": items,
        }

    def _compare_tables(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """基于表格哈希精确比较两个文档的表格重复度。"""
        common_hashes = left["exact_table_hashes"] & right["exact_table_hashes"]
        exact_match_ratio = len(common_hashes) / max(
            1,
            min(len(left["exact_table_hashes"]), len(right["exact_table_hashes"])),
        )

        items = []
        for table_hash in sorted(common_hashes):
            left_table = left["exact_table_map"].get(table_hash)
            right_table = right["exact_table_map"].get(table_hash)
            if not left_table or not right_table:
                continue
            items.append(
                {
                    "left_pages": left_table.get("pages", []),
                    "right_pages": right_table.get("pages", []),
                    "exact": True,
                    "left_rows": [self._clip(row, 200) for row in list(left_table.get("rows", []) or [])],
                    "right_rows": [self._clip(row, 200) for row in list(right_table.get("rows", []) or [])],
                    "sample_rows": [self._clip(row, 160) for row in left_table.get("rows", [])[:3]],
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_match_count": len(common_hashes),
            "exact_match_ratio": exact_match_ratio,
            "items": items,
        }

    def _compare_images(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """基于图像哈希精确比较两个文档的图片重复度。"""
        common_hashes = left["exact_image_hashes"] & right["exact_image_hashes"]
        exact_match_ratio = len(common_hashes) / max(
            1,
            min(len(left["exact_image_hashes"]), len(right["exact_image_hashes"])),
        )

        items = []
        for image_hash in sorted(common_hashes):
            left_image = left["exact_image_map"].get(image_hash)
            right_image = right["exact_image_map"].get(image_hash)
            if not left_image or not right_image:
                continue
            items.append(
                {
                    "left_pages": list(left_image.get("pages") or []),
                    "right_pages": list(right_image.get("pages") or []),
                    "left_width": left_image.get("width"),
                    "left_height": left_image.get("height"),
                    "right_width": right_image.get("width"),
                    "right_height": right_image.get("height"),
                    "image_hash": image_hash,
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_match_count": len(common_hashes),
            "exact_match_ratio": exact_match_ratio,
            "items": items,
        }

    # ── 评分与风险工具 ───────────────────────────

    def _dice_ratio(self, left: set[Any], right: set[Any]) -> float:
        """计算两个集合的 Dice 系数。"""
        if not left or not right:
            return 0.0
        if left == right:
            return 1.0
        overlap = len(left & right)
        return (2.0 * overlap) / float(len(left) + len(right))

    def _exact_match_score(
        self,
        *,
        exact_duplicate: bool,
        exact_block_overlap_ratio: float,
        exact_section_match_ratio: float,
        exact_table_match_ratio: float,
        exact_image_match_ratio: float,
    ) -> float:
        """加权计算精确匹配分数（0~1）。"""
        if exact_duplicate:
            return 1.0
        score = (
            (0.40 * exact_section_match_ratio)
            + (0.35 * exact_block_overlap_ratio)
            + (0.20 * exact_table_match_ratio)
            + (0.05 * exact_image_match_ratio)
        )
        return min(round(score, 4), 0.9999)

    def _exact_risk_level(
        self,
        *,
        exact_duplicate: bool,
        exact_match_score: float,
        exact_block_count: int,
        exact_section_count: int,
        exact_table_count: int,
        exact_image_count: int,
        exact_block_overlap_ratio: float,
    ) -> str:
        """根据精确匹配指标判定风险等级。"""
        if exact_duplicate:
            return "high"
        if exact_table_count >= 2:
            return "high"
        if exact_image_count >= 3:
            return "high"
        if exact_match_score >= 0.35 and exact_section_count >= 5:
            return "high"
        if exact_section_count >= 3 or exact_table_count >= 1 or exact_image_count >= 2:
            return "medium"
        if exact_block_count >= 5 or exact_block_overlap_ratio >= 0.15:
            return "medium"
        if exact_block_count >= 1 or exact_image_count >= 1:
            return "low"
        return "none"

    def _risk_rank(self, risk_level: Any) -> int:
        """将风险等级字符串转换为数值（用于排序）。"""
        mapping = {"high": 3, "medium": 2, "low": 1, "none": 0}
        return mapping.get(str(risk_level or "none"), 0)

    # ── 图片提取 ─────────────────────────────────

    def _get_minio_service(self) -> MinioService:
        """延迟初始化 MinIO 服务。"""
        if self._minio_service is None:
            self._minio_service = MinioService()
        return self._minio_service

    def _extract_document_images(
        self,
        record: dict[str, Any],
        *,
        role: str,
    ) -> list[dict[str, Any]]:
        """从文档记录中提取图片条目（仅技术标支持）。"""
        if role != DOCUMENT_TYPE_TECHNICAL_BID:
            return []

        file_url = str(record.get("file_url") or "").strip()
        if not file_url:
            return []
        cached = self._document_image_cache.get(file_url)
        if cached is not None:
            return [dict(item) for item in cached]

        try:
            bucket_name, object_name = MinioService.bucket_and_object_from_file_url(file_url)
            file_bytes, content_type = self._get_minio_service().get_object_bytes(
                object_name, bucket_name=bucket_name,
            )
            images = self._extract_image_entries_from_file_bytes(
                file_bytes,
                file_name=str(record.get("file_name") or object_name),
                content_type=content_type,
            )
        except Exception:
            images = []

        self._document_image_cache[file_url] = [dict(item) for item in images]
        return images

    def _extract_image_entries_from_file_bytes(
        self,
        file_bytes: bytes,
        *,
        file_name: str,
        content_type: str,
    ) -> list[dict[str, Any]]:
        """根据文件字节和类型提取图片条目。"""
        normalized_name = str(file_name or "").strip().lower()
        normalized_type = str(content_type or "").strip().lower()
        if normalized_name.endswith(".pdf") or "pdf" in normalized_type:
            return self._extract_pdf_image_entries(file_bytes)
        return self._extract_raster_image_entries(file_bytes)

    def _extract_pdf_image_entries(self, file_bytes: bytes) -> list[dict[str, Any]]:
        """从 PDF 文件中提取所有图片并计算哈希。"""
        try:
            import fitz
        except Exception:
            return []

        entries_by_hash: dict[str, dict[str, Any]] = {}
        try:
            document = fitz.open(stream=file_bytes, filetype="pdf")
        except Exception:
            return []

        try:
            for page_index in range(document.page_count):
                page = document.load_page(page_index)
                seen_page_hashes: set[str] = set()
                for image_meta in page.get_images(full=True):
                    xref = int(image_meta[0])
                    try:
                        extracted = document.extract_image(xref)
                    except Exception:
                        continue
                    image_bytes = extracted.get("image")
                    if not image_bytes:
                        continue
                    image_entry = self._build_image_entry(image_bytes=image_bytes, page=page_index + 1)
                    if image_entry is None:
                        continue
                    image_hash = str(image_entry.get("exact_hash") or "")
                    if not image_hash or image_hash in seen_page_hashes:
                        continue
                    seen_page_hashes.add(image_hash)
                    existing = entries_by_hash.get(image_hash)
                    if existing is None:
                        entries_by_hash[image_hash] = image_entry
                    else:
                        merged_pages = sorted(
                            set(existing.get("pages") or []) | set(image_entry.get("pages") or [])
                        )
                        existing["pages"] = merged_pages
        finally:
            document.close()

        return sorted(entries_by_hash.values(), key=lambda item: (item.get("pages") or [10**9])[0])

    def _extract_raster_image_entries(self, file_bytes: bytes) -> list[dict[str, Any]]:
        """将光栅图片（如 PNG/JPG）作为单张图片提取。"""
        image_entry = self._build_image_entry(image_bytes=file_bytes, page=1)
        return [image_entry] if image_entry is not None else []

    def _build_image_entry(
        self,
        *,
        image_bytes: bytes,
        page: int,
    ) -> dict[str, Any] | None:
        """根据图片字节构建图片条目，计算 SHA256 哈希。"""
        try:
            from PIL import Image
        except Exception:
            return None

        try:
            with Image.open(io.BytesIO(image_bytes)) as image:
                rgb = image.convert("RGB")
                width, height = rgb.size
                if width < 80 or height < 80 or (width * height) < 20000:
                    return None
                pixel_bytes = rgb.tobytes()
        except Exception:
            return None

        exact_hash = hashlib.sha256(
            f"{width}x{height}|rgb|".encode("utf-8") + pixel_bytes
        ).hexdigest()
        return {
            "pages": [int(page)],
            "width": int(width),
            "height": int(height),
            "exact_hash": exact_hash,
        }

    # ── 通用工具 ─────────────────────────────────

    def _pair_sort_key(self, item: dict[str, Any]) -> tuple[Any, ...]:
        """定义比较结果对（pair）的排序键，风险高的优先。"""
        metrics = item.get("metrics") or {}
        return (
            self._risk_rank(item.get("risk_level")),
            bool(item.get("exact_duplicate")),
            float(item.get("match_score") or item.get("exact_match_score") or 0.0),
            int(metrics.get("similar_table_count") or 0),
            int(metrics.get("similar_section_count") or 0),
            int(metrics.get("similar_block_count") or 0),
            int(metrics.get("exact_table_count") or 0),
            int(metrics.get("exact_section_count") or 0),
            int(metrics.get("exact_block_count") or 0),
        )

    def _normalize_requested_types(self, document_types: list[str] | None) -> tuple[str, ...]:
        """标准化请求的文档类型列表，默认返回全部支持类型。"""
        if not document_types:
            return self.SUPPORTED_DOCUMENT_TYPES

        normalized: list[str] = []
        for item in document_types:
            role = self._normalize_document_role(item)
            if role not in self.SUPPORTED_DOCUMENT_TYPES:
                raise ValueError(f"Unsupported duplicate-check document type: {item}")
            if role not in normalized:
                normalized.append(role)
        return tuple(normalized) if normalized else self.SUPPORTED_DOCUMENT_TYPES

    def _normalize_document_role(self, value: Any) -> str:
        """将文档角色字符串标准化为内部常量。"""
        normalized = str(value or "").strip().lower()
        if normalized in {"business", "business_bid"}:
            return DOCUMENT_TYPE_BUSINESS_BID
        if normalized in {"technical", "technical_bid"}:
            return DOCUMENT_TYPE_TECHNICAL_BID
        return normalized

    def _hash_text(self, text: str) -> str:
        """计算文本的 SHA256 哈希（UTF-8 编码）。"""
        return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()

    def _clip(self, text: str, max_chars: int) -> str:
        """将文本截断到指定长度，超长部分用省略号表示。"""
        if len(text) <= max_chars:
            return text
        return f"{text[:max_chars].rstrip()}..."