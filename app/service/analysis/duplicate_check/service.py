# -*- coding: utf-8 -*-
"""
文档查重服务门面（组合所有子模块，对外提供统一接口）
"""
import json
import re
from itertools import combinations
from typing import Any

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID
from app.service.minio_service import MinioService
from app.service.analysis.location_utils import append_location, make_location, normalize_bbox
from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.compliance.template_extractor import SectionClassifier

from .constants import (
    SUPPORTED_DOCUMENT_TYPES,
    BUSINESS_SCOPE_SKIP_REASON,
    TEMPLATE_EXCLUDED_SKIP_REASON,
    MAX_EVIDENCE_ITEMS_PER_PAIR,
)
from .text_utils import (
    normalize_plain_text,
    compact_raw_text,
    hash_text,
    clip,
    split_sentences,
)
from .block_extractor import extract_document_content as _extract_document_content_blocks
from .business_scope import extract_business_duplicate_segments
from .template_excluder import exclude_template_content, _build_template_placeholder_patterns
from .image_extractor import extract_document_images as _extract_document_images
from .comparators.exact import compare_blocks, compare_sections, compare_tables, compare_images
from .comparators.similarity import (
    compare_business_similarity_blocks,
    compare_business_similarity_sections,
    compare_business_similarity_tables,
)
from .risk_scorer import (
    exact_match_score,
    exact_risk_level,
    business_similarity_match_score,
    business_risk_level,
    risk_rank,
)


class DuplicateCheckService:
    """文档查重服务，支持精确匹配和基于相似度的内容分析。"""

    SUPPORTED_DOCUMENT_TYPES = SUPPORTED_DOCUMENT_TYPES
    BUSINESS_SCOPE_SKIP_REASON = BUSINESS_SCOPE_SKIP_REASON
    TEMPLATE_EXCLUDED_SKIP_REASON = TEMPLATE_EXCLUDED_SKIP_REASON

    def __init__(self) -> None:
        self._itemized_checker = ItemizedPricingChecker()
        self._deviation_checker = DeviationChecker()
        self._minio_service: MinioService | None = None
        self._document_image_cache: dict[str, list[dict[str, Any]]] = {}
        self._short_duplicate_typo_service: Any | None = None

    # 公共入口
    def check_project_documents(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
        max_evidence_sections: int = 5,
        max_pairs_per_type: int = 0,
        duplicate_scope: str | None = None,
    ) -> dict[str, Any]:
        """主入口：对项目中的文档分组进行两两比较，返回查重分析结果。"""
        requested_types = self._normalize_requested_types(document_types)
        enabled_similarity_roles = {
            role for role in requested_types if self._supports_similarity_matching(role)
        }
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

            template_context = self._get_tender_template_context(
                record, role=role, cache=template_cache,
            )
            prepared, skip_reason = self._prepare_document(
                record,
                role=role,
                template_context=template_context,
                duplicate_scope=duplicate_scope,
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
            compared_pair_items = [
                self._filter_short_duplicate_evidence(
                    self._compare_documents(left, right, role=role, max_evidence_sections=max_evidence_sections)
                )
                for left, right in combinations(documents, 2)
            ]
            total_pair_count = len(compared_pair_items)
            pair_items = [
                item for item in compared_pair_items if str(item.get("risk_level") or "none") != "none"
            ]
            pair_items.sort(key=self._pair_sort_key, reverse=True)

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
                "issues": pair_items,
            }

        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "detection_mode": self._resolve_detection_mode(
                    requested_types=requested_types,
                    enabled_similarity_roles=enabled_similarity_roles,
                ),
                "document_types": list(requested_types),
                "max_evidence_sections": int(max_evidence_sections),
                "max_pairs_per_type": int(max_pairs_per_type),
                "template_exclusion_enabled": True,
                "template_exclusion_source": "tender_document",
                "block_matching_unit": "sentence",
                "business_similarity_enabled": DOCUMENT_TYPE_BUSINESS_BID in enabled_similarity_roles,
                "technical_similarity_enabled": DOCUMENT_TYPE_TECHNICAL_BID in enabled_similarity_roles,
                "duplicate_scope": duplicate_scope or "full",
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

    # 文档预处理
    def _prepare_document(
        self,
        record: dict[str, Any],
        *,
        role: str,
        template_context: dict[str, Any] | None = None,
        duplicate_scope: str | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """对单条文档记录提取内容，排除模板，返回可用于比较的结构化对象。"""
        payload = self._coerce_payload(record.get("content"))
        if role == DOCUMENT_TYPE_BUSINESS_BID:
            # 商务标先按招标模板清洗可比内容，再进入统一查重。
            scoped_segments = extract_business_duplicate_segments(
                payload,
                itemized_checker=self._itemized_checker,
                deviation_checker=self._deviation_checker,
                star_requirement_context=(
                    template_context.get("star_requirement_context") if template_context else None
                ),
                deviation_template_context=(
                    template_context.get("deviation_template_context") if template_context else None
                ),
                itemized_template_context=(
                    template_context.get("itemized_template_context") if template_context else None
                ),
            )
            scoped_segments = self._filter_business_duplicate_segments(
                scoped_segments,
                duplicate_scope=duplicate_scope,
            )
            if not scoped_segments:
                return None, BUSINESS_SCOPE_SKIP_REASON
            ordered_blocks, table_entries = self._build_scoped_blocks_and_tables(scoped_segments)
            empty_reason = BUSINESS_SCOPE_SKIP_REASON
        else:
            ordered_blocks, table_entries, empty_reason = _extract_document_content_blocks(payload, role=role)

        if not ordered_blocks:
            # 即使没有区块，仍尝试仅用表格构建文档，供表格重复检测
            prepared = self._build_prepared_document(record, [], table_entries, role=role)
            if prepared is not None:
                return prepared, None
            return None, empty_reason

        ordered_blocks, table_entries = exclude_template_content(
            ordered_blocks, table_entries, template_context=template_context,
        )
        if not ordered_blocks:
            prepared = self._build_prepared_document(record, [], table_entries, role=role)
            if prepared is not None:
                return prepared, None
            return None, TEMPLATE_EXCLUDED_SKIP_REASON

        prepared = self._build_prepared_document(record, ordered_blocks, table_entries, role=role)
        return prepared, None if prepared is not None else empty_reason

    @staticmethod
    def _filter_business_duplicate_segments(
        segments: list[dict[str, Any]],
        *,
        duplicate_scope: str | None,
    ) -> list[dict[str, Any]]:
        scope = str(duplicate_scope or "").strip().lower()
        if not scope or scope in {"full", "business", "business_bid"}:
            return list(segments or [])
        if scope in {"itemized", "itemized_pricing", "business_itemized"}:
            allowed_sources = {"itemized_pricing"}
        elif scope in {"response", "bid_response", "deviation", "deviation_response"}:
            allowed_sources = {"deviation_table"}
        else:
            return list(segments or [])
        return [
            segment
            for segment in (segments or [])
            if str(segment.get("source") or "") in allowed_sources
        ]

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
        exact_key = compact_raw_text(full_text)
        image_entries = self._get_document_images(record, role=role)
        exact_block_units, exact_block_map, exact_block_occurrence_map = self._build_sentence_unit_index(
            ordered_blocks
        )
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
            "exact_hash": hash_text(exact_hash_source),
            "blocks": ordered_blocks,
            "exact_block_units": exact_block_units,
            "exact_block_hashes": set(exact_block_occurrence_map.keys()),
            "exact_block_map": exact_block_map,
            "exact_block_occurrence_map": exact_block_occurrence_map,
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

    def _get_document_images(
        self,
        record: dict[str, Any],
        *,
        role: str,
    ) -> list[dict[str, Any]]:
        """缓存化的图片提取。"""
        if role != DOCUMENT_TYPE_TECHNICAL_BID:
            return []

        file_url = str(record.get("file_url") or "").strip()
        if not file_url:
            return []
        cached = self._document_image_cache.get(file_url)
        if cached is not None:
            return [dict(item) for item in cached]

        minio = self._get_minio_service()
        images = _extract_document_images(
            record, role=role, minio_service=minio,
        )
        self._document_image_cache[file_url] = [dict(item) for item in images]
        return images

    def _get_minio_service(self) -> MinioService:
        """延迟初始化 MinIO 服务。"""
        if self._minio_service is None:
            self._minio_service = MinioService()
        return self._minio_service

    # 区块/区段构建辅助
    def _build_sentence_unit_map(
        self,
        ordered_blocks: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """将区块划分为句子并建立精确哈希映射。"""
        return self._build_sentence_unit_index(ordered_blocks)[1]

    def _build_sentence_unit_index(
        self,
        ordered_blocks: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        """将区块划分为有序句子，并同时建立首条映射与全量出现映射。"""
        sentence_units: list[dict[str, Any]] = []
        sentence_map: dict[str, dict[str, Any]] = {}
        occurrence_map: dict[str, list[dict[str, Any]]] = {}
        for block_index, block in enumerate(ordered_blocks):
            if block.get("type") == "heading":
                continue
            for sentence_index, sentence in enumerate(self._sentence_units_from_block(block)):
                normalized = dict(sentence)
                normalized["sequence"] = len(sentence_units)
                normalized["block_index"] = block_index
                normalized["sentence_index"] = sentence_index
                sentence_units.append(normalized)
                sentence_map.setdefault(normalized["exact_hash"], normalized)
                occurrence_map.setdefault(normalized["exact_hash"], []).append(normalized)
        return sentence_units, sentence_map, occurrence_map

    def _sentence_units_from_block(self, block: dict[str, Any]) -> list[dict[str, Any]]:
        """从文本块中拆分句子并计算哈希。"""
        from .constants import MIN_SENTENCE_COMPACT_LENGTH as MIN_LENGTH
        text = normalize_plain_text(block.get("text") or "")
        if not text:
            return []

        items: list[dict[str, Any]] = []
        for sentence_text in split_sentences(text):
            exact_key = compact_raw_text(sentence_text)
            if len(exact_key) < MIN_LENGTH:
                continue
            items.append(
                {
                    "page": block.get("page"),
                    "bbox": block.get("bbox"),
                    "type": block.get("type"),
                    "text": sentence_text,
                    "exact_key": exact_key,
                    "exact_hash": hash_text(exact_key),
                }
            )
        return items

    def _build_sections(self, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """将区块按标题聚合为区段。"""
        sections: list[dict[str, Any]] = []
        current = self._new_section("document_prelude")

        for block in blocks:
            is_heading = block["type"] == "heading" or SectionClassifier.is_heading(block["text"])
            if is_heading:
                if current["blocks"]:
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
        normalized_title = normalize_plain_text(title) or "document_prelude"
        return {"title": normalized_title, "pages": set(), "blocks": []}

    def _finalize_section(self, section: dict[str, Any]) -> dict[str, Any] | None:
        blocks = [block for block in section.get("blocks", []) if block.get("text")]
        if not blocks:
            return None

        text = "\n".join(block["text"] for block in blocks if block.get("text")).strip()
        if not text:
            return None

        exact_key = compact_raw_text(text)
        pages = sorted(page for page in section.get("pages", set()) if isinstance(page, int))
        first_bbox = next((block.get("bbox") for block in blocks if block.get("bbox")), None)
        return {
            "title": section.get("title") or "document_prelude",
            "pages": pages,
            "bbox": first_bbox,
            "text": text,
            "preview": clip(text, 160),
            "exact_key": exact_key,
            "exact_hash": hash_text(exact_key),
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

            exact_key = compact_raw_text(text)
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
                    "bbox": next((block.get("bbox") for block in chunk if block.get("bbox")), None),
                    "text": text,
                    "preview": clip(text, 160),
                    "exact_key": exact_key,
                    "exact_hash": hash_text(exact_key),
                }
            )
        return sections

    # 商务标范围转译
    def _build_scoped_blocks_and_tables(
        self,
        segments: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """将查重段落转换为内部的区块和表格结构。"""
        from .business_scope import _normalize_scope_lines as scope_lines

        blocks: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []

        for index, segment in enumerate(segments, start=1):
            lines = scope_lines(
                segment.get("lines") or [],
                preserve_common_lines=bool(segment.get("preserve_common_lines")),
            )
            if not lines:
                continue

            pages = [page for page in (segment.get("pages") or []) if isinstance(page, int)] or [1]
            title = normalize_plain_text(segment.get("title") or "") or f"scope_{index}"
            heading_key = compact_raw_text(title)
            if heading_key:
                blocks.append(
                    {
                        "type": "heading",
                        "page": pages[0],
                        "bbox": normalize_bbox(segment.get("bbox")),
                        "text": title,
                        "exact_key": heading_key,
                        "exact_hash": hash_text(heading_key),
                    }
                )

            block_type = str(segment.get("kind") or "text")
            for line in lines:
                exact_key = compact_raw_text(line)
                if block_type != "heading" and len(exact_key) < 8:
                    continue
                blocks.append(
                    {
                        "type": block_type,
                        "page": pages[0],
                        "bbox": normalize_bbox(segment.get("bbox")),
                        "text": line,
                        "exact_key": exact_key,
                        "exact_hash": hash_text(exact_key),
                    }
                )

            table_text = "\n".join(lines).strip()
            if table_text:
                tables.append(
                    {
                        "pages": pages,
                        "bbox": normalize_bbox(segment.get("bbox")),
                        "text": table_text,
                        "rows": lines,
                        "exact_hash": hash_text(compact_raw_text(table_text)),
                    }
                )

        return blocks, tables

    # 模板上下文获取
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
        ordered_blocks, table_entries, _ = _extract_document_content_blocks(tender_payload, role=role)
        # 这三类模板上下文分别服务于星标要求、偏离表、分项报价表清洗。
        star_requirement_context = self._build_star_requirement_context(tender_payload)
        deviation_template_context = self._build_deviation_template_context(tender_payload)
        itemized_template_context = self._build_itemized_template_context(tender_payload)
        # 招标文件即使正文较弱，只要还能提到模板，就继续保留模板上下文。
        if (
            not ordered_blocks
            and not table_entries
            and not (star_requirement_context.get("items") or [])
            and not (deviation_template_context.get("requirement_items") or [])
            and not (deviation_template_context.get("line_items") or [])
            and not (itemized_template_context.get("line_items") or [])
        ):
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
            "placeholder_patterns": _build_template_placeholder_patterns(ordered_blocks),
            "star_requirement_context": star_requirement_context,
            "deviation_template_context": deviation_template_context,
            "itemized_template_context": itemized_template_context,
        }
        return cache[cache_key]

    def _build_star_requirement_context(self, tender_payload: dict[str, Any]) -> dict[str, Any]:
        """抽取招标文件中带★的要求，供偏离表查重时剔除要求列使用。"""
        requirements = self._deviation_checker._extract_star_requirements(tender_payload)
        return {
            "items": [
                {
                    "requirement_id": str(item.get("requirement_id") or "").strip(),
                    "requirement": str(item.get("requirement") or "").strip(),
                    "normalized_requirement": str(item.get("normalized_requirement") or "").strip(),
                    "fragments": list(item.get("fragments") or []),
                    "section_type": str(item.get("section_type") or "").strip(),
                }
                for item in requirements
                if str(item.get("normalized_requirement") or "").strip()
            ],
        }

    # ── 文档比较核心 ─────────────────────────────

    def _build_deviation_template_context(self, tender_payload: dict[str, Any]) -> dict[str, Any]:
        """从招标文件提取偏离表模板，用于剔除投标文件偏离表模板内容。"""
        sections = self._deviation_checker._extract_bid_deviation_sections(tender_payload)
        requirement_items: list[dict[str, Any]] = []
        line_items: list[dict[str, Any]] = []
        seen_requirement_norms: set[str] = set()
        seen_line_keys: set[str] = set()

        def _append_requirement(text: str) -> None:
            cleaned = normalize_plain_text(text)
            if not cleaned:
                return
            requirement = self._deviation_checker._clean_req(cleaned)
            normalized_requirement = self._deviation_checker._norm(requirement)
            if len(normalized_requirement) < 4 or normalized_requirement in seen_requirement_norms:
                return
            seen_requirement_norms.add(normalized_requirement)
            requirement_items.append(
                {
                    "text": requirement,
                    "normalized_requirement": normalized_requirement,
                    "fragments": self._deviation_checker._fragments(requirement),
                }
            )

        def _append_line(text: str) -> None:
            cleaned = normalize_plain_text(text)
            compact = compact_raw_text(cleaned)
            if len(compact) < 4 or compact in seen_line_keys:
                return
            seen_line_keys.add(compact)
            line_items.append(
                {
                    "text": cleaned,
                    "compact": compact,
                    "normalized": self._deviation_checker._norm(cleaned),
                    "fragments": self._deviation_checker._fragments(cleaned),
                }
            )

        for row in sections.get("rows") or []:
            if not isinstance(row, dict):
                continue
            # 同时保留“要求项”和“整行模板”，便于后续分别匹配。
            _append_requirement(str(row.get("requirement_text") or ""))
            _append_line(str(row.get("joined_text") or row.get("requirement_text") or ""))

        for section in (sections.get("business") or []) + (sections.get("technical") or []):
            if not isinstance(section, dict):
                continue
            for raw_line in section.get("lines") or []:
                _append_line(str(raw_line or ""))

        for raw_line in self._extract_deviation_template_lines(tender_payload):
            _append_line(raw_line)

        return {
            "requirement_items": requirement_items,
            "line_items": line_items,
            "line_keys": seen_line_keys,
        }

    def _extract_deviation_template_lines(self, tender_payload: dict[str, Any]) -> list[str]:
        """从招标文件中抽取偏离表模板行，不要求模板表已有真实响应数据。"""
        checker = self._deviation_checker
        parsed = checker._coerce_payload(tender_payload)
        doc = checker._doc_container(parsed)
        lines: list[str] = []

        def append_if_template(raw_line: Any) -> None:
            line = normalize_plain_text(raw_line)
            if not line:
                return
            if self._looks_like_deviation_template_line(line):
                lines.append(line)

        logical_tables = doc.get("logical_tables")
        if isinstance(logical_tables, list):
            for table in logical_tables:
                if not isinstance(table, dict):
                    continue
                headers = [
                    str(header or "").strip()
                    for header in (table.get("headers") or [])
                    if str(header or "").strip()
                ]
                if self._looks_like_deviation_template_headers(headers):
                    append_if_template(" ".join(headers))
                for line in checker._split_lines(checker._section_text(table)):
                    append_if_template(line)

        for key in ("layout_sections", "table_sections"):
            sections = doc.get(key)
            if not isinstance(sections, list):
                continue
            for section in sections:
                if not isinstance(section, dict):
                    continue
                for line in checker._split_lines(checker._section_text(section)):
                    append_if_template(line)

        source_text = self._extract_payload_text_for_template(parsed)
        for line in checker._split_lines(source_text):
            append_if_template(line)

        deduped: list[str] = []
        seen = set()
        for line in lines:
            key = compact_raw_text(line)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(line)
        return deduped

    def _looks_like_deviation_template_headers(self, headers: list[str]) -> bool:
        if len(headers) < 2:
            return False
        return self._looks_like_deviation_template_line(" ".join(headers))

    def _looks_like_deviation_template_line(self, line: str) -> bool:
        compact = compact_raw_text(line)
        if not compact:
            return False
        if re.fullmatch(
            r"(?:附件|附表)?[0-9一二三四五六七八九十]*"
            r"(?:商务|技术)?(?:条款)?(?:偏离表|响应表)(?:[（(]格式[）)])?",
            compact,
        ):
            return True

        has_requirement_axis = any(
            token in compact
            for token in (
                "招标文件",
                "采购文件",
                "招标需求",
                "招标要求",
                "指标要求",
                "技术要求",
                "商务要求",
                "要求",
                "条款",
            )
        )
        has_response_axis = any(
            token in compact
            for token in (
                "投标文件",
                "投标响应",
                "响应情况",
                "响应内容",
                "响应",
                "应答",
            )
        )
        has_deviation_axis = "偏离" in compact
        header_tokens = (
            "序号",
            "招标文件",
            "采购文件",
            "要求",
            "条款",
            "投标文件",
            "响应",
            "偏离",
            "说明",
            "所在页",
            "页码",
            "备注",
        )
        hits = sum(1 for token in header_tokens if token in compact)
        if has_requirement_axis and has_response_axis and has_deviation_axis and hits >= 3:
            return True
        if (
            compact.startswith("注")
            and has_response_axis
            and has_deviation_axis
            and ("招标文件" in compact or "采购文件" in compact)
        ):
            return True
        return False

    def _build_itemized_template_context(self, tender_payload: dict[str, Any]) -> dict[str, Any]:
        """从招标文件提取分项报价表模板，用于剔除投标文件分项报价表模板内容。"""
        document = self._itemized_checker._prepare_document(tender_payload)
        line_items: list[dict[str, Any]] = []
        seen_line_keys: set[str] = set()

        def _append_line(raw_line: Any) -> None:
            cleaned = normalize_plain_text(raw_line)
            compact = compact_raw_text(cleaned)
            if len(compact) < 4 or compact in seen_line_keys:
                return
            seen_line_keys.add(compact)
            line_items.append(
                {
                    "text": cleaned,
                    "compact": compact,
                }
            )

        for section in document.get("item_sections") or []:
            for raw_line in section.get("lines") or []:
                _append_line(raw_line)

        for raw_line in self._extract_itemized_template_lines(tender_payload):
            _append_line(raw_line)

        return {
            "line_items": line_items,
            "line_keys": seen_line_keys,
        }

    def _extract_itemized_template_lines(self, tender_payload: dict[str, Any]) -> list[str]:
        """从招标文件中宽松抽取分项报价表模板行，不要求模板表已有真实报价数据。"""
        checker = self._itemized_checker
        parsed = checker._parse_payload(tender_payload) or tender_payload
        lines: list[str] = []

        def append_if_template(raw_line: Any) -> None:
            line = normalize_plain_text(raw_line)
            if not line:
                return
            if self._looks_like_itemized_template_line(line):
                lines.append(line)

        for table in checker._get_logical_tables(parsed):
            headers = checker._get_logical_table_headers(table)
            if self._looks_like_itemized_template_headers(headers):
                append_if_template(" ".join(headers))
                for line in checker._logical_table_to_lines(table, include_headers=True):
                    append_if_template(line)

        for section in checker._find_layout_table_sections(parsed, checker.ITEM_SECTION_ANCHORS):
            for line in section.get("lines") or []:
                append_if_template(line)

        source_text = checker._normalize_text(self._extract_payload_text_for_template(parsed))
        text_lines = checker._split_lines(source_text)
        for section in checker._find_sections(
            text_lines,
            checker.ITEM_SECTION_ANCHORS,
            require_score=False,
        ):
            for line in section.get("lines") or []:
                append_if_template(line)

        deduped: list[str] = []
        seen = set()
        for line in lines:
            key = compact_raw_text(line)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(line)
        return deduped

    def _extract_payload_text_for_template(self, payload: Any) -> str:
        if isinstance(payload, str):
            return payload
        if not isinstance(payload, dict):
            return str(payload or "")
        container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        layout_sections = container.get("layout_sections")
        if isinstance(layout_sections, list):
            lines = [
                str(section.get("raw_text") or section.get("text") or "").strip()
                for section in layout_sections
                if isinstance(section, dict)
                and str(section.get("raw_text") or section.get("text") or "").strip()
            ]
            if lines:
                return "\n".join(lines)
        pages = container.get("pages")
        if isinstance(pages, list):
            lines = [
                str(page.get("raw_text") or page.get("text") or "").strip()
                for page in pages
                if isinstance(page, dict)
                and str(page.get("raw_text") or page.get("text") or "").strip()
            ]
            if lines:
                return "\n".join(lines)
        recognition = container.get("recognition")
        if isinstance(recognition, dict):
            for key in ("content", "raw_text", "text", "full_text"):
                value = recognition.get(key)
                if isinstance(value, str) and value.strip():
                    return value
        for key in ("content", "raw_text", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    def _looks_like_itemized_template_headers(self, headers: list[str]) -> bool:
        if len(headers) < 3:
            return False
        return self._looks_like_itemized_template_line(" ".join(headers))

    def _looks_like_itemized_template_line(self, line: str) -> bool:
        compact = compact_raw_text(line)
        if not compact:
            return False
        if self._itemized_checker._is_table_header_line(line):
            return True
        header_tokens = (
            "设备名称",
            "产品名称",
            "项目名称",
            "规格型号",
            "型号",
            "数量",
            "单位",
            "增值税税率",
            "发票税率",
            "含税单价",
            "单价",
            "含税总价",
            "总价",
            "合计",
            "金额",
            "备注",
        )
        hits = sum(1 for token in header_tokens if token in compact)
        has_price_axis = any(token in compact for token in ("单价", "总价", "合计", "金额"))
        has_quantity_axis = "数量" in compact or "单位" in compact
        if hits >= 4 and has_price_axis and has_quantity_axis:
            return True
        if re.fullmatch(r"(?:附件|附表)?\d*分项报价表(?:（格式）)?", compact):
            return True
        return False

    def _compare_documents(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        role: str,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """执行两个文档的完整比较（精确+相似度），返回结构化比较记录。"""
        # 检测与展示解耦：证据条目使用较大的安全上限，不被入参的小默认值截断；
        # 命中总量（exact_shared_count 等）始终统计全部，风险评分不受影响。
        evidence_limit = max(int(max_evidence_sections or 0), MAX_EVIDENCE_ITEMS_PER_PAIR)
        block_metrics = compare_blocks(left, right, max_evidence_sections=evidence_limit)
        section_metrics = compare_sections(left, right, max_evidence_sections=evidence_limit)
        table_metrics = compare_tables(left, right, max_evidence_sections=evidence_limit)
        image_metrics = compare_images(left, right, max_evidence_sections=evidence_limit)

        exact_duplicate = bool(left["exact_hash"] == right["exact_hash"])
        exact_match_score_val = exact_match_score(
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
        similar_match_score_val = 0.0
        if self._supports_similarity_matching(role):
            similar_block_metrics = compare_business_similarity_blocks(
                left, right, max_evidence_sections=evidence_limit,
            )
            similar_section_metrics = compare_business_similarity_sections(
                left, right, max_evidence_sections=evidence_limit,
            )
            similar_table_metrics = compare_business_similarity_tables(
                left, right, max_evidence_sections=evidence_limit,
            )
            similar_match_score_val = business_similarity_match_score(
                similar_block_overlap_ratio=float(similar_block_metrics["similar_overlap_ratio"]),
                similar_section_match_ratio=float(similar_section_metrics["similar_match_ratio"]),
                similar_table_match_ratio=float(similar_table_metrics["similar_match_ratio"]),
            )
            risk_level = business_risk_level(
                exact_duplicate=exact_duplicate,
                exact_match_score=exact_match_score_val,
                exact_block_count=int(block_metrics["exact_shared_count"]),
                exact_section_count=int(section_metrics["exact_match_count"]),
                exact_table_count=int(table_metrics["exact_match_count"]),
                exact_image_count=int(image_metrics["exact_match_count"]),
                exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
                similar_match_score=similar_match_score_val,
                similar_block_count=int(similar_block_metrics["similar_shared_count"]),
                similar_section_count=int(similar_section_metrics["similar_match_count"]),
                similar_table_count=int(similar_table_metrics["similar_match_count"]),
                similar_block_overlap_ratio=float(similar_block_metrics["similar_overlap_ratio"]),
                similar_section_overlap_ratio=float(similar_section_metrics["similar_match_ratio"]),
                similar_table_overlap_ratio=float(similar_table_metrics["similar_match_ratio"]),
            )
        else:
            risk_level = exact_risk_level(
                exact_duplicate=exact_duplicate,
                exact_match_score=exact_match_score_val,
                exact_block_count=int(block_metrics["exact_shared_count"]),
                exact_section_count=int(section_metrics["exact_match_count"]),
                exact_table_count=int(table_metrics["exact_match_count"]),
                exact_image_count=int(image_metrics["exact_match_count"]),
                exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
            )

        match_score = max(exact_match_score_val, similar_match_score_val)

        notes = []
        if not left["tables"] or not right["tables"]:
            notes.append("at_least_one_document_has_no_structured_table_content")
        if not left["sections"] or not right["sections"]:
            notes.append("at_least_one_document_has_no_stable_section_structure")
        if not left["images"] or not right["images"]:
            notes.append("at_least_one_document_has_no_extractable_image_content")

        issue = {
            "left_document_identifier": left["identifier_id"],
            "right_document_identifier": right["identifier_id"],
            "left_relation_id": left.get("relation_id"),
            "right_relation_id": right.get("relation_id"),
            "left_file_name": left.get("file_name"),
            "right_file_name": right.get("file_name"),
            "document_type": role,
            "exact_duplicate": exact_duplicate,
            "exact_match_score": round(exact_match_score_val, 4),
            "similarity_match_score": round(similar_match_score_val, 4),
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
        issue["locations"] = self._build_duplicate_issue_locations(issue)
        return issue

    def _filter_short_duplicate_evidence(self, issue: dict[str, Any]) -> dict[str, Any]:
        """Suppress short duplicate text unless it contains typos or identical business numbers."""
        text_evidence_keys = (
            "duplicate_blocks",
            "duplicate_sections",
            "duplicate_tables",
            "similar_blocks",
            "similar_sections",
            "similar_tables",
        )
        suppressed_count = 0
        kept_count = 0
        for evidence_key in text_evidence_keys:
            kept_items: list[dict[str, Any]] = []
            for evidence in issue.get(evidence_key) or []:
                if not isinstance(evidence, dict):
                    continue
                next_evidence = dict(evidence)
                decision = self._short_duplicate_report_decision(next_evidence)
                next_evidence["duplicate_text_length"] = decision["text_length"]
                next_evidence["duplicate_report_reason"] = decision["reason"]
                if decision.get("typo_issues"):
                    next_evidence["short_duplicate_typo_issues"] = decision["typo_issues"]
                if decision["report"]:
                    kept_items.append(next_evidence)
                    kept_count += 1
                else:
                    suppressed_count += 1
            issue[evidence_key] = kept_items

        # 汇总该对文档全部证据中的错别字，去重后挂到 issue 层，供合并/前端展示。
        aggregated_typo_issues: list[dict[str, Any]] = []
        seen_typo_keys: set[tuple[str, str]] = set()
        for evidence_key in text_evidence_keys:
            for evidence in issue.get(evidence_key) or []:
                for typo in evidence.get("short_duplicate_typo_issues") or []:
                    if not isinstance(typo, dict):
                        continue
                    key = (
                        str(typo.get("matched_text") or ""),
                        str(typo.get("suggestion") or ""),
                    )
                    if not key[0] or key in seen_typo_keys:
                        continue
                    seen_typo_keys.add(key)
                    aggregated_typo_issues.append(typo)
        issue["short_duplicate_typo_issues"] = aggregated_typo_issues

        issue["short_duplicate_filter"] = {
            "enabled": True,
            "threshold_chars": 30,
            "suppressed_evidence_count": suppressed_count,
            "reported_evidence_count": kept_count,
        }
        issue["locations"] = self._build_duplicate_issue_locations(issue)
        has_image_evidence = bool(issue.get("duplicate_images"))
        if kept_count == 0 and not has_image_evidence:
            issue["risk_level"] = "none"
            issue["suspicious"] = False
        return issue

    def _short_duplicate_report_decision(self, evidence: dict[str, Any]) -> dict[str, Any]:
        left_text, right_text = self._duplicate_evidence_pair_text(evidence)
        representative_text = left_text or right_text
        text_length = self._duplicate_text_length(representative_text)
        # 对所有重复证据文本（不限长度）检测错别字，只保留两边一致的错字。
        typo_issues = self._short_duplicate_typo_issues(evidence, left_text=left_text, right_text=right_text)
        if text_length >= 30:
            decision = {"report": True, "reason": "duplicate_text_at_least_30_chars", "text_length": text_length}
            if typo_issues:
                decision["typo_issues"] = typo_issues
            return decision

        if self._has_identical_non_serial_numbers(left_text, right_text):
            decision = {"report": True, "reason": "identical_non_serial_numbers", "text_length": text_length}
            if typo_issues:
                decision["typo_issues"] = typo_issues
            return decision

        if typo_issues:
            return {
                "report": True,
                "reason": "short_duplicate_contains_typo",
                "text_length": text_length,
                "typo_issues": typo_issues,
            }
        return {"report": False, "reason": "short_duplicate_without_typo", "text_length": text_length}

    def _duplicate_evidence_pair_text(self, evidence: dict[str, Any]) -> tuple[str, str]:
        left_candidates = (
            evidence.get("left_text"),
            evidence.get("left_preview"),
            evidence.get("left_rows"),
            evidence.get("left_sample_rows"),
            evidence.get("sample_rows"),
            evidence.get("text"),
        )
        right_candidates = (
            evidence.get("right_text"),
            evidence.get("right_preview"),
            evidence.get("right_rows"),
            evidence.get("right_sample_rows"),
            evidence.get("sample_rows"),
            evidence.get("text"),
        )
        return self._join_evidence_text(left_candidates), self._join_evidence_text(right_candidates)

    def _join_evidence_text(self, values: tuple[Any, ...]) -> str:
        parts: list[str] = []
        for value in values:
            if value in (None, "", []):
                continue
            if isinstance(value, list):
                parts.extend(str(item or "").strip() for item in value if str(item or "").strip())
            else:
                parts.append(str(value or "").strip())
        deduped: list[str] = []
        for part in parts:
            if part and part not in deduped:
                deduped.append(part)
        return "\n".join(deduped).strip()

    def _duplicate_text_length(self, text: str) -> int:
        compact = compact_raw_text(text)
        return len(compact)

    def _has_identical_non_serial_numbers(self, left_text: str, right_text: str) -> bool:
        left_numbers = self._non_serial_numbers(left_text)
        right_numbers = self._non_serial_numbers(right_text)
        return bool(left_numbers and left_numbers == right_numbers)

    def _non_serial_numbers(self, text: str) -> list[str]:
        normalized = normalize_plain_text(text)
        cleaned_lines: list[str] = []
        for line in normalized.splitlines() or [normalized]:
            line = re.sub(
                r"^\s*(?:[(（]?\d{1,4}[)）]?[.、:：]?|"
                r"\d+(?:\.\d+){1,5}[、.．)]?|"
                r"[一二三四五六七八九十百千]+[、.．])\s*",
                "",
                line,
            )
            line = re.sub(r"第\s*\d+\s*页", " ", line, flags=re.IGNORECASE)
            line = re.sub(r"(?:P|p)\s*\d+(?:\s*[-~]\s*(?:P|p)?\s*\d+)?", " ", line)
            cleaned_lines.append(line)
        cleaned = "\n".join(cleaned_lines)
        return re.findall(r"(?<![A-Za-z])\d+(?:[.,]\d+)?%?", cleaned)

    def _short_duplicate_typo_issues(
        self,
        evidence: dict[str, Any],
        *,
        left_text: str,
        right_text: str,
    ) -> list[dict[str, Any]]:
        left_issues = self._check_short_duplicate_side_typos("left", left_text, evidence)
        right_issues = self._check_short_duplicate_side_typos("right", right_text, evidence)
        left_signature = self._typo_issue_signature(left_issues)
        right_signature = self._typo_issue_signature(right_issues)
        if not left_signature or left_signature != right_signature:
            return []
        return left_issues + right_issues

    def _check_short_duplicate_side_typos(
        self,
        side: str,
        text: str,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if not str(text or "").strip():
            return []
        snippet = {
            "side": side,
            "text": text,
            "page": evidence.get(f"{side}_page"),
            "bbox": evidence.get(f"{side}_bbox"),
        }
        try:
            issues = self._get_short_duplicate_typo_service().check_text_snippets_for_typos([snippet])
        except Exception:
            return []
        normalized: list[dict[str, Any]] = []
        for issue in issues or []:
            if not isinstance(issue, dict):
                continue
            item = dict(issue)
            item.setdefault("side", side)
            normalized.append(item)
        return normalized

    def _typo_issue_signature(self, issues: list[dict[str, Any]]) -> tuple[tuple[str, str], ...]:
        signatures: list[tuple[str, str]] = []
        for issue in issues or []:
            matched = str(
                issue.get("matched_text") or
                issue.get("matchedText") or
                issue.get("wrong") or
                issue.get("error_word") or
                issue.get("source") or
                ""
            ).strip()
            suggestion = str(
                issue.get("suggestion") or
                issue.get("correct") or
                issue.get("correct_word") or
                issue.get("target") or
                ""
            ).strip()
            if matched:
                signatures.append((matched, suggestion))
        return tuple(sorted(set(signatures)))

    def _get_short_duplicate_typo_service(self) -> Any:
        if self._short_duplicate_typo_service is None:
            from app.service.analysis.bid_document_review import BidDocumentReviewService

            self._short_duplicate_typo_service = BidDocumentReviewService()
        return self._short_duplicate_typo_service

    def _build_duplicate_issue_locations(self, item: dict[str, Any]) -> list[dict[str, Any]]:
        locations: list[dict[str, Any]] = []
        evidence_groups = (
            "duplicate_blocks",
            "duplicate_sections",
            "duplicate_tables",
            "duplicate_images",
            "similar_blocks",
            "similar_sections",
            "similar_tables",
        )
        for group_key in evidence_groups:
            for evidence in item.get(group_key) or []:
                if not isinstance(evidence, dict):
                    continue
                for side in ("left", "right"):
                    self._append_duplicate_evidence_location(
                        locations,
                        item=item,
                        evidence=evidence,
                        side=side,
                    )
        return locations

    def _append_duplicate_evidence_location(
        self,
        locations: list[dict[str, Any]],
        *,
        item: dict[str, Any],
        evidence: dict[str, Any],
        side: str,
    ) -> None:
        pages = self._duplicate_evidence_pages(evidence, side) or [None]
        bbox = evidence.get(f"{side}_bbox") or evidence.get("bbox")
        text = self._duplicate_evidence_text(evidence, side)
        for index, page in enumerate(pages):
            append_location(
                locations,
                make_location(
                    document_identifier_id=item.get(f"{side}_document_identifier"),
                    file_name=item.get(f"{side}_file_name"),
                    page=page,
                    bbox=bbox if index == 0 else None,
                    text=text,
                ),
            )

    def _duplicate_evidence_pages(self, evidence: dict[str, Any], side: str) -> list[int]:
        pages: list[int] = []

        def collect(value: Any) -> None:
            if value is None or isinstance(value, bool):
                return
            if isinstance(value, int):
                if value > 0 and value not in pages:
                    pages.append(value)
                return
            if isinstance(value, float):
                if value.is_integer():
                    collect(int(value))
                return
            if isinstance(value, str):
                stripped = value.strip()
                if stripped.isdigit():
                    collect(int(stripped))
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    collect(item)

        collect(evidence.get(f"{side}_pages"))
        collect(evidence.get(f"{side}_page"))
        if not pages:
            collect(evidence.get("pages"))
            collect(evidence.get("page"))
        return sorted(pages)

    def _duplicate_evidence_text(self, evidence: dict[str, Any], side: str) -> str:
        for key in (
            f"{side}_text",
            f"{side}_preview",
            f"{side}_title",
            "text",
            "preview",
            "title",
        ):
            value = evidence.get(key)
            if value not in (None, "", []):
                return clip(str(value), 200)

        for key in (
            f"{side}_sample_rows",
            f"{side}_rows",
            "sample_rows",
            "rows",
        ):
            rows = evidence.get(key)
            if isinstance(rows, list) and rows:
                return clip(" | ".join(str(row) for row in rows[:3]), 200)

        image_hash = str(evidence.get("image_hash") or "").strip()
        return f"image:{image_hash[:16]}" if image_hash else ""

    def _supports_similarity_matching(self, role: str) -> bool:
        """当前仅商务标支持相似度匹配，技术标只保留精确查重。"""
        return role == DOCUMENT_TYPE_BUSINESS_BID

    def _resolve_detection_mode(
        self,
        *,
        requested_types: tuple[str, ...],
        enabled_similarity_roles: set[str],
    ) -> str:
        """根据当前请求的文档类型返回实际启用的查重模式。"""
        if not enabled_similarity_roles:
            return "exact_only"
        if len(enabled_similarity_roles) == len(requested_types):
            return "exact_plus_similarity"
        return "mixed_exact_plus_similarity"

    # ── 通用辅助 ─────────────────────────────────

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

    def _normalize_requested_types(self, document_types: list[str] | None) -> tuple[str, ...]:
        """标准化请求的文档类型列表，默认返回全部支持类型。"""
        if not document_types:
            return SUPPORTED_DOCUMENT_TYPES

        normalized: list[str] = []
        for item in document_types:
            role = self._normalize_document_role(item)
            if role not in SUPPORTED_DOCUMENT_TYPES:
                raise ValueError(f"Unsupported duplicate-check document type: {item}")
            if role not in normalized:
                normalized.append(role)
        return tuple(normalized) if normalized else SUPPORTED_DOCUMENT_TYPES

    def _normalize_document_role(self, value: Any) -> str:
        """将文档角色字符串标准化为内部常量。"""
        normalized = str(value or "").strip().lower()
        if normalized in {"business", "business_bid"}:
            return DOCUMENT_TYPE_BUSINESS_BID
        if normalized in {"technical", "technical_bid"}:
            return DOCUMENT_TYPE_TECHNICAL_BID
        return normalized

    def _pair_sort_key(self, item: dict[str, Any]) -> tuple[Any, ...]:
        """定义比较结果对（pair）的排序键，风险高的优先。"""
        metrics = item.get("metrics") or {}
        return (
            risk_rank(item.get("risk_level")),
            bool(item.get("exact_duplicate")),
            float(item.get("match_score") or item.get("exact_match_score") or 0.0),
            int(metrics.get("similar_table_count") or 0),
            int(metrics.get("similar_section_count") or 0),
            int(metrics.get("similar_block_count") or 0),
            int(metrics.get("exact_table_count") or 0),
            int(metrics.get("exact_section_count") or 0),
            int(metrics.get("exact_block_count") or 0),
        )
