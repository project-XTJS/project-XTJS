# -*- coding: utf-8 -*-
"""
文档查重服务门面（组合所有子模块，对外提供统一接口）
"""
import json
from itertools import combinations
from typing import Any

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID
from app.service.minio_service import MinioService
from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.compliance.template_extractor import SectionClassifier

from .constants import (
    SUPPORTED_DOCUMENT_TYPES,
    BUSINESS_SCOPE_SKIP_REASON,
    TEMPLATE_EXCLUDED_SKIP_REASON,
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
    ) -> tuple[dict[str, Any] | None, str | None]:
        """对单条文档记录提取内容，排除模板，返回可用于比较的结构化对象。"""
        payload = self._coerce_payload(record.get("content"))
        if role == DOCUMENT_TYPE_BUSINESS_BID:
            # 商务标特殊处理
            scoped_segments = extract_business_duplicate_segments(
                payload,
                itemized_checker=self._itemized_checker,
                deviation_checker=self._deviation_checker,
                star_requirement_context=(
                    template_context.get("star_requirement_context") if template_context else None
                ),
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
            "exact_hash": hash_text(exact_hash_source),
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
        sentence_map: dict[str, dict[str, Any]] = {}
        for block in ordered_blocks:
            if block.get("type") == "heading":
                continue
            for sentence in self._sentence_units_from_block(block):
                sentence_map.setdefault(sentence["exact_hash"], sentence)
        return sentence_map

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
        return {
            "title": section.get("title") or "document_prelude",
            "pages": sorted(page for page in section.get("pages", set()) if isinstance(page, int)),
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
            "placeholder_patterns": _build_template_placeholder_patterns(ordered_blocks),
            "star_requirement_context": self._build_star_requirement_context(tender_payload),
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

    def _compare_documents(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        role: str,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        """执行两个文档的完整比较（精确+相似度），返回结构化比较记录。"""
        block_metrics = compare_blocks(left, right, max_evidence_sections=max_evidence_sections)
        section_metrics = compare_sections(left, right, max_evidence_sections=max_evidence_sections)
        table_metrics = compare_tables(left, right, max_evidence_sections=max_evidence_sections)
        image_metrics = compare_images(left, right, max_evidence_sections=max_evidence_sections)

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
                left, right, max_evidence_sections=max_evidence_sections,
            )
            similar_section_metrics = compare_business_similarity_sections(
                left, right, max_evidence_sections=max_evidence_sections,
            )
            similar_table_metrics = compare_business_similarity_tables(
                left, right, max_evidence_sections=max_evidence_sections,
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

        return {
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
