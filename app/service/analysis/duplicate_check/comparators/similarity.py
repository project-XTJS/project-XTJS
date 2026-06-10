# -*- coding: utf-8 -*-
"""
相似度比较（商务标块/区段/表格）
"""
from typing import Any

from ..text_utils import (
    clip,
    business_similarity_key,
    similarity_ratio,
    normalize_plain_text,
    compact_raw_text,
)
from ..constants import (
    BUSINESS_BLOCK_SIMILARITY_THRESHOLD,
    BUSINESS_SECTION_SIMILARITY_THRESHOLD,
    BUSINESS_TABLE_SIMILARITY_THRESHOLD,
    BUSINESS_SIMILARITY_MIN_KEY_LENGTH,
)


def build_similarity_block_units(document: dict[str, Any]) -> list[dict[str, Any]]:
    """从文档中构建用于相似度比较的块单元。"""
    units: list[dict[str, Any]] = []
    for block in document.get("blocks") or []:
        if str(block.get("type") or "") == "heading":
            continue
        if _is_generic_deviation_similarity_text(block.get("text") or ""):
            continue
        similarity_key = business_similarity_key(block.get("text") or "")
        if len(compact_raw_text(similarity_key)) < BUSINESS_SIMILARITY_MIN_KEY_LENGTH:
            continue
        units.append(
            {
                "page": block.get("page"),
                "bbox": block.get("bbox"),
                "type": block.get("type"),
                "text": str(block.get("text") or ""),
                "exact_hash": str(block.get("exact_hash") or ""),
                "similarity_key": similarity_key,
            }
        )
    return units


def build_similarity_section_units(document: dict[str, Any]) -> list[dict[str, Any]]:
    """从文档中构建用于相似度比较的区段单元。"""
    units: list[dict[str, Any]] = []
    for section in document.get("sections") or []:
        section_text = str(section.get("text") or "")
        if _all_deviation_similarity_rows_generic(section_text.splitlines()):
            continue
        similarity_key = business_similarity_key(section.get("text") or "")
        if len(compact_raw_text(similarity_key)) < BUSINESS_SIMILARITY_MIN_KEY_LENGTH:
            continue
        units.append(
            {
                "title": str(section.get("title") or ""),
                "pages": list(section.get("pages") or []),
                "bbox": section.get("bbox"),
                "preview": str(section.get("preview") or ""),
                "text": str(section.get("text") or ""),
                "exact_hash": str(section.get("exact_hash") or ""),
                "similarity_key": similarity_key,
            }
        )
    return units


def build_similarity_table_units(document: dict[str, Any]) -> list[dict[str, Any]]:
    """从文档中构建用于相似度比较的表格单元。"""
    units: list[dict[str, Any]] = []
    for table in document.get("tables") or []:
        rows = _normalize_scope_lines(table.get("rows") or [])
        if not rows:
            continue
        similarity_rows = [
            business_similarity_key(row)
            for row in rows
            if business_similarity_key(row) and not _is_generic_deviation_similarity_text(row)
        ]
        similarity_rows = [
            row for row in similarity_rows
            if len(compact_raw_text(row)) >= BUSINESS_SIMILARITY_MIN_KEY_LENGTH
        ]
        if not similarity_rows:
            continue
        units.append(
            {
                "pages": list(table.get("pages") or []),
                "bbox": table.get("bbox"),
                "rows": rows,
                "exact_hash": str(table.get("exact_hash") or ""),
                "similarity_rows": similarity_rows,
                "similarity_key": "\n".join(similarity_rows),
            }
        )
    return units


def match_similarity_units(
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
            ratio = similarity_ratio(left_key, right_key)
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


def compare_business_similarity_blocks(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    max_evidence_sections: int,
) -> dict[str, Any]:
    """比较两个文档的块级别相似度。"""
    left_units = build_similarity_block_units(left)
    right_units = build_similarity_block_units(right)
    matches = match_similarity_units(
        left_units,
        right_units,
        threshold=BUSINESS_BLOCK_SIMILARITY_THRESHOLD,
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
                "left_bbox": left_unit.get("bbox"),
                "right_bbox": right_unit.get("bbox"),
                "type": "similar_sentence",
                "left_type": left_unit.get("type"),
                "right_type": right_unit.get("type"),
                "left_text": clip(left_unit.get("text") or "", 4000),
                "right_text": clip(right_unit.get("text") or "", 4000),
                "similarity": round(ratio, 4),
            }
        )
    return {
        "similar_overlap_ratio": overlap_ratio,
        "similar_shared_count": matched_count,
        "items": items,
    }


def compare_business_similarity_sections(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    max_evidence_sections: int,
) -> dict[str, Any]:
    """比较两个文档的区段级别相似度。"""
    left_units = build_similarity_section_units(left)
    right_units = build_similarity_section_units(right)
    matches = match_similarity_units(
        left_units,
        right_units,
        threshold=BUSINESS_SECTION_SIMILARITY_THRESHOLD,
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
                "left_bbox": left_unit.get("bbox"),
                "right_bbox": right_unit.get("bbox"),
                "exact": False,
                "similarity": round(ratio, 4),
                "left_preview": clip(left_unit.get("text") or left_unit.get("preview") or "", 4000),
                "right_preview": clip(right_unit.get("text") or right_unit.get("preview") or "", 4000),
            }
        )
    return {
        "similar_match_count": matched_count,
        "similar_match_ratio": overlap_ratio,
        "items": items,
    }


def _table_similarity_ratio(left_rows: list[str], right_rows: list[str]) -> float:
    """计算两个表格行列表的相似度（基于行匹配比例）。"""
    if not left_rows or not right_rows:
        return 0.0
    matches = match_similarity_units(
        [{"similarity_key": row, "exact_hash": row} for row in left_rows],
        [{"similarity_key": row, "exact_hash": row} for row in right_rows],
        threshold=BUSINESS_BLOCK_SIMILARITY_THRESHOLD,
        key_getter=lambda item: item.get("similarity_key"),
        exact_match_getter=None,
    )
    return len(matches) / max(1, min(len(left_rows), len(right_rows)))


def compare_business_similarity_tables(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    max_evidence_sections: int,
) -> dict[str, Any]:
    """比较两个文档的表格级别相似度。"""
    left_units = build_similarity_table_units(left)
    right_units = build_similarity_table_units(right)
    candidates: list[tuple[float, int, int]] = []
    for left_index, left_unit in enumerate(left_units):
        for right_index, right_unit in enumerate(right_units):
            if left_unit.get("exact_hash") == right_unit.get("exact_hash"):
                continue
            text_ratio = similarity_ratio(
                str(left_unit.get("similarity_key") or ""),
                str(right_unit.get("similarity_key") or ""),
            )
            row_ratio = _table_similarity_ratio(
                list(left_unit.get("similarity_rows") or []),
                list(right_unit.get("similarity_rows") or []),
            )
            score = max(text_ratio, row_ratio)
            if score >= BUSINESS_TABLE_SIMILARITY_THRESHOLD:
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
                "left_bbox": left_unit.get("bbox"),
                "right_bbox": right_unit.get("bbox"),
                "exact": False,
                "similarity": round(score, 4),
                "left_rows": [str(row) for row in list(left_unit.get("rows") or [])],
                "right_rows": [str(row) for row in list(right_unit.get("rows") or [])],
                "left_sample_rows": [str(row) for row in list(left_unit.get("rows") or [])],
                "right_sample_rows": [str(row) for row in list(right_unit.get("rows") or [])],
            }
        )
    return {
        "similar_match_count": matched_count,
        "similar_match_ratio": overlap_ratio,
        "items": items,
    }


def _normalize_scope_lines(
    values: list[Any],
    *,
    preserve_common_lines: bool = False,
) -> list[str]:
    """复用 business_scope 中的轻量级规范化，避免循环依赖。"""
    from ..business_scope import _normalize_scope_lines as impl
    return impl(values, preserve_common_lines=preserve_common_lines)


def _is_generic_deviation_similarity_text(value: Any) -> bool:
    """通用偏离响应只参与精确查重，不参与相似度查重。"""
    from ..business_scope import _is_generic_deviation_similarity_line as impl
    return impl(str(value or ""))


def _all_deviation_similarity_rows_generic(values: list[Any]) -> bool:
    """判断一个区段是否仅由通用偏离响应组成。"""
    rows = _normalize_scope_lines(values, preserve_common_lines=False)
    if not rows:
        return False
    return all(_is_generic_deviation_similarity_text(row) for row in rows)
