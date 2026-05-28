# -*- coding: utf-8 -*-
"""
精确比较（块/区段/表格/图像）
"""
from typing import Any

from ..text_utils import clip


def compare_blocks(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    max_evidence_sections: int,
) -> dict[str, Any]:
    """基于句子哈希精确比较两个文档的区块重复度。"""
    common_hashes = set(left["exact_block_hashes"]) & set(right["exact_block_hashes"])
    overlap_ratio = _dice_ratio(left["exact_block_hashes"], right["exact_block_hashes"])

    items = []
    pairs = _ordered_block_pairs(left, right, common_hashes)
    for run in _merge_adjacent_block_pairs(pairs):
        item = _build_block_run_item(run)
        if not item:
            continue
        items.append(item)
        if len(items) >= max_evidence_sections:
            break

    return {
        "exact_overlap_ratio": overlap_ratio,
        "exact_shared_count": len(common_hashes),
        "items": items,
    }


def _ordered_block_pairs(
    left: dict[str, Any],
    right: dict[str, Any],
    common_hashes: set[Any],
) -> list[tuple[dict[str, Any], dict[str, Any], Any]]:
    left_occurrences = _block_occurrences_by_hash(left)
    right_occurrences = _block_occurrences_by_hash(right)
    pairs: list[tuple[dict[str, Any], dict[str, Any], Any]] = []
    for block_hash in common_hashes:
        left_units = left_occurrences.get(block_hash) or []
        right_units = right_occurrences.get(block_hash) or []
        for index in range(min(len(left_units), len(right_units))):
            pairs.append((left_units[index], right_units[index], block_hash))
    pairs.sort(
        key=lambda pair: (
            _unit_sequence(pair[0]),
            _unit_sequence(pair[1]),
            str(pair[2]),
        )
    )
    return pairs


def _block_occurrences_by_hash(document: dict[str, Any]) -> dict[Any, list[dict[str, Any]]]:
    occurrences = document.get("exact_block_occurrence_map")
    if isinstance(occurrences, dict) and occurrences:
        return {
            block_hash: [item for item in items if isinstance(item, dict)]
            for block_hash, items in occurrences.items()
            if isinstance(items, list)
        }

    fallback: dict[Any, list[dict[str, Any]]] = {}
    for block_hash, item in (document.get("exact_block_map") or {}).items():
        if isinstance(item, dict):
            fallback[block_hash] = [item]
    return fallback


def _merge_adjacent_block_pairs(
    pairs: list[tuple[dict[str, Any], dict[str, Any], Any]],
) -> list[list[tuple[dict[str, Any], dict[str, Any], Any]]]:
    runs: list[list[tuple[dict[str, Any], dict[str, Any], Any]]] = []
    for pair in pairs:
        if runs and _block_pairs_are_adjacent(runs[-1][-1], pair):
            runs[-1].append(pair)
        else:
            runs.append([pair])
    return runs


def _block_pairs_are_adjacent(
    previous: tuple[dict[str, Any], dict[str, Any], Any],
    current: tuple[dict[str, Any], dict[str, Any], Any],
) -> bool:
    previous_left, previous_right, _ = previous
    current_left, current_right, _ = current
    return (
        _unit_sequence(previous_left) + 1 == _unit_sequence(current_left)
        and _unit_sequence(previous_right) + 1 == _unit_sequence(current_right)
    )


def _build_block_run_item(
    run: list[tuple[dict[str, Any], dict[str, Any], Any]],
) -> dict[str, Any] | None:
    if not run:
        return None
    left_units = [pair[0] for pair in run]
    right_units = [pair[1] for pair in run]
    first_left = left_units[0]
    first_right = right_units[0]
    left_text = _join_unit_text(left_units)
    right_text = _join_unit_text(right_units)
    sentence_count = len(run)
    left_pages = _ordered_pages(left_units)
    right_pages = _ordered_pages(right_units)
    return {
        "page": first_left.get("page"),
        "left_page": first_left.get("page"),
        "right_page": first_right.get("page"),
        "left_pages": left_pages,
        "right_pages": right_pages,
        "left_bbox": first_left.get("bbox"),
        "right_bbox": first_right.get("bbox"),
        "type": "sentence_sequence" if sentence_count > 1 else "sentence",
        "left_type": first_left.get("type"),
        "right_type": first_right.get("type"),
        "sentence_count": sentence_count,
        "left_start_index": first_left.get("sequence"),
        "right_start_index": first_right.get("sequence"),
        "text": clip(left_text, 200),
        "left_text": clip(left_text, 200),
        "right_text": clip(right_text, 200),
    }


def _join_unit_text(units: list[dict[str, Any]]) -> str:
    return " ".join(str(unit.get("text") or "").strip() for unit in units if str(unit.get("text") or "").strip())


def _ordered_pages(units: list[dict[str, Any]]) -> list[int]:
    pages: list[int] = []
    for unit in units:
        page = unit.get("page")
        if isinstance(page, int) and page > 0 and page not in pages:
            pages.append(page)
    return pages


def _unit_sequence(unit: dict[str, Any]) -> int:
    try:
        return int(unit.get("sequence"))
    except (TypeError, ValueError):
        return 10**9


def compare_sections(
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
    for section_hash in sorted(
        common_hashes,
        key=lambda value: _section_order_key(
            left["exact_section_map"].get(value),
            right["exact_section_map"].get(value),
            value,
        ),
    ):
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
                "left_bbox": left_section.get("bbox"),
                "right_bbox": right_section.get("bbox"),
                "exact": True,
                "left_preview": clip(left_section.get("text") or left_section.get("preview") or "", 200),
                "right_preview": clip(right_section.get("text") or right_section.get("preview") or "", 200),
            }
        )
        if len(items) >= max_evidence_sections:
            break

    return {
        "exact_match_count": len(common_hashes),
        "exact_match_ratio": exact_match_ratio,
        "items": items,
    }


def compare_tables(
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
    for table_hash in sorted(
        common_hashes,
        key=lambda value: _page_order_key(
            left["exact_table_map"].get(value),
            right["exact_table_map"].get(value),
            value,
        ),
    ):
        left_table = left["exact_table_map"].get(table_hash)
        right_table = right["exact_table_map"].get(table_hash)
        if not left_table or not right_table:
            continue
        items.append(
            {
                "left_pages": left_table.get("pages", []),
                "right_pages": right_table.get("pages", []),
                "left_bbox": left_table.get("bbox"),
                "right_bbox": right_table.get("bbox"),
                "exact": True,
                "left_rows": [str(row) for row in list(left_table.get("rows", []) or [])],
                "right_rows": [str(row) for row in list(right_table.get("rows", []) or [])],
                "sample_rows": [str(row) for row in list(left_table.get("rows", []) or [])],
            }
        )
        if len(items) >= max_evidence_sections:
            break

    return {
        "exact_match_count": len(common_hashes),
        "exact_match_ratio": exact_match_ratio,
        "items": items,
    }


def compare_images(
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
    for image_hash in sorted(
        common_hashes,
        key=lambda value: _page_order_key(
            left["exact_image_map"].get(value),
            right["exact_image_map"].get(value),
            value,
        ),
    ):
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


def _dice_ratio(left: set[Any], right: set[Any]) -> float:
    """计算两个集合的 Dice 系数。"""
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    overlap = len(left & right)
    return (2.0 * overlap) / float(len(left) + len(right))


def _section_order_key(left_item: dict[str, Any] | None, right_item: dict[str, Any] | None, fallback: Any) -> tuple[Any, ...]:
    return _page_order_key(left_item, right_item, fallback)


def _page_order_key(left_item: dict[str, Any] | None, right_item: dict[str, Any] | None, fallback: Any) -> tuple[Any, ...]:
    return (
        _first_page(left_item),
        _first_page(right_item),
        str(fallback),
    )


def _first_page(item: dict[str, Any] | None) -> int:
    if not isinstance(item, dict):
        return 10**9
    pages = item.get("pages")
    if isinstance(pages, list):
        for page in pages:
            if isinstance(page, int) and page > 0:
                return page
    page = item.get("page")
    if isinstance(page, int) and page > 0:
        return page
    return 10**9
