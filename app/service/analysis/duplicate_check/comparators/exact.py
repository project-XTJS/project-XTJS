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
    common_hashes = left["exact_block_hashes"] & right["exact_block_hashes"]
    overlap_ratio = _dice_ratio(left["exact_block_hashes"], right["exact_block_hashes"])

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
                "text": clip(left_block.get("text") or "", 160),
            }
        )
        if len(items) >= max_evidence_sections:
            break

    return {
        "exact_overlap_ratio": overlap_ratio,
        "exact_shared_count": len(common_hashes),
        "items": items,
    }


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
                "left_rows": [clip(row, 200) for row in list(left_table.get("rows", []) or [])],
                "right_rows": [clip(row, 200) for row in list(right_table.get("rows", []) or [])],
                "sample_rows": [clip(row, 160) for row in left_table.get("rows", [])[:3]],
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


def _dice_ratio(left: set[Any], right: set[Any]) -> float:
    """计算两个集合的 Dice 系数。"""
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    overlap = len(left & right)
    return (2.0 * overlap) / float(len(left) + len(right))