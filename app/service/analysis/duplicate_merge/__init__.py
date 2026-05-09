# -*- coding: utf-8 -*-
"""查重合并模块入口"""
from typing import Any

# 导出常量
from .constants import (
    MERGED_RESULT_KEY_BY_DOC_TYPE,
    RAW_RESULT_KEY_BY_DOC_TYPE,
    DOC_TYPE_BY_MERGED_RESULT_KEY,
    DOC_TYPES_BY_SOURCE_RESULT_KEY,
)

# 导出核心类
from .merger import DuplicateResultMerger


def build_duplicate_merge_results(
    *,
    raw_result: dict[str, Any],
    source_result_key: str,
    helper: Any | None = None,
) -> dict[str, dict[str, Any]]:
    """
    根据原始查重结果和源键名，为涉及的每一个文档类型生成合并后的聚类视图。
    返回 { merged_result_key: merge_payload } 的字典。
    """
    if helper is None:
        from app.service.analysis.visualizer import ReportVisualizer
        helper = ReportVisualizer()

    merger = DuplicateResultMerger(helper)
    available_groups = raw_result.get("groups") or {}
    doc_types = DOC_TYPES_BY_SOURCE_RESULT_KEY.get(source_result_key) or list(available_groups.keys())
    merged_results: dict[str, dict[str, Any]] = {}
    for doc_type in doc_types:
        merged_key = MERGED_RESULT_KEY_BY_DOC_TYPE.get(doc_type)
        if not merged_key:
            continue
        if doc_type not in available_groups:
            continue
        merged_results[merged_key] = merger.build_merge_payload(
            raw_result=raw_result,
            doc_type=doc_type,
            source_result_key=source_result_key,
        )
    return merged_results


__all__ = [
    "MERGED_RESULT_KEY_BY_DOC_TYPE",
    "RAW_RESULT_KEY_BY_DOC_TYPE",
    "DOC_TYPE_BY_MERGED_RESULT_KEY",
    "DOC_TYPES_BY_SOURCE_RESULT_KEY",
    "DuplicateResultMerger",
    "build_duplicate_merge_results",
]