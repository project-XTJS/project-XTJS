# -*- coding: utf-8 -*-
"""
风险评分与等级判定
"""
from typing import Any


def exact_match_score(
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


def exact_risk_level(
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


def business_similarity_match_score(
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


def business_risk_level(
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
    exact_risk = exact_risk_level(
        exact_duplicate=exact_duplicate,
        exact_match_score=exact_match_score,
        exact_block_count=exact_block_count,
        exact_section_count=exact_section_count,
        exact_table_count=exact_table_count,
        exact_image_count=exact_image_count,
        exact_block_overlap_ratio=exact_block_overlap_ratio,
    )
    if _risk_rank(exact_risk) >= _risk_rank("medium"):
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


def risk_rank(risk_level: Any) -> int:
    """将风险等级字符串转换为数值（用于排序）。"""
    mapping = {"high": 3, "medium": 2, "low": 1, "none": 0}
    return mapping.get(str(risk_level or "none"), 0)


def _risk_rank(risk_level: Any) -> int:
    """内部调用 risk_rank 的别名，避免循环引用。"""
    return risk_rank(risk_level)