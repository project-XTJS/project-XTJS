# -*- coding: utf-8 -*-
"""
Token 提取与聚类属性 Mixin
"""
import html
import json
import re
from typing import Any


class TokenExtractorMixin:
    """提供聚类 family/mode、token 提取、优先级等方法。"""

    helper: Any

    def _cluster_family(self, kind: str) -> str:
        """从证据种类中提取家族名称（去除 similar_ 前缀）。"""
        return kind[8:] if kind.startswith("similar_") else kind

    def _cluster_mode(self, kind: str) -> str:
        """根据证据种类前缀判断是精确匹配还是相似匹配。"""
        return "similar" if kind.startswith("similar_") else "exact"

    def _normalize_cluster_token(self, value: Any) -> str:
        """将证据中的文本或数据转换为可用于聚类的规范化 token。"""
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            text = json.dumps(value, ensure_ascii=False)
        else:
            text = str(value)
        text = html.unescape(text)
        text = re.sub(r"\d+(?:[\.,]\d+)?", "#", text)
        text = re.sub(r"""[，,。；;：:、\[\]\(\){}（）“”‘’"'`]+""", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        if len(text.replace("#", "").strip()) < 4:
            return ""
        return text

    def _occurrence_tokens(self, kind: str, evidence: dict[str, Any]) -> list[str]:
        """根据证据类型提取用于聚类的 token 列表。"""
        candidates: list[Any] = []
        if kind in {"section", "similar_section"}:
            candidates.extend(
                [
                    evidence.get("left_preview"),
                    evidence.get("left_title"),
                    evidence.get("right_preview"),
                    evidence.get("right_title"),
                ]
            )
        elif kind == "block":
            candidates.append(evidence.get("text"))
        elif kind == "similar_block":
            candidates.extend([evidence.get("left_text"), evidence.get("right_text"), evidence.get("text")])
        elif kind == "table":
            sample_rows = evidence.get("sample_rows") or []
            candidates.extend(sample_rows)
            candidates.extend([sample_rows, evidence.get("sample_text"), evidence.get("header_signature")])
        elif kind == "similar_table":
            left_rows = evidence.get("left_sample_rows") or []
            right_rows = evidence.get("right_sample_rows") or []
            candidates.extend(left_rows)
            candidates.extend(right_rows)
            candidates.extend(
                [
                    left_rows,
                    right_rows,
                    evidence.get("sample_rows"),
                    evidence.get("header_signature"),
                ]
            )
        elif kind == "image":
            candidates.extend(
                [
                    evidence.get("image_hash"),
                    [
                        evidence.get("left_width"),
                        evidence.get("left_height"),
                        evidence.get("right_width"),
                        evidence.get("right_height"),
                    ],
                ]
            )
        tokens: list[str] = []
        for candidate in candidates:
            token = self._normalize_cluster_token(candidate)
            if token and token not in tokens:
                tokens.append(token)
        return tokens

    def _cluster_rank(self, cluster: dict[str, Any]) -> int:
        """为聚类分配合并优先级数值（精确表格 > 精确段落 > ... > 相似句子）。"""
        mode = str(cluster.get("mode") or "similar")
        family = str(cluster.get("family") or "block")
        rank_map = {
            ("exact", "mixed"): 8,
            ("exact", "table"): 7,
            ("exact", "section"): 6,
            ("exact", "image"): 5,
            ("exact", "block"): 4,
            ("similar", "mixed"): 3,
            ("similar", "table"): 3,
            ("similar", "section"): 2,
            ("similar", "block"): 1,
        }
        return rank_map.get((mode, family), 0)
