# -*- coding: utf-8 -*-
"""
查重结果合并模块。

负责将原始查重结果中的证据项按文本相似度和页面范围进行聚类，
生成便于前端展示的合并后的聚类视图。
"""

from __future__ import annotations

import html
import json
import re
from typing import Any, Optional

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)


# 按文档类型映射到聚类结果键名
MERGED_RESULT_KEY_BY_DOC_TYPE = {
    DOCUMENT_TYPE_BUSINESS_BID: "business_bid_duplicate_clusters",
    DOCUMENT_TYPE_TECHNICAL_BID: "technical_bid_duplicate_clusters",
}

# 按文档类型映射到原始查重结果键名
RAW_RESULT_KEY_BY_DOC_TYPE = {
    DOCUMENT_TYPE_BUSINESS_BID: "business_bid_duplicate_check",
    DOCUMENT_TYPE_TECHNICAL_BID: "technical_bid_duplicate_check",
}

# 逆映射：由聚类结果键名反向获取文档类型
DOC_TYPE_BY_MERGED_RESULT_KEY = {
    value: key for key, value in MERGED_RESULT_KEY_BY_DOC_TYPE.items()
}

# 按源结果键名获取对应的文档类型列表（用于处理合并后的查重结果）
DOC_TYPES_BY_SOURCE_RESULT_KEY = {
    "duplicate_check": [DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID],
    "business_bid_duplicate_check": [DOCUMENT_TYPE_BUSINESS_BID],
    "technical_bid_duplicate_check": [DOCUMENT_TYPE_TECHNICAL_BID],
}


class DuplicateResultMerger:
    """查重结果聚类合并器，将多个比较对中发现的重叠证据聚类为分组。"""

    def __init__(self, helper: Any) -> None:
        """helper 需提供 _coalesce_page_ranges、_project_normalize_pages 等辅助方法。"""
        self.helper = helper

    # ── 页面范围分割 ─────────────────────────────

    def _split_occurrence_ranges(
        self,
        left_pages: Any,
        right_pages: Any,
    ) -> list[tuple[list[int], list[int]]]:
        """
        将左右页面的范围列表拆分为多组 (左页列表, 右页列表)，
        用于将跨页的证据爆炸为多个单页或短范围条目。
        """
        left_ranges = self.helper._coalesce_page_ranges(
            self.helper._project_normalize_pages(left_pages)
        )
        right_ranges = self.helper._coalesce_page_ranges(
            self.helper._project_normalize_pages(right_pages)
        )
        if not left_ranges and not right_ranges:
            return [([], [])]
        if not left_ranges:
            return [([], list(range(start, end + 1))) for start, end in right_ranges]
        if not right_ranges:
            return [(list(range(start, end + 1)), []) for start, end in left_ranges]
        if len(left_ranges) == len(right_ranges):
            return [
                (list(range(left_start, left_end + 1)), list(range(right_start, right_end + 1)))
                for (left_start, left_end), (right_start, right_end) in zip(
                    left_ranges,
                    right_ranges,
                )
            ]
        if len(left_ranges) == 1:
            left_start, left_end = left_ranges[0]
            left_span = list(range(left_start, left_end + 1))
            return [
                (left_span, list(range(right_start, right_end + 1)))
                for right_start, right_end in right_ranges
            ]
        if len(right_ranges) == 1:
            right_start, right_end = right_ranges[0]
            right_span = list(range(right_start, right_end + 1))
            return [
                (list(range(left_start, left_end + 1)), right_span)
                for left_start, left_end in left_ranges
            ]
        pair_count = min(len(left_ranges), len(right_ranges))
        return [
            (
                list(range(left_ranges[index][0], left_ranges[index][1] + 1)),
                list(range(right_ranges[index][0], right_ranges[index][1] + 1)),
            )
            for index in range(pair_count)
        ]

    # ── 证据爆炸 ─────────────────────────────────

    def _explode_evidence_occurrences(self, evidence: Any, kind: str) -> list[dict[str, Any]]:
        """
        将一条包含页面范围的证据拆分为多个独立条目，
        每个条目仅对应一组具体的左右页码，以便后续聚类。
        """
        if not isinstance(evidence, dict):
            return [evidence]
        if kind in {"block", "similar_block"}:
            left_pages = self.helper._project_normalize_pages(
                evidence.get("left_pages"),
                evidence.get("left_page"),
            )
            right_pages = self.helper._project_normalize_pages(
                evidence.get("right_pages"),
                evidence.get("right_page"),
            )
            if not left_pages:
                left_pages = self.helper._project_normalize_pages(evidence.get("page"))
            if not right_pages:
                right_pages = self.helper._project_normalize_pages(evidence.get("page"))
            pairs = self._split_occurrence_ranges(left_pages, right_pages)
            exploded: list[dict[str, Any]] = []
            for left_pair, right_pair in pairs:
                entry = dict(evidence)
                entry["page"] = None
                entry["pages"] = None
                entry["left_page"] = left_pair[0] if left_pair else None
                entry["right_page"] = right_pair[0] if right_pair else None
                entry["left_pages"] = left_pair
                entry["right_pages"] = right_pair
                exploded.append(entry)
            return exploded or [evidence]

        left_pages = self.helper._project_normalize_pages(evidence.get("left_pages"))
        right_pages = self.helper._project_normalize_pages(evidence.get("right_pages"))
        pairs = self._split_occurrence_ranges(left_pages, right_pages)
        exploded: list[dict[str, Any]] = []
        for left_pair, right_pair in pairs:
            entry = dict(evidence)
            entry["left_pages"] = left_pair
            entry["right_pages"] = right_pair
            exploded.append(entry)
        return exploded or [evidence]

    # ── 聚类属性提取 ─────────────────────────────

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
            ("exact", "table"): 7,
            ("exact", "section"): 6,
            ("exact", "image"): 5,
            ("exact", "block"): 4,
            ("similar", "table"): 3,
            ("similar", "section"): 2,
            ("similar", "block"): 1,
        }
        return rank_map.get((mode, family), 0)

    # ── 嵌套关系判断 ─────────────────────────────

    def _ranges_cover(
        self,
        container_ranges: list[tuple[int, int]],
        candidate_ranges: list[tuple[int, int]],
        *,
        tolerance: int = 0,
    ) -> bool:
        """检查 container 的页面范围是否完全覆盖 candidate 的范围。"""
        if not container_ranges or not candidate_ranges:
            return False
        for candidate_start, candidate_end in candidate_ranges:
            matched = False
            for container_start, container_end in container_ranges:
                if (
                    container_start - tolerance <= candidate_start
                    and container_end + tolerance >= candidate_end
                ):
                    matched = True
                    break
            if not matched:
                return False
        return True

    def _clusters_are_textually_related(
        self,
        strong: dict[str, Any],
        weak: dict[str, Any],
    ) -> bool:
        """判断两个聚类的 token 是否有交集或包含关系。"""
        strong_tokens = [token for token in (strong.get("tokens") or []) if token]
        weak_tokens = [token for token in (weak.get("tokens") or []) if token]
        if not strong_tokens or not weak_tokens:
            return False
        if set(strong_tokens) & set(weak_tokens):
            return True
        for strong_token in strong_tokens:
            for weak_token in weak_tokens:
                if len(strong_token) >= len(weak_token):
                    if weak_token and weak_token in strong_token:
                        return True
                elif strong_token and strong_token in weak_token:
                    return True
        return False

    def _clusters_have_nested_ranges(
        self,
        strong: dict[str, Any],
        weak: dict[str, Any],
    ) -> bool:
        """检查弱聚类的页面范围是否完全被强聚类覆盖。"""
        strong_family = str(strong.get("family") or "block")
        weak_family = str(weak.get("family") or "block")
        common_files = set(strong.get("files") or []) & set(weak.get("files") or [])
        if len(common_files) < 2:
            return False
        tolerance = 1 if strong_family == "section" and weak_family == "block" else 0
        for file_name in common_files:
            strong_ranges = strong.get("doc_ranges_by_file", {}).get(file_name) or []
            weak_ranges = weak.get("doc_ranges_by_file", {}).get(file_name) or []
            if not self._ranges_cover(strong_ranges, weak_ranges, tolerance=tolerance):
                return False
        return True

    # ── 展示文本与聚类标题 ─────────────────────────

    def _occurrence_preview(self, kind: str, evidence: dict[str, Any], side: str) -> str:
        """根据证据类型和左右侧提取用于展示的预览文本。"""
        if kind in {"section", "similar_section"}:
            if side == "left":
                raw = evidence.get("left_preview") or evidence.get("left_title") or evidence.get("preview") or "-"
            else:
                raw = evidence.get("right_preview") or evidence.get("right_title") or evidence.get("preview") or "-"
            return self.helper._project_trim_text(str(raw), 220)
        if kind == "block":
            return self.helper._project_trim_text(str(evidence.get("text") or "-"), 220)
        if kind == "similar_block":
            raw = evidence.get(f"{side}_text") or evidence.get("text") or "-"
            return self.helper._project_trim_text(str(raw), 220)
        if kind == "table":
            rows = evidence.get("sample_rows") or []
            return self.helper._project_trim_text(json.dumps(rows, ensure_ascii=False), 220) if rows else "-"
        if kind == "similar_table":
            rows = evidence.get(f"{side}_sample_rows") or evidence.get("sample_rows") or []
            return self.helper._project_trim_text(json.dumps(rows, ensure_ascii=False), 220) if rows else "-"
        if kind == "image":
            width = evidence.get(f"{side}_width")
            height = evidence.get(f"{side}_height")
            dimensions = ""
            if isinstance(width, int) and isinstance(height, int) and width > 0 and height > 0:
                dimensions = f"{width}x{height}"
            pages = self.helper._project_normalize_pages(
                evidence.get(f"{side}_pages"),
                evidence.get(f"{side}_page"),
                evidence.get("page"),
            )
            page_label = ""
            if pages:
                coalesced = self.helper._coalesce_page_ranges(pages)
                if coalesced:
                    start_page, end_page = coalesced[0]
                    page_label = f"第{start_page}页" if start_page == end_page else f"第{start_page}-{end_page}页"
            parts = [part for part in (page_label, dimensions, "相同图片") if part]
            return " / ".join(parts) if parts else "相同图片"
        return "-"

    def _cluster_title(self, cluster: dict[str, Any]) -> str:
        """生成聚类的展示标题，包含类型前缀和预览文本。"""
        family = str(cluster.get("family") or "block")
        mode = str(cluster.get("mode") or "exact")
        title_map = {
            ("exact", "section"): "重复段落",
            ("exact", "block"): "重复句子",
            ("exact", "table"): "重复表格",
            ("exact", "image"): "重复图片",
            ("similar", "section"): "相似段落",
            ("similar", "block"): "相似句子",
            ("similar", "table"): "相似表格",
        }
        prefix = title_map.get((mode, family), "重复证据")
        previews_by_file = cluster.get("doc_previews_by_file") or {}
        preview_text = ""
        for file_name in cluster.get("files") or []:
            values = previews_by_file.get(file_name) or []
            if values:
                preview_text = str(values[0]).strip()
                if preview_text:
                    break
        preview_text = self.helper._project_trim_text(preview_text, 42).strip(" -") if preview_text else ""
        return f"{prefix}：{preview_text}" if preview_text else prefix

    def _cluster_id(self, doc_type: str, cluster: dict[str, Any]) -> str:
        """生成聚类的唯一标识符。"""
        files = cluster.get("files") or []
        parts = [doc_type, cluster.get("family"), cluster.get("mode"), *files]
        for file_name in files:
            for start_page, end_page in cluster.get("doc_ranges_by_file", {}).get(file_name, []) or []:
                parts.append(f"{file_name}:{start_page}-{end_page}")
        for token in cluster.get("tokens") or []:
            parts.append(str(token)[:48])
        return f"duplicate-cluster-{doc_type}-{self.helper._project_make_stable_token(*parts)}"

    # ── 主聚类方法 ───────────────────────────────

    def cluster_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        将查重比较对中的各类证据（精确/相似，段落/句子/表格/图片）
        按文本 token 的共有性聚合为聚类，并消除嵌套子集。
        """
        # 定义需要处理的证据组键名和对应的种类
        evidence_groups = (
            ("duplicate_sections", "section"),
            ("duplicate_blocks", "block"),
            ("duplicate_tables", "table"),
            ("duplicate_images", "image"),
            ("similar_sections", "similar_section"),
            ("similar_blocks", "similar_block"),
            ("similar_tables", "similar_table"),
        )
        occurrences: list[dict[str, Any]] = []
        for raw_item in items:
            item = self.helper._project_filter_duplicate_item_evidence(raw_item)
            left_file = str(item.get("left_file_name") or "").strip()
            right_file = str(item.get("right_file_name") or "").strip()
            if not left_file or not right_file:
                continue
            raw_score = item.get("match_score")
            if raw_score is None:
                raw_score = item.get("exact_match_score")
            try:
                score_value = float(raw_score or 0)
            except (TypeError, ValueError):
                score_value = 0.0
            for group_key, kind in evidence_groups:
                for evidence in item.get(group_key) or []:
                    for occurrence in self._explode_evidence_occurrences(evidence, kind):
                        docs: dict[str, dict[str, Any]] = {}
                        for side, file_name in (("left", left_file), ("right", right_file)):
                            pages = self.helper._project_evidence_pages(occurrence, side)
                            docs[file_name] = {
                                "pages": self.helper._project_normalize_pages(pages),
                                "preview": self._occurrence_preview(kind, occurrence, side),
                            }
                        tokens = self._occurrence_tokens(kind, occurrence)
                        if not tokens:
                            tokens = [
                                self.helper._project_make_stable_token(
                                    kind,
                                    left_file,
                                    right_file,
                                    json.dumps(docs, ensure_ascii=False),
                                )
                            ]
                        occurrences.append(
                            {
                                "kind": kind,
                                "family": self._cluster_family(kind),
                                "mode": self._cluster_mode(kind),
                                "risk_level": str(item.get("risk_level") or "none"),
                                "score_value": score_value,
                                "score_display": self.helper._project_duplicate_score(item),
                                "similarity": occurrence.get("similarity"),
                                "tokens": tokens,
                                "docs": docs,
                                "item": item,
                                "evidence": occurrence,
                            }
                        )

        if not occurrences:
            return []

        # 并查集：按 token 共有性将 occurrences 合并到同一聚类
        parent = list(range(len(occurrences)))

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(left: int, right: int) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        token_owner: dict[tuple[str, str], int] = {}
        for index, occurrence in enumerate(occurrences):
            for token in occurrence.get("tokens") or []:
                key = (str(occurrence.get("family") or "block"), token)
                owner = token_owner.get(key)
                if owner is None:
                    token_owner[key] = index
                else:
                    union(index, owner)

        # 按根节点分组
        grouped: dict[int, list[dict[str, Any]]] = {}
        for index, occurrence in enumerate(occurrences):
            grouped.setdefault(find(index), []).append(occurrence)

        # 为每个分组构建聚类
        clusters: list[dict[str, Any]] = []
        for members in grouped.values():
            # 若包含精确匹配，则仅使用精确匹配成员作为聚类核心
            has_exact = any(member.get("mode") == "exact" for member in members)
            active_mode = "exact" if has_exact else "similar"
            active_members = [member for member in members if member.get("mode") == active_mode]
            if not active_members:
                continue

            files: list[str] = []
            doc_ranges_by_file: dict[str, list[tuple[int, int]]] = {}
            doc_previews_by_file: dict[str, list[str]] = {}
            for member in active_members:
                for file_name, doc in (member.get("docs") or {}).items():
                    if file_name not in files:
                        files.append(file_name)
                    target_ranges = doc_ranges_by_file.setdefault(file_name, [])
                    for start_page, end_page in self.helper._coalesce_page_ranges(
                        self.helper._project_normalize_pages(doc.get("pages"))
                    ):
                        pair = (start_page, end_page)
                        if pair not in target_ranges:
                            target_ranges.append(pair)
                    preview_text = str(doc.get("preview") or "").strip()
                    if preview_text:
                        previews = doc_previews_by_file.setdefault(file_name, [])
                        if preview_text not in previews:
                            previews.append(preview_text)

            family = str(active_members[0].get("family") or "block")
            metrics = {
                "exact_section_count": 0,
                "similar_section_count": 0,
                "exact_block_count": 0,
                "similar_block_count": 0,
                "exact_table_count": 0,
                "similar_table_count": 0,
                "exact_image_count": 0,
                "similar_image_count": 0,
            }
            metric_key = f"{active_mode}_{family}_count"
            if metric_key in metrics:
                metrics[metric_key] = 1

            # 选取风险最高/分数最高的成员作为聚类代表
            best_member = max(
                active_members,
                key=lambda member: (
                    self.helper._project_duplicate_risk_rank(str(member.get("risk_level") or "none")),
                    float(member.get("score_value") or 0),
                ),
            )
            cluster_tokens: list[str] = []
            for member in active_members:
                for token in member.get("tokens") or []:
                    if token not in cluster_tokens:
                        cluster_tokens.append(token)

            cluster = {
                "files": files,
                "family": family,
                "mode": active_mode,
                "items": [member.get("item") for member in active_members],
                "occurrences": active_members,
                "doc_ranges_by_file": doc_ranges_by_file,
                "doc_previews_by_file": doc_previews_by_file,
                "metrics": metrics,
                "risk_level": str(best_member.get("risk_level") or "none"),
                "score_display": str(best_member.get("score_display") or "0"),
                "score_value": float(best_member.get("score_value") or 0),
                "similarity": max(
                    [
                        float(member.get("similarity") or 0)
                        for member in active_members
                        if member.get("similarity") is not None
                    ]
                    or [0]
                ),
                "tokens": cluster_tokens,
            }
            clusters.append(cluster)

        # 按优先级排序并消除被高优先级聚类完全覆盖的低优先级聚类
        sorted_clusters = sorted(
            clusters,
            key=lambda cluster: (
                -self._cluster_rank(cluster),
                -self.helper._project_duplicate_risk_rank(str(cluster.get("risk_level") or "none")),
                -float(cluster.get("score_value") or 0),
                "/".join(cluster.get("files") or []),
            ),
        )
        dropped_indexes: set[int] = set()
        for strong_index, strong in enumerate(sorted_clusters):
            if strong_index in dropped_indexes:
                continue
            strong_files = set(strong.get("files") or [])
            for weak_index in range(strong_index + 1, len(sorted_clusters)):
                if weak_index in dropped_indexes:
                    continue
                weak = sorted_clusters[weak_index]
                if self._cluster_rank(strong) <= self._cluster_rank(weak):
                    continue
                if not self._clusters_are_textually_related(strong, weak):
                    continue
                common_files = strong_files & set(weak.get("files") or [])
                if len(common_files) < 2:
                    continue
                if not self._clusters_have_nested_ranges(strong, weak):
                    continue
                # 将弱聚类合并到强聚类
                for file_name in weak.get("files") or []:
                    if file_name not in strong["files"]:
                        strong["files"].append(file_name)
                    target_ranges = strong["doc_ranges_by_file"].setdefault(file_name, [])
                    for pair in weak.get("doc_ranges_by_file", {}).get(file_name) or []:
                        if pair not in target_ranges:
                            target_ranges.append(pair)
                    target_previews = strong["doc_previews_by_file"].setdefault(file_name, [])
                    for preview in weak.get("doc_previews_by_file", {}).get(file_name) or []:
                        if preview not in target_previews:
                            target_previews.append(preview)
                for token in weak.get("tokens") or []:
                    if token not in strong["tokens"]:
                        strong["tokens"].append(token)
                strong["items"].extend(weak.get("items") or [])
                strong["occurrences"].extend(weak.get("occurrences") or [])
                strong_files = set(strong.get("files") or [])
                dropped_indexes.add(weak_index)

        return [
            cluster
            for index, cluster in enumerate(sorted_clusters)
            if index not in dropped_indexes
        ]

    # ── 构建最终合并结果字典 ──────────────────────

    def build_merge_payload(
        self,
        *,
        raw_result: dict[str, Any],
        doc_type: str,
        source_result_key: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        为指定文档类型构建合并后的聚类视图，包含摘要统计和序列化聚类信息。
        """
        source_key = source_result_key or RAW_RESULT_KEY_BY_DOC_TYPE.get(doc_type) or "duplicate_check"
        group = ((raw_result.get("groups") or {}).get(doc_type) or {})
        items = list(self.helper._project_iter_duplicate_items(raw_result, doc_type))
        clusters = self.cluster_items(items)
        suspicious_clusters = [
            cluster
            for cluster in clusters
            if str(cluster.get("risk_level") or "none") != "none"
        ]
        high_clusters = [
            cluster for cluster in clusters if str(cluster.get("risk_level") or "none") == "high"
        ]
        medium_clusters = [
            cluster for cluster in clusters if str(cluster.get("risk_level") or "none") == "medium"
        ]

        serialized_clusters: list[dict[str, Any]] = []
        for cluster in clusters:
            serialized_clusters.append(
                {
                    "cluster_id": self._cluster_id(doc_type, cluster),
                    "title": self._cluster_title(cluster),
                    "family": str(cluster.get("family") or "block"),
                    "mode": str(cluster.get("mode") or "exact"),
                    "risk_level": str(cluster.get("risk_level") or "none"),
                    "score_display": str(cluster.get("score_display") or "0"),
                    "score_value": float(cluster.get("score_value") or 0),
                    "similarity": float(cluster.get("similarity") or 0),
                    "files": list(cluster.get("files") or []),
                    "file_count": len(cluster.get("files") or []),
                    "metrics": dict(cluster.get("metrics") or {}),
                    "doc_ranges_by_file": {
                        file_name: [
                            {"start_page": int(start_page), "end_page": int(end_page)}
                            for start_page, end_page in ranges
                        ]
                        for file_name, ranges in (cluster.get("doc_ranges_by_file") or {}).items()
                    },
                    "doc_previews_by_file": {
                        file_name: list(previews or [])
                        for file_name, previews in (cluster.get("doc_previews_by_file") or {}).items()
                    },
                    "tokens": list(cluster.get("tokens") or []),
                    "occurrence_count": len(cluster.get("occurrences") or []),
                    "pair_item_count": len(cluster.get("items") or []),
                    "occurrences": list(cluster.get("occurrences") or []),
                    "items": list(cluster.get("items") or []),
                }
            )

        return {
            "project": raw_result.get("project"),
            "source_result_key": source_key,
            "merged_result_key": MERGED_RESULT_KEY_BY_DOC_TYPE.get(doc_type),
            "document_type": doc_type,
            "config": {
                "merge_strategy": "duplicate_pair_cluster_merge",
                "source_document_types": list((raw_result.get("config") or {}).get("document_types") or []),
                "source_pair_count": int(group.get("pair_count") or 0),
                "source_reported_pair_count": int(group.get("reported_pair_count") or 0),
                "source_suspicious_pair_count": int(group.get("suspicious_pair_count") or 0),
            },
            "summary": {
                "document_count": int(group.get("document_count") or 0),
                "pair_count": int(group.get("pair_count") or 0),
                "reported_pair_count": int(group.get("reported_pair_count") or 0),
                "suspicious_pair_count": int(group.get("suspicious_pair_count") or 0),
                "high_risk_pair_count": int(group.get("high_risk_pair_count") or 0),
                "medium_risk_pair_count": int(group.get("medium_risk_pair_count") or 0),
                "cluster_count": len(serialized_clusters),
                "suspicious_cluster_count": len(suspicious_clusters),
                "high_risk_cluster_count": len(high_clusters),
                "medium_risk_cluster_count": len(medium_clusters),
            },
            "documents": list(group.get("documents") or []),
            "skipped_documents": list(group.get("skipped_documents") or []),
            "clusters": serialized_clusters,
        }


# ── 公开构建函数 ────────────────────────────────

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