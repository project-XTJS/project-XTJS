# -*- coding: utf-8 -*-
"""
查重结果合并器（组合所有 Mixin）
"""
from typing import Any, Optional

from .constants import (
    MERGE_STRATEGY,
    MERGED_RESULT_KEY_BY_DOC_TYPE,
    RAW_RESULT_KEY_BY_DOC_TYPE,
)
from .mixins.range_splitter import RangeSplitterMixin
from .mixins.token_extractor import TokenExtractorMixin
from .mixins.cluster_engine import ClusterEngineMixin
from .mixins.presentation import PresentationMixin


class DuplicateResultMerger(
    RangeSplitterMixin,
    TokenExtractorMixin,
    ClusterEngineMixin,
    PresentationMixin,
):
    """查重结果聚类合并器，将多个比较对中发现的重叠证据聚类为分组。"""

    def __init__(self, helper: Any) -> None:
        """helper 需提供 _coalesce_page_ranges、_project_normalize_pages 等辅助方法。"""
        self.helper = helper

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
                "merge_strategy": MERGE_STRATEGY,
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
