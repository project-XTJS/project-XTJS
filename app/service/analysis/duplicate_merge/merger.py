# -*- coding: utf-8 -*-
"""
查重结果合并器（组合所有 Mixin）
"""
import hashlib
import json
from typing import Any, Optional

from .constants import (
    MERGE_STRATEGY,
    MERGED_RESULT_KEY_BY_DOC_TYPE,
    RAW_RESULT_KEY_BY_DOC_TYPE,
)
from .storage import DUPLICATE_STORAGE_SCHEMA_VERSION, source_item_id
from .mixins.range_splitter import RangeSplitterMixin
from .mixins.token_extractor import TokenExtractorMixin
from .mixins.cluster_engine import ClusterEngineMixin
from .mixins.presentation import PresentationMixin
from app.service.analysis.location_utils import append_location, collect_locations, make_location


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

    @staticmethod
    def _cluster_typo_results(cluster: dict[str, Any], source_key: str) -> list[dict[str, Any]]:
        """Aggregate only candidates attached to occurrences in this cluster."""

        def occurrence_identity(value: dict[str, Any]) -> str:
            identity = {
                key: value.get(key)
                for key in (
                    "document_identifier_id", "file_name", "page", "bbox", "source_kind",
                    "source_text_length", "source_text_hash", "start", "end", "word_start", "word_end",
                )
            }
            body = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return hashlib.sha256(body.encode()).hexdigest()[:24]

        aggregated: list[dict[str, Any]] = []
        occurrence_sets: list[set[str]] = []
        for cluster_occurrence in cluster.get("occurrences") or []:
            if not isinstance(cluster_occurrence, dict):
                continue
            evidence = cluster_occurrence.get("evidence")
            if not isinstance(evidence, dict):
                continue
            for raw in evidence.get(source_key) or []:
                if not isinstance(raw, dict):
                    continue
                typo_occurrences = [
                    dict(value)
                    for value in raw.get("occurrences") or []
                    if isinstance(value, dict)
                ]
                if not typo_occurrences:
                    continue
                identities = {occurrence_identity(value) for value in typo_occurrences}
                core = (
                    str(raw.get("original_word") or raw.get("matched_text") or ""),
                    str(raw.get("replacement_word") or raw.get("suggestion") or ""),
                    str(raw.get("original") or raw.get("highlight_text") or ""),
                    str(raw.get("replacement") or ""),
                    str(raw.get("verification_status") or ""),
                )
                merge_index = None
                for index, existing in enumerate(aggregated):
                    existing_core = (
                        str(existing.get("original_word") or existing.get("matched_text") or ""),
                        str(existing.get("replacement_word") or existing.get("suggestion") or ""),
                        str(existing.get("original") or existing.get("highlight_text") or ""),
                        str(existing.get("replacement") or ""),
                        str(existing.get("verification_status") or ""),
                    )
                    if core == existing_core and occurrence_sets[index] & identities:
                        merge_index = index
                        break
                if merge_index is None:
                    item = dict(raw)
                    item["occurrences"] = typo_occurrences
                    item["locations"] = list(raw.get("locations") or [])
                    aggregated.append(item)
                    occurrence_sets.append(set(identities))
                    continue
                item = aggregated[merge_index]
                known = occurrence_sets[merge_index]
                for value in typo_occurrences:
                    identity = occurrence_identity(value)
                    if identity not in known:
                        item["occurrences"].append(value)
                        known.add(identity)
                for location in raw.get("locations") or []:
                    if location not in item["locations"]:
                        item["locations"].append(location)

        for index, item in enumerate(aggregated):
            material = "|".join(
                [
                    str(item.get("original_word") or item.get("matched_text") or ""),
                    str(item.get("replacement_word") or item.get("suggestion") or ""),
                    *sorted(occurrence_sets[index]),
                ]
            )
            item["shared_id"] = hashlib.sha256(material.encode()).hexdigest()[:32]
        return aggregated

    @classmethod
    def _cluster_typo_issues(cls, cluster: dict[str, Any]) -> list[dict[str, Any]]:
        return cls._cluster_typo_results(cluster, "short_duplicate_typo_issues")

    @classmethod
    def _cluster_typo_review_candidates(cls, cluster: dict[str, Any]) -> list[dict[str, Any]]:
        return cls._cluster_typo_results(cluster, "typo_review_candidates")

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
        source_items: dict[str, dict[str, Any]] = {}

        def remember_source(item: Any) -> str:
            if not isinstance(item, dict):
                raise ValueError("查重聚类来源项必须是对象")
            identifier = source_item_id(item)
            source_items.setdefault(identifier, item)
            return identifier

        for cluster in clusters:
            typo_issues = self._cluster_typo_issues(cluster)
            typo_review_candidates = self._cluster_typo_review_candidates(cluster)
            source_typo_checks = [
                occurrence["evidence"].get("typo_check") or {}
                for occurrence in cluster.get("occurrences") or []
                if isinstance(occurrence, dict)
                and isinstance(occurrence.get("evidence"), dict)
            ]
            typo_incomplete = any(
                isinstance(occurrence, dict)
                and isinstance(occurrence.get("evidence"), dict)
                and (occurrence["evidence"].get("typo_check") or {}).get("status") == "incomplete"
                for occurrence in cluster.get("occurrences") or []
            )
            source_typo_statuses = {
                str(value.get("status") or "") for value in source_typo_checks
            }
            typo_status = (
                "incomplete"
                if typo_incomplete
                else "disabled"
                if source_typo_statuses and source_typo_statuses <= {"disabled"}
                else "completed"
            )
            review_only = (
                bool(typo_review_candidates or typo_incomplete)
                and not typo_issues
                and str(cluster.get("risk_level") or "none") == "none"
            )
            serialized_occurrences = []
            for occurrence in cluster.get("occurrences") or []:
                source = occurrence.get("item") if isinstance(occurrence, dict) else None
                serialized = {
                    key: value
                    for key, value in dict(occurrence or {}).items()
                    if key not in {"item", "source_item_id"}
                }
                if isinstance(source, dict):
                    serialized["source_item_id"] = remember_source(source)
                    serialized.setdefault("left_file_name", source.get("left_file_name"))
                    serialized.setdefault("right_file_name", source.get("right_file_name"))
                serialized_occurrences.append(serialized)
            serialized_clusters.append(
                {
                    "cluster_id": self._cluster_id(doc_type, cluster),
                    "title": self._cluster_title(cluster),
                    "family": str(cluster.get("family") or "block"),
                    "mode": str(cluster.get("mode") or "exact"),
                    "risk_level": str(cluster.get("risk_level") or "none"),
                    "status": "unclear" if review_only else "failed" if str(cluster.get("risk_level") or "none") != "none" else "passed",
                    "review_only": review_only,
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
                    "short_duplicate_typo_issues": typo_issues,
                    "typo_review_candidates": typo_review_candidates,
                    "typo_check": {
                        "status": typo_status,
                        "rule_version": next((item.get("rule_version") for item in typo_issues + typo_review_candidates if item.get("rule_version")), None)
                        or next((value.get("rule_version") for value in source_typo_checks if value.get("rule_version")), None),
                        "word_rule_version": next((item.get("word_rule_version") for item in typo_issues + typo_review_candidates if item.get("word_rule_version")), None)
                        or next((value.get("word_rule_version") for value in source_typo_checks if value.get("word_rule_version")), None),
                        "verifier_model": next((value.get("verifier_model") for value in source_typo_checks if value.get("verifier_model")), None),
                        "detector_model": next((value.get("detector_model") for value in source_typo_checks if value.get("detector_model")), None),
                        "confirmed_count": len(typo_issues),
                        "review_candidate_count": len(typo_review_candidates),
                        "eligible_count": sum(int(value.get("eligible_count") or 0) for value in source_typo_checks),
                        "hidden_count": sum(int(value.get("hidden_count") or 0) for value in source_typo_checks),
                        "incomplete_count": sum(int(value.get("incomplete_count") or 0) for value in source_typo_checks),
                        **{
                            key: sum(int(value.get(key) or 0) for value in source_typo_checks)
                            for key in ("budget_skipped_count", "verifier_rejected_count", "cec3_unsupported_count", "word_invalid_count", "position_invalid_count", "detector_rejected_count", "source_valid_rejected_count", "similarity_rejected_count")
                        },
                    },
                    "occurrence_count": len(cluster.get("occurrences") or []),
                    "source_issue_count": len(cluster.get("items") or []),
                    "locations": self._cluster_locations(cluster),
                    "occurrences": serialized_occurrences,
                    "source_issue_ids": [
                        remember_source(item)
                        for item in cluster.get("items") or []
                    ],
                }
            )

        return {
            "storage_schema_version": DUPLICATE_STORAGE_SCHEMA_VERSION,
            "source_items": source_items,
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
                "typo_check": dict(group.get("typo_check") or {}),
            },
            "documents": list(group.get("documents") or []),
            "skipped_documents": list(group.get("skipped_documents") or []),
            "issues": serialized_clusters,
        }

    def _cluster_locations(self, cluster: dict[str, Any]) -> list[dict[str, Any]]:
        """Build locations from the cluster's own occurrences, not whole pair issues."""
        locations: list[dict[str, Any]] = []
        for occurrence in cluster.get("occurrences") or []:
            if not isinstance(occurrence, dict):
                continue
            item = occurrence.get("item") if isinstance(occurrence.get("item"), dict) else {}
            evidence = occurrence.get("evidence") if isinstance(occurrence.get("evidence"), dict) else {}
            docs = occurrence.get("docs") if isinstance(occurrence.get("docs"), dict) else {}

            for side in ("left", "right"):
                file_name = str(item.get(f"{side}_file_name") or "").strip()
                if not file_name:
                    continue
                doc = docs.get(file_name) if isinstance(docs.get(file_name), dict) else {}
                pages = self.helper._project_normalize_pages(
                    doc.get("pages"),
                    evidence.get(f"{side}_pages"),
                    evidence.get(f"{side}_page"),
                    evidence.get("page"),
                )
                text = (
                    doc.get("preview")
                    or evidence.get(f"{side}_text")
                    or evidence.get(f"{side}_preview")
                    or evidence.get(f"{side}_title")
                    or evidence.get("text")
                    or evidence.get("preview")
                    or evidence.get("title")
                    or ""
                )
                bbox = evidence.get(f"{side}_bbox") or evidence.get("bbox")
                for index, page in enumerate(pages or []):
                    append_location(
                        locations,
                        make_location(
                            document_identifier_id=(
                                item.get(f"{side}_document_identifier")
                                or item.get(f"{side}_document_identifier_id")
                                or item.get(f"{side}_document_id")
                            ),
                            file_name=file_name,
                            page=page,
                            bbox=bbox if index == 0 else None,
                            text=text,
                        ),
                    )

        if locations:
            return locations
        return collect_locations(cluster.get("items") or [])
