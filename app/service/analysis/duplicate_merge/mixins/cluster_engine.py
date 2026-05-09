# -*- coding: utf-8 -*-
"""
聚类引擎 Mixin：并查集聚类与嵌套消除
"""
from typing import Any


class ClusterEngineMixin:
    """包含主聚类方法 cluster_items 以及范围、文本关联判断。"""

    helper: Any

    # 以下方法声明为抽象依赖，实际由其他 Mixin 提供
    _explode_evidence_occurrences: Any
    _cluster_family: Any
    _cluster_mode: Any
    _occurrence_tokens: Any
    _cluster_rank: Any
    _occurrence_preview: Any
    _cluster_title: Any
    _cluster_id: Any

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

    def cluster_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        将查重比较对中的各类证据（精确/相似，段落/句子/表格/图片）
        按文本 token 的共有性聚合为聚类，并消除嵌套子集。
        """
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