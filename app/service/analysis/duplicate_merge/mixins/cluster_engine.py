# -*- coding: utf-8 -*-
"""
聚类引擎 Mixin：并查集聚类与嵌套消除
"""
import json
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

    def _pair_item_signature(self, item: dict[str, Any]) -> str:
        """生成底层查重 pair 的稳定签名，用于后续聚合收口。"""
        if not isinstance(item, dict):
            return self.helper._project_make_stable_token("duplicate-pair-item", str(item))
        sides = []
        for prefix in ("left", "right"):
            sides.append(
                "|".join(
                    [
                        str(item.get(f"{prefix}_relation_id") or "").strip(),
                        str(item.get(f"{prefix}_document_identifier") or "").strip(),
                        str(item.get(f"{prefix}_file_name") or "").strip(),
                    ]
                )
            )
        return self.helper._project_make_stable_token(
            "duplicate-pair-item",
            str(item.get("document_type") or "").strip(),
            *sorted(sides),
        )

    def _occurrence_signature(self, occurrence: dict[str, Any]) -> str:
        """生成单条聚类证据的稳定签名，用于去重。"""
        docs = occurrence.get("docs") or {}
        normalized_docs = {
            str(file_name): {
                "pages": list((doc or {}).get("pages") or []),
                "preview": str((doc or {}).get("preview") or "").strip(),
            }
            for file_name, doc in docs.items()
        }
        return self.helper._project_make_stable_token(
            "duplicate-occurrence",
            str(occurrence.get("family") or "").strip(),
            str(occurrence.get("mode") or "").strip(),
            json.dumps(normalized_docs, ensure_ascii=False, sort_keys=True),
            json.dumps(list(occurrence.get("tokens") or []), ensure_ascii=False),
            str(occurrence.get("similarity") or ""),
        )

    def _cluster_group_signature(self, cluster: dict[str, Any]) -> str:
        """按 mode + 底层 pair 集合生成聚类收口签名。"""
        tokens = sorted(
            {
                str(token).strip()
                for token in (cluster.get("tokens") or [])
                if str(token).strip()
            }
        )
        if tokens:
            return self.helper._project_make_stable_token(
                "duplicate-cluster-group",
                str(cluster.get("mode") or "exact"),
                str(cluster.get("family") or "block"),
                *tokens,
            )

        signatures = sorted(
            {
                str(signature).strip()
                for signature in (cluster.get("item_signatures") or [])
                if str(signature).strip()
            }
        )
        if signatures:
            return self.helper._project_make_stable_token(
                "duplicate-cluster-group",
                str(cluster.get("mode") or "exact"),
                *signatures,
            )
        return self.helper._project_make_stable_token(
            "duplicate-cluster-group-fallback",
            str(cluster.get("mode") or "exact"),
            *sorted(str(file_name) for file_name in (cluster.get("files") or []) if file_name),
        )

    def _merge_cluster_into(
        self,
        target: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        """合并两个已归组的 cluster。"""
        for file_name in source.get("files") or []:
            if file_name not in target["files"]:
                target["files"].append(file_name)

        for file_name, ranges in (source.get("doc_ranges_by_file") or {}).items():
            target_ranges = target["doc_ranges_by_file"].setdefault(file_name, [])
            for pair in ranges or []:
                if pair not in target_ranges:
                    target_ranges.append(pair)

        for file_name, previews in (source.get("doc_previews_by_file") or {}).items():
            target_previews = target["doc_previews_by_file"].setdefault(file_name, [])
            for preview in previews or []:
                if preview not in target_previews:
                    target_previews.append(preview)

        target["items"].extend(source.get("items") or [])
        target["item_signatures"].extend(source.get("item_signatures") or [])
        target["occurrences"].extend(source.get("occurrences") or [])
        target["tokens"].extend(source.get("tokens") or [])
        return target

    def _coalesce_cluster_ranges(
        self,
        ranges: list[tuple[int, int]],
    ) -> list[tuple[int, int]]:
        """Merge adjacent page ranges so connected evidence is shown as one span."""
        normalized = sorted(
            {
                (min(int(start), int(end)), max(int(start), int(end)))
                for start, end in ranges
            }
        )
        if not normalized:
            return []
        coalesced: list[tuple[int, int]] = []
        current_start, current_end = normalized[0]
        for start, end in normalized[1:]:
            if start <= current_end + 1:
                current_end = max(current_end, end)
                continue
            coalesced.append((current_start, current_end))
            current_start, current_end = start, end
        coalesced.append((current_start, current_end))
        return coalesced

    def _finalize_cluster(self, cluster: dict[str, Any]) -> dict[str, Any]:
        """对 cluster 做去重和派生字段重算，避免同一 pair 被拆成重复组。"""
        files: list[str] = []
        for file_name in cluster.get("files") or []:
            normalized = str(file_name or "").strip()
            if normalized and normalized not in files:
                files.append(normalized)
        cluster["files"] = files

        doc_ranges_by_file: dict[str, list[tuple[int, int]]] = {}
        for file_name, ranges in (cluster.get("doc_ranges_by_file") or {}).items():
            normalized_ranges: list[tuple[int, int]] = []
            for pair in ranges or []:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    continue
                try:
                    normalized_pair = (int(pair[0]), int(pair[1]))
                except (TypeError, ValueError):
                    continue
                if normalized_pair not in normalized_ranges:
                    normalized_ranges.append(normalized_pair)
            if normalized_ranges:
                doc_ranges_by_file[str(file_name)] = self._coalesce_cluster_ranges(normalized_ranges)
        cluster["doc_ranges_by_file"] = doc_ranges_by_file

        doc_previews_by_file: dict[str, list[str]] = {}
        for file_name, previews in (cluster.get("doc_previews_by_file") or {}).items():
            normalized_previews: list[str] = []
            for preview in previews or []:
                normalized = str(preview or "").strip()
                if normalized and normalized not in normalized_previews:
                    normalized_previews.append(normalized)
            if normalized_previews:
                doc_previews_by_file[str(file_name)] = normalized_previews
        cluster["doc_previews_by_file"] = doc_previews_by_file

        tokens: list[str] = []
        for token in cluster.get("tokens") or []:
            normalized = str(token or "").strip()
            if normalized and normalized not in tokens:
                tokens.append(normalized)
        cluster["tokens"] = tokens

        unique_items: list[dict[str, Any]] = []
        item_signatures: list[str] = []
        seen_item_signatures: set[str] = set()
        for item in cluster.get("items") or []:
            signature = self._pair_item_signature(item)
            if signature in seen_item_signatures:
                continue
            seen_item_signatures.add(signature)
            item_signatures.append(signature)
            unique_items.append(item)
        cluster["items"] = unique_items
        cluster["item_signatures"] = item_signatures

        unique_occurrences: list[dict[str, Any]] = []
        seen_occurrence_signatures: set[str] = set()
        for occurrence in cluster.get("occurrences") or []:
            signature = self._occurrence_signature(occurrence)
            if signature in seen_occurrence_signatures:
                continue
            seen_occurrence_signatures.add(signature)
            unique_occurrences.append(occurrence)
        cluster["occurrences"] = unique_occurrences

        families = {
            str(occurrence.get("family") or "").strip()
            for occurrence in unique_occurrences
            if str(occurrence.get("family") or "").strip()
        }
        if not families:
            families = {str(cluster.get("family") or "block")}
        cluster["family"] = next(iter(families)) if len(families) == 1 else "mixed"

        mode = str(cluster.get("mode") or "exact")
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
        for item in unique_items:
            item_metrics = item.get("_project_raw_metrics") or item.get("metrics") or {}
            for key in metrics:
                metrics[key] += int(item_metrics.get(key) or 0)
        cluster["metrics"] = metrics

        best_risk_level = "none"
        best_risk_rank = -1
        best_score_value = 0.0
        best_score_display = str(cluster.get("score_display") or "0")
        for item in unique_items:
            risk_level = str(item.get("risk_level") or "none")
            risk_rank = self.helper._project_duplicate_risk_rank(risk_level)
            score_raw = item.get("match_score")
            if score_raw is None:
                score_raw = item.get("exact_match_score")
            try:
                score_value = float(score_raw or 0)
            except (TypeError, ValueError):
                score_value = 0.0
            if (
                risk_rank > best_risk_rank
                or (risk_rank == best_risk_rank and score_value >= best_score_value)
            ):
                best_risk_rank = risk_rank
                best_risk_level = risk_level
                best_score_value = score_value
                best_score_display = self.helper._project_duplicate_score(item)

        if best_risk_rank >= 0:
            cluster["risk_level"] = best_risk_level
            cluster["score_value"] = best_score_value
            cluster["score_display"] = best_score_display
        else:
            cluster["risk_level"] = str(cluster.get("risk_level") or "none")
            cluster["score_value"] = float(cluster.get("score_value") or 0)
            cluster["score_display"] = str(cluster.get("score_display") or "0")

        cluster["similarity"] = max(
            [
                float(occurrence.get("similarity") or 0)
                for occurrence in unique_occurrences
                if occurrence.get("similarity") is not None
            ]
            or [0]
        )
        return cluster

    def _consolidate_clusters(
        self,
        clusters: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """将同一底层 pair 集合的 cluster 合并成更稳定的展示组。"""
        grouped: dict[str, dict[str, Any]] = {}
        ordered_keys: list[str] = []
        for cluster in clusters:
            finalized = self._finalize_cluster(cluster)
            key = self._cluster_group_signature(finalized)
            target = grouped.get(key)
            if target is None:
                grouped[key] = finalized
                ordered_keys.append(key)
                continue
            self._merge_cluster_into(target, finalized)

        consolidated = [self._finalize_cluster(grouped[key]) for key in ordered_keys]
        consolidated = self._merge_adjacent_text_clusters(consolidated)
        return sorted(
            consolidated,
            key=lambda cluster: (
                -self._cluster_rank(cluster),
                -self.helper._project_duplicate_risk_rank(str(cluster.get("risk_level") or "none")),
                -float(cluster.get("score_value") or 0),
                "/".join(cluster.get("files") or []),
            ),
        )

    def _ranges_are_near(
        self,
        left_ranges: list[tuple[int, int]],
        right_ranges: list[tuple[int, int]],
        *,
        max_gap: int = 1,
    ) -> bool:
        """Return true when every right range touches or nearly touches a left range."""
        if not left_ranges or not right_ranges:
            return False
        for right_start, right_end in right_ranges:
            matched = False
            for left_start, left_end in left_ranges:
                if right_start <= left_end + max_gap and left_start <= right_end + max_gap:
                    matched = True
                    break
            if not matched:
                return False
        return True

    def _clusters_are_adjacent_text_evidence(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
    ) -> bool:
        """Merge same-pair textual clusters when their page ranges are continuous."""
        if str(left.get("mode") or "") != str(right.get("mode") or ""):
            return False
        left_family = str(left.get("family") or "")
        right_family = str(right.get("family") or "")
        if left_family != right_family or left_family not in {"section", "block"}:
            return False

        left_files = {str(file_name) for file_name in (left.get("files") or []) if str(file_name)}
        right_files = {str(file_name) for file_name in (right.get("files") or []) if str(file_name)}
        if len(left_files) < 2 or left_files != right_files:
            return False

        left_signatures = {
            str(signature)
            for signature in (left.get("item_signatures") or [])
            if str(signature)
        }
        right_signatures = {
            str(signature)
            for signature in (right.get("item_signatures") or [])
            if str(signature)
        }
        if left_signatures and right_signatures and not (left_signatures & right_signatures):
            return False

        left_ranges_by_file = left.get("doc_ranges_by_file") or {}
        right_ranges_by_file = right.get("doc_ranges_by_file") or {}
        for file_name in left_files:
            if not self._ranges_are_near(
                left_ranges_by_file.get(file_name) or [],
                right_ranges_by_file.get(file_name) or [],
            ):
                return False
        return True

    def _merge_adjacent_text_clusters(
        self,
        clusters: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Collapse continuous same-pair text clusters into fewer display groups."""
        merged: list[dict[str, Any]] = []
        for cluster in clusters:
            placed = False
            for target in merged:
                if not self._clusters_are_adjacent_text_evidence(target, cluster):
                    continue
                self._merge_cluster_into(target, cluster)
                placed = True
                break
            if not placed:
                merged.append(cluster)
        return [self._finalize_cluster(cluster) for cluster in merged]

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
            if isinstance(item, dict) and isinstance(raw_item, dict):
                item["_project_raw_metrics"] = dict(raw_item.get("metrics") or {})
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
                                "item_signature": self._pair_item_signature(item),
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
            active_members = list(members)
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
                "item_signatures": [
                    str(member.get("item_signature") or "").strip()
                    for member in active_members
                    if str(member.get("item_signature") or "").strip()
                ],
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
            clusters.append(self._finalize_cluster(cluster))

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
                strong["item_signatures"].extend(weak.get("item_signatures") or [])
                strong["occurrences"].extend(weak.get("occurrences") or [])
                strong_files = set(strong.get("files") or [])
                dropped_indexes.add(weak_index)

        surviving_clusters = [
            self._finalize_cluster(cluster)
            for index, cluster in enumerate(sorted_clusters)
            if index not in dropped_indexes
        ]
        return self._consolidate_clusters(surviving_clusters)
