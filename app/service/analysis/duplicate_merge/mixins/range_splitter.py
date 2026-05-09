# -*- coding: utf-8 -*-
"""
页面范围拆分与证据爆炸 Mixin
"""
from typing import Any


class RangeSplitterMixin:
    """提供 _split_occurrence_ranges 和 _explode_evidence_occurrences 方法。"""

    # 依赖 helper 对象，由最终子类提供
    helper: Any

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