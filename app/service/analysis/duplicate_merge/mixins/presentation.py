# -*- coding: utf-8 -*-
"""
聚类展示 Mixin：预览文本、聚类标题、ID 生成
"""
import json
from typing import Any


class PresentationMixin:
    """提供 _occurrence_preview, _cluster_title, _cluster_id 方法。"""

    helper: Any

    def _occurrence_preview(self, kind: str, evidence: dict[str, Any], side: str) -> str:
        """根据证据类型和左右侧提取用于展示的预览文本。"""
        if kind in {"section", "similar_section"}:
            if side == "left":
                raw = evidence.get("left_preview") or evidence.get("left_title") or evidence.get("preview") or "-"
            else:
                raw = evidence.get("right_preview") or evidence.get("right_title") or evidence.get("preview") or "-"
            return self.helper._project_trim_text(str(raw), 200)
        if kind == "block":
            return self.helper._project_trim_text(str(evidence.get("text") or "-"), 200)
        if kind == "similar_block":
            raw = evidence.get(f"{side}_text") or evidence.get("text") or "-"
            return self.helper._project_trim_text(str(raw), 200)
        if kind == "table":
            rows = evidence.get("sample_rows") or []
            return self.helper._project_trim_text(json.dumps(rows, ensure_ascii=False), 200) if rows else "-"
        if kind == "similar_table":
            rows = evidence.get(f"{side}_sample_rows") or evidence.get("sample_rows") or []
            return self.helper._project_trim_text(json.dumps(rows, ensure_ascii=False), 200) if rows else "-"
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
            ("exact", "mixed"): "重复证据组",
            ("similar", "section"): "相似段落",
            ("similar", "block"): "相似句子",
            ("similar", "table"): "相似表格",
            ("similar", "mixed"): "相似证据组",
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
        if preview_text:
            if family == "table":
                preview_text = self.helper._project_trim_text(preview_text, 200).strip(" -")
            else:
                preview_text = self.helper._project_trim_text(preview_text, 42).strip(" -")
        else:
            preview_text = ""
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
