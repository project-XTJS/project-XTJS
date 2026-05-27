# -*- coding: utf-8 -*-
"""文档解析 Mixin"""
import json
import re
from typing import Any

from app.service.analysis.location_utils import normalize_bbox


class ParseMixin:
    """提供文档加载、页面行提取、区段处理等方法。"""

    # 依赖常量、文本工具
    STAR_RE: re.Pattern
    _norm: Any
    _merge_unique_parts: Any
    _split_lines: Any
    _normalize_markup_text: Any
    _section_text: Any

    def _coerce_payload(self, value: Any) -> dict:
        """将原始输入规范化成字典，支持 JSON 字符串自动解析。"""
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("{") or raw.startswith("["):
                try:
                    loaded = json.loads(raw)
                except json.JSONDecodeError:
                    return {"content": value}
                return loaded if isinstance(loaded, dict) else {"data": loaded}
            return {"content": value}
        return {}

    def _has_extractable_fields(self, obj: Any) -> bool:
        """判断字典中是否含有可提取的文档字段。"""
        if not isinstance(obj, dict):
            return False
        return any(
            key in obj
            for key in (
                "content",
                "text",
                "pages",
                "blocks",
                "layout_sections",
                "table_sections",
                "logical_tables",
            )
        )

    def _doc_container(self, payload: dict) -> dict:
        """从可能的嵌套结构中找到包含实际内容的字典。"""
        if self._has_extractable_fields(payload):
            return payload
        data = payload.get("data")
        if self._has_extractable_fields(data):
            return data
        doc = payload.get("document")
        if self._has_extractable_fields(doc):
            return doc
        return payload

    def _extract_text(self, payload: dict) -> str:
        """将文档对象的所有文本内容合并为一个字符串。"""
        doc = self._doc_container(payload)
        parts: list[str] = []
        for key in ("content", "text"):
            val = doc.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
        pages = doc.get("pages")
        if isinstance(pages, list):
            parts.extend(str((x.get("text") if isinstance(x, dict) else x) or "").strip() for x in pages)
        blocks = doc.get("blocks")
        if isinstance(blocks, list):
            parts.extend(str((x.get("text") if isinstance(x, dict) else "") or "").strip() for x in blocks)
        parts.extend(section["text"] for section in self._section_items(doc))
        return "\n".join(self._merge_unique_parts(parts)).strip()

    def _page_lines(self, payload: dict) -> list[dict[str, Any]]:
        """将文档按页、按行拆分为带页码信息的结构列表。"""
        doc = self._doc_container(payload)
        pages = doc.get("pages")
        out: list[dict[str, Any]] = []
        if isinstance(pages, list):
            for idx, page in enumerate(pages, start=1):
                page_no, text = idx, ""
                if isinstance(page, dict):
                    page_no = int(page.get("page") or idx)
                    text = str(page.get("text") or "")
                else:
                    text = str(page or "")
                page_bbox = (
                    normalize_bbox(page.get("bbox") or page.get("bbox_ocr") or page.get("box"))
                    if isinstance(page, dict)
                    else None
                )
                for ln, line in enumerate(self._split_lines(text), start=1):
                    out.append({"page": page_no, "line_number": ln, "text": line, "bbox": page_bbox})

        if not out:
            section_line_counter: dict[int | None, int] = {}
            for section in self._section_items(doc):
                page_no = section.get("page")
                for line in self._split_lines(section.get("text", "")):
                    current = section_line_counter.get(page_no, 0) + 1
                    section_line_counter[page_no] = current
                    out.append({"page": page_no, "line_number": current, "text": line, "bbox": section.get("bbox")})

        if out:
            return out
        for ln, line in enumerate(self._split_lines(self._extract_text(payload)), start=1):
            out.append({"page": None, "line_number": ln, "text": line})
        return out

    def _section_items(self, doc: dict) -> list[dict[str, Any]]:
        """从文档中提取所有标准化区段（按页码、类型排序）。"""
        sections: list[dict[str, Any]] = []
        seen = set()

        for source_idx, key in enumerate(("layout_sections", "table_sections", "logical_tables")):
            raw_sections = doc.get(key)
            if not isinstance(raw_sections, list):
                continue
            for item_idx, item in enumerate(raw_sections):
                if isinstance(item, dict):
                    page_raw = item.get("page")
                    if page_raw is None and key == "logical_tables":
                        pages = item.get("pages")
                        if isinstance(pages, list) and pages:
                            page_raw = pages[0]
                    default_type = "table" if key in ("table_sections", "logical_tables") else "text"
                    section_type = str(item.get("type") or default_type).strip().lower() or "text"
                    text = self._section_text(item)
                    bbox = normalize_bbox(
                        item.get("bbox")
                        or item.get("bbox_ocr")
                        or item.get("box")
                        or item.get("block_bbox")
                    )
                else:
                    page_raw = None
                    section_type = "table" if key in ("table_sections", "logical_tables") else "text"
                    text = str(item or "").strip()
                    bbox = None

                if not text:
                    continue

                page_no: int | None
                try:
                    page_no = int(page_raw) if page_raw is not None else None
                except (TypeError, ValueError):
                    page_no = None

                signature = (page_no, section_type, self._norm(text)[:260])
                if not signature[2] or signature in seen:
                    continue
                seen.add(signature)
                sections.append(
                    {
                        "page": page_no,
                        "type": section_type,
                        "text": text,
                        "bbox": bbox,
                        "_source_order": source_idx,
                        "_item_order": item_idx,
                    }
                )

        sections.sort(
            key=lambda x: (
                x["page"] if isinstance(x.get("page"), int) else 10**9,
                x.get("_source_order", 0),
                x.get("_item_order", 0),
            )
        )
        return sections

    def _section_text(self, section: Any) -> str:
        """从区段中提取所有可能的文本内容。"""
        if isinstance(section, str):
            return section.strip()
        if not isinstance(section, dict):
            return ""

        parts: list[str] = []
        for key in ("text", "raw_text", "markdown", "html", "pred_html", "content", "caption", "block_content"):
            val = section.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(self._normalize_markup_text(val, preserve_lines=key in {"html", "pred_html", "block_content"}))

        for key in ("cell_texts", "texts", "rec_texts", "headers"):
            val = section.get(key)
            if isinstance(val, list):
                parts.extend(
                    self._normalize_markup_text(x, preserve_lines=False)
                    for x in val
                    if self._normalize_markup_text(x, preserve_lines=False)
                )

        rows = section.get("rows")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, list):
                    parts.append(" ".join(str(x or "").strip() for x in row if str(x or "").strip()))
                elif isinstance(row, dict):
                    parts.append(" ".join(str(x or "").strip() for x in row.values() if str(x or "").strip()))

        records = section.get("records")
        if isinstance(records, list):
            for row in records:
                if isinstance(row, dict):
                    parts.append(" ".join(str(x or "").strip() for x in row.values() if str(x or "").strip()))

        for key, val in section.items():
            if key.startswith("col_") and isinstance(val, str) and val.strip():
                parts.append(self._normalize_markup_text(val, preserve_lines=False))

        return "\n".join(self._merge_unique_parts(parts)).strip()
