# -*- coding: utf-8 -*-
"""
从 OCR 结构中提取区块、表格、回退全文
"""
import html
import re
from html.parser import HTMLParser
from typing import Any

from app.service.analysis.template_extractor import SectionClassifier

from .text_utils import (
    normalize_plain_text,
    compact_raw_text,
    hash_text,
    is_noise_block,
)
from .constants import (
    SPLIT_LINE_PATTERN,
    BUSINESS_SIMILARITY_MIN_KEY_LENGTH,
)


class _TableHTMLParser(HTMLParser):
    """简易 HTML 表格解析器，用于提取表格的行文本。"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized == "tr":
            self._current_row = []
            return
        if normalized in {"td", "th"} and self._current_row is not None:
            self._current_cell_parts = []
            return
        if normalized == "br" and self._current_cell_parts is not None:
            self._current_cell_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"td", "th"} and self._current_row is not None and self._current_cell_parts is not None:
            text = "".join(self._current_cell_parts)
            text = re.sub(r"\s+", " ", html.unescape(text)).strip()
            self._current_row.append(text)
            self._current_cell_parts = None
            return
        if normalized == "tr" and self._current_row is not None:
            if any(cell for cell in self._current_row):
                self.rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        if self._current_cell_parts is not None:
            self._current_cell_parts.append(data)


def extract_document_content(
    payload: dict[str, Any],
    *,
    role: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """按文档角色提取可供查重的区块和表格，返回 (blocks, tables, skip_reason)。"""
    container = _container(payload)
    ordered_blocks, table_entries = _extract_ordered_blocks(container)

    if not ordered_blocks:
        # 回退到纯文本全文
        fallback_text = _fallback_text(container)
        if fallback_text:
            exact_key = compact_raw_text(fallback_text)
            ordered_blocks = [
                {
                    "type": "text",
                    "page": 1,
                    "text": fallback_text,
                    "exact_key": exact_key,
                    "exact_hash": hash_text(exact_key),
                }
            ]

    return ordered_blocks, table_entries, "missing_or_unusable_ocr_content"


def _container(payload: dict[str, Any]) -> dict[str, Any]:
    """提取 payload 中的实际内容容器（优先使用 data 字段）。"""
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def _extract_ordered_blocks(
    container: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """从文档容器中提取顺序排列的区块和表格。"""
    table_queues, table_entries = _build_table_queues(container)
    layout_sections = _layout_sections(container)
    ordered_blocks: list[dict[str, Any]] = []

    for section in layout_sections:
        section_type = str(section.get("type") or "text").strip().lower()
        if section_type in {"seal", "signature"}:
            continue

        page = section.get("page") if isinstance(section.get("page"), int) else 1
        if section_type == "table":
            page_tables = table_queues.get(page) or []
            text = page_tables.pop(0) if page_tables else ""
            if not text:
                text = normalize_plain_text(
                    section.get("raw_text") or section.get("text") or ""
                )
        else:
            text = normalize_plain_text(section.get("text") or section.get("raw_text") or "")

        if is_noise_block(text, section_type):
            continue

        exact_key = compact_raw_text(text)
        if section_type != "heading" and len(exact_key) < 8:
            continue

        ordered_blocks.append(
            {
                "type": section_type,
                "page": page,
                "text": text,
                "exact_key": exact_key,
                "exact_hash": hash_text(exact_key),
            }
        )

    return ordered_blocks, table_entries


def _layout_sections(container: dict[str, Any]) -> list[dict[str, Any]]:
    """获取并按位置排序布局区段。"""
    items = container.get("layout_sections")
    if not isinstance(items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = item.get("text") or item.get("raw_text")
        if not text:
            continue
        bbox = item.get("bbox") or item.get("bbox_ocr") or item.get("box")
        anchor = _bbox_anchor(bbox)
        normalized.append(
            {
                "page": item.get("page") if isinstance(item.get("page"), int) else 1,
                "type": str(item.get("type") or "text").strip().lower() or "text",
                "text": text,
                "raw_text": item.get("raw_text"),
                "_sort_y": anchor[1],
                "_sort_x": anchor[0],
            }
        )

    normalized.sort(key=lambda item: (item["page"], item["_sort_y"], item["_sort_x"]))
    return normalized


def _bbox_anchor(bbox: Any) -> tuple[int, int]:
    """获取 bbox 左上角坐标，用于排序。"""
    if isinstance(bbox, (list, tuple)) and len(bbox) >= 2:
        if all(isinstance(item, (int, float)) for item in bbox[:2]):
            return (int(round(float(bbox[0]))), int(round(float(bbox[1]))))
        if bbox and all(
            isinstance(item, (list, tuple))
            and len(item) >= 2
            and all(isinstance(value, (int, float)) for value in item[:2])
            for item in bbox
        ):
            xs = [int(round(float(item[0]))) for item in bbox]
            ys = [int(round(float(item[1]))) for item in bbox]
            return (min(xs), min(ys))
    return (10**9, 10**9)


def _build_table_queues(
    container: dict[str, Any],
) -> tuple[dict[int, list[str]], list[dict[str, Any]]]:
    """构建按页码索引的表格文本队列和表格条目列表。"""
    candidates: list[Any] = []
    if isinstance(container.get("logical_tables"), list):
        candidates.extend(container.get("logical_tables") or [])
    if isinstance(container.get("table_sections"), list):
        candidates.extend(container.get("table_sections") or [])

    page_queues: dict[int, list[str]] = {}
    table_entries: list[dict[str, Any]] = []
    for raw_item in candidates:
        if not isinstance(raw_item, dict):
            continue

        lines = _table_to_lines(raw_item)
        if not lines:
            continue

        pages = raw_item.get("pages")
        if isinstance(pages, list):
            normalized_pages = [page for page in pages if isinstance(page, int)]
        else:
            page = raw_item.get("page")
            normalized_pages = [page] if isinstance(page, int) else [1]

        text = "\n".join(lines).strip()
        if not text:
            continue

        for page in normalized_pages or [1]:
            page_queues.setdefault(page, []).append(text)

        table_entries.append(
            {
                "pages": normalized_pages or [1],
                "text": text,
                "rows": lines,
                "exact_hash": hash_text(compact_raw_text(text)),
            }
        )

    return page_queues, table_entries


def _table_to_lines(table: dict[str, Any]) -> list[str]:
    """将表格的多种表示形式统一转换为行列表。"""
    rows = table.get("rows")
    if isinstance(rows, list) and rows:
        result = []
        for row in rows:
            if isinstance(row, dict):
                values = [str(value).strip() for value in row.values() if str(value).strip()]
            elif isinstance(row, list):
                values = [str(value).strip() for value in row if str(value).strip()]
            else:
                values = [str(row).strip()]
            if values:
                result.append(" | ".join(values))
        if result:
            return result

    records = table.get("records")
    if isinstance(records, list) and records:
        result = []
        for record in records:
            if not isinstance(record, dict):
                continue
            values = [str(value).strip() for value in record.values() if str(value).strip()]
            if values:
                result.append(" | ".join(values))
        if result:
            return result

    for key in ("block_content", "html"):
        candidate = str(table.get(key) or "").strip()
        if "<table" in candidate.lower():
            parser = _TableHTMLParser()
            try:
                parser.feed(candidate)
                parser.close()
            except Exception:
                parser = None
            if parser and parser.rows:
                return [
                    " | ".join(cell for cell in row if cell)
                    for row in parser.rows
                    if any(cell for cell in row)
                ]

    for key in ("raw_text", "text", "block_content"):
        candidate = normalize_plain_text(table.get(key) or "")
        if not candidate:
            continue
        lines = [line.strip() for line in SPLIT_LINE_PATTERN.split(candidate) if line.strip()]
        if lines:
            return lines
        return [candidate]

    return []


def _fallback_text(container: dict[str, Any]) -> str:
    """从容器中提取回退全文文本。"""
    for key in ("content", "text", "full_text"):
        value = container.get(key)
        if isinstance(value, str) and value.strip():
            return normalize_plain_text(value)

    pages = container.get("pages")
    if isinstance(pages, list):
        parts = []
        for item in pages:
            if isinstance(item, dict):
                text = normalize_plain_text(item.get("text") or "")
                if text:
                    parts.append(text)
        return "\n".join(parts).strip()

    return ""