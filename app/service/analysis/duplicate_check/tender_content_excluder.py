# -*- coding: utf-8 -*-
"""Remove text that occurs anywhere in the associated tender document."""
import html
import re
from collections import defaultdict
from typing import Any

from .constants import COMMON_DUPLICATE_HEADER_TOKENS
from .block_extractor import _TableHTMLParser
from .text_utils import compact_raw_text, hash_text, normalize_plain_text


MIN_SHARED_LENGTH = 10
_FIXED_LABELS = set(COMMON_DUPLICATE_HEADER_TOKENS) | {
    "序号", "名称", "单位", "数量", "技术要求", "技术参数", "指标要求",
    "投标响应", "响应内容", "响应情况", "偏离情况", "偏离说明", "备注",
    "规格型号", "品牌", "金额", "税率", "页码",
}


def _normalized_with_offsets(text: str) -> tuple[str, list[int]]:
    """Whitespace-insensitive key and positions in the displayed text."""
    chars: list[str] = []
    offsets: list[int] = []
    for offset, char in enumerate(text):
        if not char.isspace():
            chars.append(char)
            offsets.append(offset)
    return "".join(chars), offsets


def _table_cells(table: dict[str, Any]) -> list[str]:
    cells: list[str] = []
    for key in ("headers", "rows", "records"):
        values = table.get(key)
        if not isinstance(values, list):
            continue
        for row in values:
            if isinstance(row, dict):
                cells.extend(str(value) for value in row.values() if value is not None)
            elif isinstance(row, list):
                cells.extend(str(value) for value in row if value is not None)
            elif row is not None:
                cells.extend(re.split(r"\s*[|｜]\s*", str(row)))
    return cells


def tender_text_units(payload: dict[str, Any]) -> list[str]:
    """Index every available OCR source, including text outside tender templates."""
    container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(container, dict):
        return []
    units: list[str] = []
    by_page: dict[int, list[str]] = defaultdict(list)
    for section in container.get("layout_sections") or []:
        if not isinstance(section, dict):
            continue
        for key in ("text", "raw_text", "markdown"):
            value = section.get(key)
            if not value:
                continue
            text = str(value)
            units.append(text)
            if key == "text" and isinstance(section.get("page"), int):
                by_page[section["page"]].append(text)
    units.extend("\n".join(values) for values in by_page.values() if values)
    for page in container.get("pages") or []:
        if isinstance(page, dict):
            for key in ("text", "raw_text", "markdown"):
                if page.get(key):
                    units.append(str(page[key]))
    for key in ("logical_tables", "table_sections"):
        for table in container.get(key) or []:
            if not isinstance(table, dict):
                continue
            units.extend(_table_cells(table))
            for text_key in ("text", "raw_text", "block_content"):
                if table.get(text_key):
                    units.append(str(table[text_key]))
            table_html = str(table.get("html") or table.get("block_content") or "")
            if "<table" in table_html.lower():
                parser = _TableHTMLParser()
                try:
                    parser.feed(table_html)
                    parser.close()
                    units.extend(cell for row in parser.rows for cell in row if cell)
                except Exception:
                    pass
    recognition = container.get("recognition")
    if isinstance(recognition, dict):
        units.extend(str(recognition[key]) for key in ("content", "raw_text", "text", "full_text", "markdown") if recognition.get(key))
    units.extend(str(container[key]) for key in ("content", "raw_text", "text", "full_text", "markdown") if container.get(key))
    return units


class TenderContentIndex:
    """Exact substring index; a source position never implies a bid position."""

    def __init__(self, units: list[str]) -> None:
        self.sources: list[str] = []
        self.anchors: dict[str, list[tuple[int, int]]] = defaultdict(list)
        self.short_labels: set[str] = set()
        seen: set[str] = set()
        for raw in units:
            text = normalize_plain_text(html.unescape(str(raw or "")))
            key, _ = _normalized_with_offsets(text)
            if not key or key in seen:
                continue
            seen.add(key)
            if key in _FIXED_LABELS:
                self.short_labels.add(key)
            if len(key) < MIN_SHARED_LENGTH:
                continue
            source_id = len(self.sources)
            self.sources.append(key)
            for offset in range(len(key) - MIN_SHARED_LENGTH + 1):
                self.anchors[key[offset:offset + MIN_SHARED_LENGTH]].append((source_id, offset))

    def __bool__(self) -> bool:
        return bool(self.sources or self.short_labels)

    def _matched_ranges(self, key: str) -> list[tuple[int, int]]:
        ranges: list[tuple[int, int]] = []
        covered_until = 0
        for start in range(len(key) - MIN_SHARED_LENGTH + 1):
            # A new match extending beyond a covered range must still contain
            # one of its final nine starting positions.
            if start < covered_until - (MIN_SHARED_LENGTH - 1):
                continue
            anchors = self.anchors.get(key[start:start + MIN_SHARED_LENGTH], ())
            for source_id, source_offset in anchors:
                source = self.sources[source_id]
                left = start
                source_left = source_offset
                while left > 0 and source_left > 0 and key[left - 1] == source[source_left - 1]:
                    left -= 1
                    source_left -= 1
                right = start + MIN_SHARED_LENGTH
                source_right = source_offset + MIN_SHARED_LENGTH
                while right < len(key) and source_right < len(source) and key[right] == source[source_right]:
                    right += 1
                    source_right += 1
                ranges.append((left, right))
                covered_until = max(covered_until, right)
        if not ranges:
            return []
        ranges.sort()
        merged = [ranges[0]]
        for start, end in ranges[1:]:
            if start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        return merged

    def strip_fragment(self, value: str, *, structural: bool = False) -> str:
        text = normalize_plain_text(html.unescape(value))
        key, offsets = _normalized_with_offsets(text)
        if not key:
            return ""
        if structural and key in self.short_labels:
            return ""
        ranges = self._matched_ranges(key)
        if not ranges:
            return text
        pieces: list[str] = []
        cursor = 0
        for start, end in ranges:
            pieces.append(text[cursor:offsets[start]])
            cursor = offsets[end - 1] + 1
        pieces.append(text[cursor:])
        # Keep the two sides of a removed quote separate: joining them would
        # manufacture a sentence that never appeared in the bid document.
        retained = [normalize_plain_text(piece).strip(" |｜;；,，。.") for piece in pieces]
        return "\n".join(piece for piece in retained if piece)

    def strip_text(self, value: str, *, structured: bool = False) -> str:
        value = self.strip_fragment(value)
        if not structured or not value:
            return value
        lines: list[str] = []
        for line in value.splitlines():
            parts = re.split(r"\s*[|｜]\s*", line) if re.search(r"[|｜]", line) else [line]
            retained = [
                cleaned
                for part in parts
                if (cleaned := self.strip_fragment(part, structural=True))
            ]
            if retained:
                lines.append(" | ".join(retained) if len(parts) > 1 else retained[0])
        return "\n".join(lines)

    def strip_blocks_and_tables(
        self,
        blocks: list[dict[str, Any]],
        tables: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        cleaned_blocks: list[dict[str, Any]] = []
        for block in blocks:
            value = self.strip_text(str(block.get("text") or ""), structured=block.get("type") in {"table", "heading"})
            key = compact_raw_text(value)
            if not key:
                continue
            cleaned_blocks.append({**block, "text": value, "exact_key": key, "exact_hash": hash_text(key)})

        cleaned_tables: list[dict[str, Any]] = []
        for table in tables:
            rows = [
                value
                for row in table.get("rows") or []
                if (value := self.strip_text(str(row), structured=True))
            ]
            if not rows:
                continue
            value = "\n".join(rows)
            cleaned_tables.append({**table, "rows": rows, "text": value, "exact_hash": hash_text(compact_raw_text(value))})
        return cleaned_blocks, cleaned_tables
