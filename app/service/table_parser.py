from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import Any


_HEADER_KEYWORDS = (
    "序号",
    "条款内容",
    "合同条款号",
    "约定内容",
    "备注",
    "项目名称",
    "名称",
    "内容",
    "规格",
    "规格型号",
    "型号",
    "参数",
    "单位",
    "数量",
    "单价",
    "合价",
    "金额",
    "总价",
    "税率",
    "税额",
    "price",
    "item",
    "name",
    "spec",
    "model",
    "unit",
    "qty",
    "quantity",
    "amount",
    "remark",
)

_HEADER_KEYWORD_TOKENS = tuple(sorted({_normalize_key for _normalize_key in (
    re.sub(r"[\s_/|]+", "", str(item or "")).lower() for item in _HEADER_KEYWORDS
) if _normalize_key}, key=len, reverse=True))


class _HTMLTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[dict[str, Any]]] = []
        self._table_depth = 0
        self._current_row: list[dict[str, Any]] | None = None
        self._current_cell: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "table":
            self._table_depth += 1
            return
        if self._table_depth <= 0:
            return

        if normalized_tag == "tr":
            self._current_row = []
            return
        if normalized_tag in {"td", "th"}:
            if self._current_row is None:
                self._current_row = []
            attr_map = {str(key).lower(): value for key, value in attrs}
            self._current_cell = {
                "text_parts": [],
                "rowspan": _safe_positive_int(attr_map.get("rowspan"), default=1),
                "colspan": _safe_positive_int(attr_map.get("colspan"), default=1),
                "is_header": normalized_tag == "th",
            }
            return
        if normalized_tag == "br" and self._current_cell is not None:
            self._current_cell["text_parts"].append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        if normalized_tag in {"td", "th"}:
            if self._current_cell is None:
                return
            text = "".join(self._current_cell.pop("text_parts", []))
            self._current_row = self._current_row or []
            self._current_row.append(
                {
                    "text": text,
                    "rowspan": self._current_cell.get("rowspan", 1),
                    "colspan": self._current_cell.get("colspan", 1),
                    "is_header": bool(self._current_cell.get("is_header")),
                }
            )
            self._current_cell = None
            return
        if normalized_tag == "tr":
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            return
        if normalized_tag == "table" and self._table_depth > 0:
            self._table_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._table_depth <= 0 or self._current_cell is None:
            return
        self._current_cell["text_parts"].append(data)


def build_table_structure(
    *,
    html_parts: list[str] | None = None,
    markdown_parts: list[str] | None = None,
    cell_texts: list[str] | None = None,
    raw_text: str = "",
) -> dict[str, Any] | None:
    html_parts = _dedupe_text_parts(html_parts or [])
    markdown_parts = _dedupe_text_parts(markdown_parts or [])
    cell_texts = _dedupe_text_parts(cell_texts or [])

    for parser_name, parser_input in (
        ("html", "\n\n".join(html_parts)),
        ("markdown", "\n\n".join(markdown_parts)),
        ("cell_texts", "\n".join(cell_texts)),
        ("text", raw_text),
    ):
        if not str(parser_input or "").strip():
            continue
        structure = _parse_table_structure(parser_name, str(parser_input or ""))
        if structure is not None:
            return structure
    return None


def build_logical_tables(layout_sections: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not isinstance(layout_sections, list):
        return []

    logical_tables: list[dict[str, Any]] = []
    for section_index, section in enumerate(layout_sections):
        if not isinstance(section, dict):
            continue
        if str(section.get("type") or "").strip().lower() != "table":
            continue

        structure = section.get("table_structure")
        if not isinstance(structure, dict):
            continue
        rows = structure.get("rows")
        if not isinstance(rows, list) or not rows:
            continue

        page_no = _coerce_int(section.get("page"))
        current = {
            "id": "",
            "pages": [page_no] if page_no is not None else [],
            "source_section_indexes": [section_index],
            "parser_chain": [str(structure.get("parser") or "unknown")],
            "column_count": int(structure.get("column_count") or 0),
            "header_row_count": int(structure.get("header_row_count") or 0),
            "headers": [str(item or "") for item in (structure.get("headers") or [])],
            "rows": [list(map(lambda item: str(item or ""), row)) for row in rows if isinstance(row, list)],
            "records": [dict(record) for record in (structure.get("records") or []) if isinstance(record, dict)],
            "header_signature": str(structure.get("header_signature") or ""),
            "continued": False,
        }
        current["row_count"] = len(current["rows"])
        current["data_row_count"] = len(current["records"])

        if logical_tables and _can_merge_logical_tables(logical_tables[-1], current):
            _merge_logical_table(logical_tables[-1], current)
            continue

        logical_tables.append(current)

    for index, table in enumerate(logical_tables, start=1):
        table["id"] = f"table_{index}"
    return logical_tables


def _parse_table_structure(parser_name: str, payload: str) -> dict[str, Any] | None:
    if parser_name == "html":
        return _parse_html_table_structure(payload)
    if parser_name == "markdown":
        return _parse_markdown_table_structure(payload)
    return _parse_plain_text_table_structure(payload, parser_name=parser_name)


def _parse_html_table_structure(payload: str) -> dict[str, Any] | None:
    parser = _HTMLTableParser()
    try:
        parser.feed(payload)
        parser.close()
    except Exception:
        return None
    return _build_structured_table_from_raw_rows(parser.rows, parser_name="html")


def _parse_markdown_table_structure(payload: str) -> dict[str, Any] | None:
    lines = [line.strip() for line in str(payload or "").splitlines() if line.strip()]
    if len(lines) < 2:
        return None

    candidate_rows = [_split_pipe_row(line) for line in lines if line.count("|") >= 2]
    candidate_rows = [row for row in candidate_rows if len(row) >= 2]
    if len(candidate_rows) < 2:
        return None

    raw_rows: list[list[dict[str, Any]]] = []
    forced_header_rows = 0
    separator_index = next((idx for idx, row in enumerate(candidate_rows) if _is_markdown_separator_row(row)), None)
    if separator_index == 1:
        raw_rows.append([_make_raw_cell(text, is_header=True) for text in candidate_rows[0]])
        forced_header_rows = 1
        iter_rows = candidate_rows[2:]
    else:
        iter_rows = [row for row in candidate_rows if not _is_markdown_separator_row(row)]

    for row in iter_rows:
        raw_rows.append([_make_raw_cell(text) for text in row])

    return _build_structured_table_from_raw_rows(
        raw_rows,
        parser_name="markdown",
        forced_header_rows=forced_header_rows if forced_header_rows > 0 else None,
    )


def _parse_plain_text_table_structure(payload: str, *, parser_name: str) -> dict[str, Any] | None:
    normalized = _normalize_text(payload, preserve_lines=True)
    if not normalized:
        return None

    raw_rows: list[list[dict[str, Any]]] = []
    for line in normalized.splitlines():
        cells = _split_text_row(line)
        if len(cells) < 2:
            continue
        raw_rows.append([_make_raw_cell(text) for text in cells])

    return _build_structured_table_from_raw_rows(raw_rows, parser_name=parser_name)


def _build_structured_table_from_raw_rows(
    raw_rows: list[list[dict[str, Any]]],
    *,
    parser_name: str,
    forced_header_rows: int | None = None,
) -> dict[str, Any] | None:
    if not raw_rows:
        return None

    grid: list[list[dict[str, Any] | None]] = []
    for row_index, raw_row in enumerate(raw_rows):
        while len(grid) <= row_index:
            grid.append([])
        col_index = 0
        for raw_cell in raw_row:
            if not isinstance(raw_cell, dict):
                continue
            row_slots = grid[row_index]
            while col_index < len(row_slots) and row_slots[col_index] is not None:
                col_index += 1

            text = _normalize_text(raw_cell.get("text") or "", preserve_lines=True)
            rowspan = _safe_positive_int(raw_cell.get("rowspan"), default=1)
            colspan = _safe_positive_int(raw_cell.get("colspan"), default=1)
            is_header = bool(raw_cell.get("is_header"))

            for target_row_index in range(row_index, row_index + rowspan):
                while len(grid) <= target_row_index:
                    grid.append([])
                target_row = grid[target_row_index]
                if len(target_row) < col_index + colspan:
                    target_row.extend([None] * (col_index + colspan - len(target_row)))
                for target_col_index in range(col_index, col_index + colspan):
                    if target_row[target_col_index] is not None:
                        continue
                    target_row[target_col_index] = {
                        "text": text,
                        "row": target_row_index,
                        "col": target_col_index,
                        "origin_row": row_index,
                        "origin_col": col_index,
                        "rowspan": rowspan if (target_row_index == row_index and target_col_index == col_index) else 0,
                        "colspan": colspan if (target_row_index == row_index and target_col_index == col_index) else 0,
                        "is_header": is_header,
                        "is_origin": target_row_index == row_index and target_col_index == col_index,
                    }
            col_index += colspan

    column_count = max(
        (
            index + 1
            for row in grid
            for index, cell in enumerate(row)
            if isinstance(cell, dict) and str(cell.get("text") or "").strip()
        ),
        default=max((len(row) for row in grid), default=0),
    )
    if column_count <= 0:
        return None

    rows: list[list[str]] = []
    spans: list[dict[str, Any]] = []
    header_flags: list[list[bool]] = []
    for row_index, row in enumerate(grid):
        padded = list(row[:column_count])
        if len(padded) < column_count:
            padded.extend([None] * (column_count - len(padded)))

        row_values: list[str] = []
        row_header_flags: list[bool] = []
        for col_index, cell in enumerate(padded):
            if not isinstance(cell, dict):
                row_values.append("")
                row_header_flags.append(False)
                continue

            cell_text = str(cell.get("text") or "")
            row_values.append(cell_text)
            row_header_flags.append(bool(cell.get("is_header")))
            if bool(cell.get("is_origin")) and (
                int(cell.get("rowspan") or 0) > 1 or int(cell.get("colspan") or 0) > 1
            ):
                spans.append(
                    {
                        "row": row_index,
                        "col": col_index,
                        "text": cell_text,
                        "rowspan": int(cell.get("rowspan") or 1),
                        "colspan": int(cell.get("colspan") or 1),
                        "is_header": bool(cell.get("is_header")),
                    }
                )

        if any(value.strip() for value in row_values):
            rows.append(row_values)
            header_flags.append(row_header_flags)

    if not rows:
        return None

    rows, header_flags, spans = _compress_empty_columns(rows, header_flags, spans)
    if not rows or max((len(row) for row in rows), default=0) <= 0:
        return None

    header_row_count = (
        min(max(int(forced_header_rows or 0), 0), len(rows))
        if forced_header_rows is not None
        else _detect_header_row_count(rows, header_flags)
    )
    headers = _build_headers(rows, header_row_count)
    records = _build_records(rows, headers, header_row_count)
    return {
        "parser": parser_name,
        "row_count": len(rows),
        "column_count": max((len(row) for row in rows), default=0),
        "header_row_count": header_row_count,
        "headers": headers,
        "rows": rows,
        "records": records,
        "data_row_count": len(records),
        "spans": spans,
        "header_signature": _build_header_signature(headers),
    }


def _split_text_row(line: str) -> list[str]:
    normalized = _normalize_text(line, preserve_lines=True)
    if not normalized:
        return []
    if "\t" in normalized:
        parts = [_normalize_text(item) for item in normalized.split("\t")]
    elif normalized.count("|") >= 2:
        parts = [_normalize_text(item) for item in _split_pipe_row(normalized)]
    else:
        parts = [_normalize_text(item) for item in re.split(r"\s{2,}", normalized)]

    while parts and not parts[-1]:
        parts.pop()
    return parts


def _split_pipe_row(line: str) -> list[str]:
    stripped = str(line or "").strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [item.strip() for item in stripped.split("|")]


def _is_markdown_separator_row(cells: list[str]) -> bool:
    cleaned = [str(item or "").strip() for item in cells]
    if not cleaned:
        return False
    return all(bool(re.fullmatch(r":?-{3,}:?", item)) for item in cleaned if item)


def _detect_header_row_count(rows: list[list[str]], header_flags: list[list[bool]]) -> int:
    if not rows:
        return 0

    header_count = 0
    for row_index, row in enumerate(rows[:3]):
        flags = header_flags[row_index] if row_index < len(header_flags) else []
        if any(flags):
            header_count += 1
            continue
        if row_index > 0 and _looks_like_data_row(row):
            break
        if _row_has_header_signal(row):
            header_count += 1
            continue
        break
    if header_count == 0 and _looks_like_first_row_header(rows):
        return 1
    return header_count


def _row_has_header_signal(row: list[str]) -> bool:
    normalized_cells = [_normalize_header_token(item) for item in row if str(item or "").strip()]
    if len(normalized_cells) < 2:
        return False

    strong_hits = sum(1 for cell in normalized_cells if _header_cell_score(cell) >= 2)
    weak_hits = sum(1 for cell in normalized_cells if _header_cell_score(cell) >= 1)
    non_numeric = sum(1 for cell in normalized_cells if not re.fullmatch(r"[\d\W_]+", cell))
    short_cells = sum(1 for cell in normalized_cells if len(cell) <= 12)
    return (
        strong_hits >= 2
        or (
            strong_hits >= 1
            and weak_hits >= max(2, len(normalized_cells) - 1)
            and short_cells >= max(2, len(normalized_cells) - 1)
            and non_numeric >= 2
        )
    )


def _looks_like_data_row(row: list[str]) -> bool:
    non_empty_cells = [str(item or "").strip() for item in row if str(item or "").strip()]
    if len(non_empty_cells) < 2:
        return False

    first_cell = non_empty_cells[0]
    if re.fullmatch(r"\d+(?:\.\d+)?", first_cell):
        return True

    amount_like_count = sum(
        1
        for cell in non_empty_cells
        if re.search(r"(?:\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)", cell)
    )
    header_like_count = sum(1 for cell in non_empty_cells if _header_cell_score(_normalize_header_token(cell)) >= 1)
    if amount_like_count >= 2 and header_like_count <= 1:
        return True

    if any(token in "".join(non_empty_cells) for token in ("姓名", "日历天", "个月", "负责人", "须知前附表", "%")):
        return True
    return False


def _looks_like_first_row_header(rows: list[list[str]]) -> bool:
    if len(rows) < 2:
        return False

    first_row = [str(item or "").strip() for item in rows[0] if str(item or "").strip()]
    second_row = [str(item or "").strip() for item in rows[1] if str(item or "").strip()]
    if len(first_row) < 2 or len(second_row) < 2:
        return False

    first_row_numeric = sum(1 for item in first_row if re.search(r"\d", item))
    second_row_numeric = sum(1 for item in second_row if re.search(r"\d", item))
    short_cells = sum(1 for item in first_row if len(item) <= 16)
    return first_row_numeric == 0 and second_row_numeric >= 1 and short_cells >= max(2, len(first_row) - 1)


def _header_cell_score(normalized_cell: str) -> int:
    if not normalized_cell:
        return 0
    if normalized_cell in _HEADER_KEYWORD_TOKENS:
        return 2
    if any(
        normalized_cell.startswith(keyword)
        for keyword in _HEADER_KEYWORD_TOKENS
        if len(keyword) >= 3
    ):
        return 2
    return 0


def _compress_empty_columns(
    rows: list[list[str]],
    header_flags: list[list[bool]],
    spans: list[dict[str, Any]],
) -> tuple[list[list[str]], list[list[bool]], list[dict[str, Any]]]:
    if not rows:
        return rows, header_flags, spans

    column_count = max((len(row) for row in rows), default=0)
    if column_count <= 0:
        return rows, header_flags, spans

    keep_indexes = [
        col_index
        for col_index in range(column_count)
        if any(
            col_index < len(row) and str(row[col_index] or "").strip()
            for row in rows
        )
    ]
    if len(keep_indexes) == column_count:
        return rows, header_flags, spans

    index_map = {old_index: new_index for new_index, old_index in enumerate(keep_indexes)}
    compressed_rows = [
        [row[col_index] if col_index < len(row) else "" for col_index in keep_indexes]
        for row in rows
    ]
    compressed_flags = [
        [row_flags[col_index] if col_index < len(row_flags) else False for col_index in keep_indexes]
        for row_flags in header_flags
    ]
    compressed_spans = []
    for span in spans:
        old_col = int(span.get("col", -1))
        if old_col not in index_map:
            continue
        updated = dict(span)
        updated["col"] = index_map[old_col]
        compressed_spans.append(updated)
    return compressed_rows, compressed_flags, compressed_spans


def _build_headers(rows: list[list[str]], header_row_count: int) -> list[str]:
    column_count = max((len(row) for row in rows), default=0)
    if column_count <= 0:
        return []
    if header_row_count <= 0:
        return [f"col_{index + 1}" for index in range(column_count)]

    headers: list[str] = []
    for col_index in range(column_count):
        parts: list[str] = []
        for row_index in range(min(header_row_count, len(rows))):
            if col_index >= len(rows[row_index]):
                continue
            text = str(rows[row_index][col_index] or "").strip()
            if text and text not in parts:
                parts.append(text)
        headers.append(" / ".join(parts) if parts else f"col_{col_index + 1}")
    return headers


def _build_records(rows: list[list[str]], headers: list[str], header_row_count: int) -> list[dict[str, str]]:
    if not rows:
        return []

    column_count = max((len(row) for row in rows), default=0)
    resolved_headers = _ensure_unique_headers(headers or [f"col_{index + 1}" for index in range(column_count)])
    start_index = header_row_count if 0 <= header_row_count < len(rows) else 0

    records: list[dict[str, str]] = []
    for row in rows[start_index:]:
        padded = list(row[: len(resolved_headers)])
        if len(padded) < len(resolved_headers):
            padded.extend([""] * (len(resolved_headers) - len(padded)))
        if not any(str(item or "").strip() for item in padded):
            continue
        records.append({header: str(value or "") for header, value in zip(resolved_headers, padded)})
    return records


def _build_header_signature(headers: list[str]) -> str:
    normalized = [_normalize_header_token(item) for item in headers if _normalize_header_token(item)]
    return "|".join(normalized)


def _ensure_unique_headers(headers: list[str]) -> list[str]:
    resolved: list[str] = []
    seen: dict[str, int] = {}
    for index, header in enumerate(headers):
        base = str(header or "").strip() or f"col_{index + 1}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        resolved.append(base if count == 1 else f"{base}_{count}")
    return resolved


def _can_merge_logical_tables(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_pages = [page for page in left.get("pages", []) if isinstance(page, int)]
    right_pages = [page for page in right.get("pages", []) if isinstance(page, int)]
    if not left_pages or not right_pages or right_pages[0] != left_pages[-1] + 1:
        return False

    left_columns = int(left.get("column_count") or 0)
    right_columns = int(right.get("column_count") or 0)
    if left_columns <= 0 or right_columns <= 0 or abs(left_columns - right_columns) > 1:
        return False

    left_signature = str(left.get("header_signature") or "")
    right_signature = str(right.get("header_signature") or "")
    if left_signature and right_signature and left_signature == right_signature:
        return True
    return _headers_match(left.get("headers") or [], right.get("headers") or [])


def _merge_logical_table(target: dict[str, Any], source: dict[str, Any]) -> None:
    source_rows = [list(row) for row in source.get("rows", []) if isinstance(row, list)]
    if not source_rows:
        return

    rows_to_add = source_rows
    if _headers_match(target.get("headers") or [], source.get("headers") or []):
        header_row_count = int(source.get("header_row_count") or 0)
        if header_row_count > 0:
            rows_to_add = source_rows[header_row_count:]

    target["rows"].extend(rows_to_add)
    target["records"] = _build_records(
        target.get("rows") or [],
        target.get("headers") or [],
        int(target.get("header_row_count") or 0),
    )
    target["row_count"] = len(target.get("rows") or [])
    target["data_row_count"] = len(target.get("records") or [])
    target["continued"] = True

    for page in source.get("pages", []):
        if page not in target["pages"]:
            target["pages"].append(page)
    for section_index in source.get("source_section_indexes", []):
        if section_index not in target["source_section_indexes"]:
            target["source_section_indexes"].append(section_index)
    for parser_name in source.get("parser_chain", []):
        if parser_name not in target["parser_chain"]:
            target["parser_chain"].append(parser_name)


def _headers_match(left: list[str], right: list[str]) -> bool:
    if not left or not right:
        return False
    left_normalized = [_normalize_header_token(item) for item in left if _normalize_header_token(item)]
    right_normalized = [_normalize_header_token(item) for item in right if _normalize_header_token(item)]
    if not left_normalized or not right_normalized:
        return False
    if left_normalized == right_normalized:
        return True

    overlap = sum(1 for item in left_normalized if item in right_normalized)
    baseline = max(1, min(len(left_normalized), len(right_normalized)))
    return overlap >= max(2, baseline - 1)


def _normalize_header_token(value: str) -> str:
    normalized = _normalize_text(value)
    normalized = re.sub(r"[\s_/|]+", "", normalized)
    return normalized.lower()


def _normalize_text(text: str, *, preserve_lines: bool = False) -> str:
    normalized = html.unescape(str(text or ""))
    if preserve_lines:
        normalized = re.sub(r"\r\n?", "\n", normalized)
        normalized = re.sub(r"<br\s*/?>", "\n", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>", "\n", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"</?(td|th)[^>]*>", "\t", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"[^\S\n\t]+", " ", normalized)
        normalized = re.sub(r" *\t *", "\t", normalized)
        normalized = re.sub(r"[ \t]*\n[ \t]*", "\n", normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        lines = []
        for line in normalized.splitlines():
            cells = [re.sub(r" {2,}", " ", cell).strip() for cell in line.split("\t")]
            cleaned = "\t".join(cells).strip()
            lines.append(cleaned)
        return "\n".join(line for line in lines if line)

    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _dedupe_text_parts(parts: list[str]) -> list[str]:
    deduped: list[str] = []
    seen = set()
    for part in parts:
        normalized = str(part or "").strip()
        if normalized and normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return deduped


def _make_raw_cell(text: str, *, is_header: bool = False) -> dict[str, Any]:
    return {"text": text, "rowspan": 1, "colspan": 1, "is_header": is_header}


def _safe_positive_int(value: Any, *, default: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
