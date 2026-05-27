# itemized/table_extractor.py
"""
分项报价 - 表格提取 Mixin

负责从 layout_sections 中定位锚点后的结构化表格序列，
将 logical_tables 展开为逐行文本，并处理跨页续表拼接。
"""

from __future__ import annotations

import re
from typing import Any

from app.service.analysis.itemized.html_parser import _TableHTMLParser
from app.service.analysis.location_utils import normalize_bbox


class TableExtractorMixin:

    # 需要由使用该 Mixin 的类提供的常量
    ITEM_SECTION_ANCHORS: tuple
    TOTAL_SECTION_ANCHORS: tuple

    # 结构化容器访问
    def _get_layout_sections(self, payload: dict | None) -> list[dict]:
        """从 OCR 结果中提取版面分区列表。"""
        container = self._get_structured_container(payload)
        if not isinstance(container, dict):
            return []
        layout_sections = container.get("layout_sections")
        if not isinstance(layout_sections, list):
            return []
        return [section for section in layout_sections if isinstance(section, dict)]

    def _get_logical_tables(self, payload: dict | None) -> list[dict]:
        """从 OCR 结果中提取逻辑表格列表。"""
        container = self._get_structured_container(payload)
        if not isinstance(container, dict):
            return []
        logical_tables = container.get("logical_tables")
        if not isinstance(logical_tables, list):
            return []
        return [table for table in logical_tables if isinstance(table, dict)]

    def _get_structured_container(self, payload: dict | None) -> dict | None:
        """兼容直接字典和带 data 包裹的结构化 OCR 结果。"""
        if not isinstance(payload, dict):
            return None
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload

    # 布局表格定位与提取
    def _find_layout_table_sections(
        self, payload: dict | None, anchors: tuple[str, ...]
    ) -> list[dict]:
        """从 layout_sections 中定位锚点后的表格序列，优先使用结构化表格结果。"""
        layout_sections = self._get_layout_sections(payload)
        logical_tables = self._get_logical_tables(payload)
        if not layout_sections:
            return []

        sections = []
        for idx, section in enumerate(layout_sections):
            anchor_text = self._get_section_text(section)
            if not anchor_text:
                continue

            matched_anchor = next(
                (anchor for anchor in anchors if anchor in anchor_text), None
            )
            if not matched_anchor or not self._is_anchor_line(
                anchor_text, matched_anchor
            ):
                continue

            anchor_page = (
                int(section.get("page"))
                if isinstance(section.get("page"), int)
                else None
            )
            lines = []
            pages = []
            logical_table_refs = []
            bboxes = []
            table_started = False

            for follower in layout_sections[idx + 1:]:
                section_type = str(follower.get("type") or "").lower()
                section_text = self._get_section_text(follower)
                if not section_text:
                    continue

                if not table_started:
                    if section_type == "table":
                        follower_page = (
                            int(follower.get("page"))
                            if isinstance(follower.get("page"), int)
                            else None
                        )
                        if not self._is_layout_table_near_anchor(
                            anchor_page, follower_page
                        ):
                            break
                        table_started = True
                        payload = self._extract_layout_table_payload(
                            follower, logical_tables
                        )
                        lines.extend(payload["lines"])
                        pages.extend(payload["pages"])
                        logical_table_refs.extend(payload["logical_table_refs"])
                        bboxes.extend(payload.get("bboxes") or [])
                        continue
                    if self._matches_other_anchor(section_text, anchors):
                        break
                    if self._is_heading_line(section_text):
                        break
                    continue

                if section_type == "table":
                    if not self._should_attach_following_layout_table(section_text):
                        continue
                    payload = self._extract_layout_table_payload(
                        follower, logical_tables
                    )
                    lines.extend(payload["lines"])
                    pages.extend(payload["pages"])
                    logical_table_refs.extend(payload["logical_table_refs"])
                    bboxes.extend(payload.get("bboxes") or [])
                    continue
                if self._is_layout_bridge_text(section_text):
                    continue
                break

            if not lines:
                continue
            if (
                matched_anchor in self.TOTAL_SECTION_ANCHORS
                and self._looks_like_financial_receipt_section(lines)
            ):
                continue

            deduped_pages = []
            seen_pages = set()
            for page in pages:
                if page in seen_pages or page is None:
                    continue
                seen_pages.add(page)
                deduped_pages.append(page)
            deduped_table_refs = list(
                dict.fromkeys(str(ref) for ref in logical_table_refs if ref)
            )

            sections.append(
                {
                    "anchor": matched_anchor,
                    "lines": lines,
                    "start": idx,
                    "end": idx + len(lines),
                    "score": len(lines),
                    "source": "layout_table_sequence",
                    "pages": deduped_pages,
                    "bbox": self._merge_table_bboxes(bboxes),
                    "logical_table_refs": deduped_table_refs,
                    "section_id": (
                        f"layout:{matched_anchor}:{idx}:{'-'.join(str(page) for page in deduped_pages)}"
                        if deduped_pages
                        else f"layout:{matched_anchor}:{idx}"
                    ),
                }
            )

        sections.sort(key=lambda item: item.get("start", 0))
        return self._dedupe_sections(sections)

    @staticmethod
    def _merge_table_bboxes(values: list[Any]) -> list[float] | None:
        bboxes = [normalize_bbox(value) for value in values]
        bboxes = [bbox for bbox in bboxes if bbox]
        if not bboxes:
            return None
        return [
            min(float(bbox[0]) for bbox in bboxes),
            min(float(bbox[1]) for bbox in bboxes),
            max(float(bbox[2]) for bbox in bboxes),
            max(float(bbox[3]) for bbox in bboxes),
        ]

    def _is_layout_table_near_anchor(
        self, anchor_page: int | None, table_page: int | None
    ) -> bool:
        """限制标题与首张表的页码距离，避免把远处附件表误挂到当前锚点。"""
        if anchor_page is None or table_page is None:
            return True
        max_gap = int(getattr(self, "LAYOUT_TABLE_START_PAGE_GAP", 2) or 2)
        return abs(table_page - anchor_page) <= max_gap

    def _looks_like_financial_receipt_section(self, lines: list[str]) -> bool:
        """识别投标保证金回单等金融凭证，避免误当成报价总价表。"""
        combined = self._normalize_label_key(" ".join(lines))
        receipt_hints = (
            "付款人",
            "收款人",
            "开户银行",
            "交易时间",
            "交易流水号",
            "投标保证金",
            "账号",
            "网上汇款",
        )
        hint_hits = sum(1 for hint in receipt_hints if hint in combined)
        return hint_hits >= 3

    def _prioritize_item_sections(self, sections: list[dict]) -> list[dict]:
        """优先保留更像正式分项报价表的核心区段。"""
        if not sections:
            return sections
        primary_sections = [
            section
            for section in sections
            if str(section.get("anchor") or "") in self.PRIMARY_ITEM_SECTION_ANCHORS
        ]
        return primary_sections or sections

    def _extract_layout_table_payload(
        self, section: dict, logical_tables: list[dict]
    ) -> dict:
        """将 layout 表格区段转换为逐行文本，必要时绑定对应 logical table。"""
        logical_table_index = self._match_logical_table_index(
            section, logical_tables
        )
        if logical_table_index is not None:
            logical_lines = []
            logical_table_refs = []
            logical_table_pages = []
            logical_table_bboxes = [normalize_bbox(section.get("bbox") or section.get("box"))]
            for offset, logical_table in enumerate(
                self._collect_logical_table_sequence(
                    logical_tables, logical_table_index
                ),
            ):
                table_index = logical_table_index + offset
                logical_lines.extend(
                    self._logical_table_to_lines(
                        logical_table, include_headers=(offset == 0)
                    )
                )
                logical_table_refs.append(
                    self._logical_table_ref(logical_table, table_index)
                )
                logical_table_pages.extend(
                    self._get_logical_table_pages(logical_table)
                )
                logical_table_bboxes.append(
                    normalize_bbox(logical_table.get("bbox") or logical_table.get("box"))
                )
            if logical_lines:
                bboxes = [bbox for bbox in logical_table_bboxes if bbox]
                return {
                    "lines": logical_lines,
                    "logical_table_refs": logical_table_refs,
                    "pages": logical_table_pages,
                    "bboxes": bboxes,
                    "bbox": self._merge_table_bboxes(bboxes),
                }

        section_text = self._get_section_text(section)
        section_page = section.get("page")
        bbox = normalize_bbox(section.get("bbox") or section.get("box"))
        return {
            "lines": self._split_lines(self._normalize_text(section_text)),
            "logical_table_refs": [],
            "pages": [section_page] if isinstance(section_page, int) else [],
            "bboxes": [bbox] if bbox else [],
            "bbox": bbox,
        }

    def _extract_layout_table_lines(
        self, section: dict, logical_tables: list[dict]
    ) -> list[str]:
        """将 layout 表格区段转换为逐行文本（便捷方法）。"""
        return self._extract_layout_table_payload(section, logical_tables)["lines"]

    # 逻辑表格与区段匹配
    def _match_logical_table_index(
        self, section: dict, logical_tables: list[dict]
    ) -> int | None:
        """为某个 layout 表格区段匹配最可能对应的 logical table 起点。"""
        if not logical_tables:
            return None

        section_page = section.get("page")
        section_text = self._get_section_text(section)
        compact_section_text = re.sub(r"\s+", "", section_text)
        page_candidates = []
        for index, table in enumerate(logical_tables):
            if section_page in self._get_logical_table_pages(table):
                page_candidates.append((index, table))

        candidates = page_candidates or logical_tables
        best_table_index = None
        best_score = -1
        for candidate in candidates:
            if isinstance(candidate, tuple):
                table_index, table = candidate
            else:
                table_index, table = logical_tables.index(candidate), candidate
            score = 0
            headers = self._get_logical_table_headers(table)
            header_text = "".join(headers)
            if header_text:
                compact_header_text = re.sub(r"\s+", "", header_text)
                if (
                    compact_header_text
                    and compact_header_text in compact_section_text
                ):
                    score += 5

            for preview_line in self._logical_table_preview_lines(table)[:3]:
                for cell_text in self._split_lines(preview_line):
                    cell_text = str(cell_text).strip()
                    if len(cell_text) >= 2 and cell_text in section_text:
                        score += 1

            if score > best_score:
                best_score = score
                best_table_index = table_index

        return best_table_index if best_score > 0 else None

    def _collect_logical_table_sequence(
        self, logical_tables: list[dict], start_index: int
    ) -> list[dict]:
        """从一张 logical table 开始收集其后连续的跨页续表。"""
        collected = [logical_tables[start_index]]
        current_table = logical_tables[start_index]
        for next_table in logical_tables[start_index + 1:]:
            if not self._is_logical_table_continuation(current_table, next_table):
                break
            collected.append(next_table)
            current_table = next_table
        return collected

    def _is_logical_table_continuation(
        self, current_table: dict, next_table: dict
    ) -> bool:
        """判断后一张 logical table 是否属于当前表格的续页。"""
        if not isinstance(next_table, dict):
            return False
        if self._is_spare_parts_marker_text(
            " ".join(self._get_logical_table_headers(next_table))
        ):
            return False
        if not bool(next_table.get("continued")) and not self._looks_like_html_table_continuation(
            current_table, next_table
        ):
            return False

        current_pages = self._get_logical_table_pages(current_table)
        next_pages = self._get_logical_table_pages(next_table)
        if (
            current_pages
            and next_pages
            and next_pages[0] - current_pages[-1] > 3
        ):
            return False
        return True

    # 逻辑表格序列化为文本
    def _logical_table_to_lines(
        self, table: dict, *, include_headers: bool = True
    ) -> list[str]:
        """把逻辑表格统一展开为逐行文本，兼容 HTML 表与普通二维数组表。"""
        html_lines = self._logical_html_table_to_lines(
            table, include_headers=include_headers
        )
        if html_lines:
            return html_lines

        lines = []
        headers = self._get_logical_table_headers(table)
        if (
            include_headers
            and headers
            and not all(
                re.fullmatch(r"col_\d+", header, re.IGNORECASE)
                for header in headers
            )
        ):
            lines.append(" ".join(headers))

        rows = table.get("rows") or []
        header_row_count = int(table.get("header_row_count") or 0)
        start_index = min(len(rows), header_row_count)
        for row in rows[start_index:]:
            if not isinstance(row, list):
                continue
            cells = [str(cell).strip() for cell in row if str(cell).strip()]
            if not cells:
                continue
            lines.append(" ".join(cells))
        return lines

    def _get_logical_table_pages(self, table: dict) -> list[int]:
        """读取 logical table 关联的页码列表。"""
        pages = table.get("pages")
        if isinstance(pages, list):
            return [page for page in pages if isinstance(page, int)]
        page = table.get("page")
        return [page] if isinstance(page, int) else []

    def _get_logical_table_headers(self, table: dict) -> list[str]:
        """提取 logical table 表头，必要时从 HTML 表格内容里反推。"""
        headers = [
            str(header).strip()
            for header in (table.get("headers") or [])
            if str(header).strip()
        ]
        if headers and not all(
            re.fullmatch(r"col_\d+", header, re.IGNORECASE) for header in headers
        ):
            return headers

        row_headers = self._extract_row_based_table_headers(table)
        if row_headers:
            return row_headers

        html_rows = self._parse_html_table_rows(table)
        if len(html_rows) < 2:
            return headers

        header_row = self._extract_html_header_row(html_rows[1:])
        return header_row or headers

    def _extract_row_based_table_headers(self, table: dict) -> list[str]:
        """从 rows 列表中推测表头行。"""
        rows = [
            row for row in (table.get("rows") or [])[:3] if isinstance(row, list)
        ]
        if not rows:
            return []

        start_index = 0
        first_row = [
            str(cell).strip() for cell in rows[0] if str(cell).strip()
        ]
        unique_first_row = []
        for value in first_row:
            if value not in unique_first_row:
                unique_first_row.append(value)
        if len(unique_first_row) == 1 and len(rows) > 1:
            start_index = 1

        header_hints = (
            "序号", "编号", "名称", "项目", "功能", "内容", "描述", "说明",
            "参数", "规格", "型号", "金额", "总价", "合计",
        )
        for row in rows[start_index : start_index + 2]:
            values = [str(cell).strip() for cell in row if str(cell).strip()]
            if len(values) < 2:
                continue
            compact_values = {re.sub(r"\s+", "", value) for value in values}
            header_hits = sum(
                1 for value in compact_values if any(hint in value for hint in header_hints)
            )
            if header_hits >= 2 and not any(
                self._extract_money_candidates(value) for value in values
            ):
                return values
        return []

    def _logical_table_preview_lines(self, table: dict) -> list[str]:
        """生成表格预览文本，用于 layout 表格与 logical table 的匹配评分。"""
        html_lines = self._logical_html_table_to_lines(
            table, include_headers=True
        )
        if html_lines:
            return html_lines

        rows = table.get("rows") or []
        preview = []
        for row in rows[:3]:
            if not isinstance(row, list):
                continue
            cells = [str(cell).strip() for cell in row if str(cell).strip()]
            if cells:
                preview.append(" ".join(cells))
        return preview

    def _logical_table_ref(self, table: dict, index: int) -> str:
        """生成逻辑表格的唯一引用字符串。"""
        return str(table.get("id") or f"table_index_{index}")

    # HTML 表格处理
    def _logical_html_table_to_lines(
        self, table: dict, *, include_headers: bool = True
    ) -> list[str]:
        """把 HTML 形式的逻辑表格展平成逐行文本，并合并被拆断的续行。"""
        html_rows = self._parse_html_table_rows(table)
        if not html_rows:
            return []

        lines: list[str] = []
        data_start_index = 0

        title_row = self._extract_html_title_row(html_rows)
        if include_headers and title_row:
            lines.append(title_row)
            data_start_index = 1

        header_row = self._extract_html_header_row(html_rows[data_start_index:])
        if include_headers and header_row:
            lines.append(" ".join(header_row))

        header_offset = 1 if header_row else 0
        data_rows = html_rows[data_start_index + header_offset:]
        previous_line_index: int | None = None

        for row in data_rows:
            row_cells = [cell for cell in row if cell["text"]]
            if not row_cells:
                continue
            if self._is_html_bridge_row(row):
                continue

            row_text = " ".join(cell["text"] for cell in row_cells)
            if self._is_table_header_line(row_text):
                continue

            leading_placeholders = sum(
                1
                for cell in row[:2]
                if not cell["text"] or bool(cell.get("inherited"))
            )
            trailing_cell = next(
                (cell for cell in reversed(row) if cell["text"]), None
            )
            has_own_total = bool(
                trailing_cell
                and not trailing_cell.get("inherited")
                and len(self._extract_money_candidates(trailing_cell["text"])) == 1
            )

            rendered_cells = []
            for cell in row:
                text = cell["text"]
                if not text:
                    continue
                if cell.get("inherited") and len(rendered_cells) < 2:
                    continue
                rendered_cells.append(text)

            if not rendered_cells:
                continue

            rendered_line = " ".join(rendered_cells)
            if (
                leading_placeholders >= 2
                and not has_own_total
                and previous_line_index is not None
            ):
                lines[previous_line_index] = (
                    f"{lines[previous_line_index]} {rendered_line}".strip()
                )
                continue

            lines.append(rendered_line)
            previous_line_index = len(lines) - 1

        return lines

    def _parse_html_table_rows(self, table: dict) -> list[list[dict]]:
        """解析 logical table 中保存的 HTML 表格内容，并补齐跨行继承单元格。"""
        block_content = table.get("block_content")
        if (
            not isinstance(block_content, str)
            or "<table" not in block_content.lower()
        ):
            return []

        parser = _TableHTMLParser()
        parser.feed(block_content)
        raw_rows = parser.rows
        if not raw_rows:
            return []

        active_spans: dict[int, dict] = {}
        expanded_rows: list[list[dict]] = []
        max_columns = 0

        for raw_row in raw_rows:
            row: list[dict] = []
            column_index = 0

            def extend_active_spans() -> None:
                nonlocal column_index
                while column_index in active_spans:
                    span_info = active_spans[column_index]
                    row.append({"text": span_info["text"], "inherited": True})
                    span_info["remaining"] -= 1
                    if span_info["remaining"] <= 0:
                        del active_spans[column_index]
                    column_index += 1

            extend_active_spans()
            for cell in raw_row:
                extend_active_spans()
                text = str(cell.get("text") or "").strip()
                rowspan = max(1, int(cell.get("rowspan") or 1))
                colspan = max(1, int(cell.get("colspan") or 1))
                for offset in range(colspan):
                    row.append({"text": text, "inherited": False})
                    if rowspan > 1:
                        active_spans[column_index + offset] = {
                            "text": text,
                            "remaining": rowspan - 1,
                        }
                column_index += colspan

            extend_active_spans()
            max_columns = max(max_columns, len(row))
            expanded_rows.append(row)

        for row in expanded_rows:
            while len(row) < max_columns:
                row.append({"text": "", "inherited": False})
        return expanded_rows

    def _extract_html_title_row(self, rows: list[list[dict]]) -> str | None:
        """识别 HTML 表格顶部可能存在的单行标题。"""
        if not rows:
            return None
        values = [cell["text"] for cell in rows[0] if cell["text"]]
        unique_values = []
        for value in values:
            if value not in unique_values:
                unique_values.append(value)
        if len(unique_values) == 1 and len(unique_values[0]) >= 4:
            return unique_values[0]
        return None

    def _extract_html_header_row(self, rows: list[list[dict]]) -> list[str]:
        """识别 HTML 表格中真正的列表头。"""
        header_hints = (
            "序号", "编号", "名称", "项目", "功能", "内容", "描述", "说明",
            "参数", "规格", "型号", "金额", "总价", "合计",
        )
        for row in rows[:3]:
            values = [cell["text"] for cell in row if cell["text"]]
            if len(values) < 2:
                continue
            compact_values = {re.sub(r"\s+", "", value) for value in values}
            if {"序号", "单价", "合计"}.issubset(compact_values):
                return values
            header_hits = sum(
                1 for value in compact_values if any(hint in value for hint in header_hints)
            )
            if header_hits >= 2 and not any(
                self._extract_money_candidates(value) for value in values
            ):
                return values
        return []

    def _first_html_data_row(self, rows: list[list[dict]]) -> list[dict]:
        """返回第一行有效数据行，供续表识别逻辑使用。"""
        start_index = 1 if self._extract_html_title_row(rows) else 0
        if self._extract_html_header_row(rows[start_index:]):
            start_index += 1
        for row in rows[start_index:]:
            if any(cell["text"] for cell in row):
                if self._is_html_bridge_row(row):
                    continue
                return row
        return []

    def _html_table_contains_spare_parts(self, rows: list[list[dict]]) -> bool:
        """判断 HTML 表格是否已经进入备件等无需参与主校验的区域。"""
        for row in rows[:3]:
            row_text = "".join(cell["text"] for cell in row if cell["text"])
            if self._is_spare_parts_marker_text(row_text):
                return True
        return False

    def _is_html_bridge_row(self, row: list[dict]) -> bool:
        """判断 HTML 行是否只是分页桥接文本而非真实报价行。"""
        values = [cell["text"] for cell in row if cell["text"]]
        if not values:
            return True
        if len(values) > 1:
            return False

        value = values[0]
        if self._extract_money_candidates(value):
            return False
        if self._extract_row_serial(value):
            return False
        compact = re.sub(r"\s+", "", value)
        return bool(compact) and len(compact) <= 4

    def _looks_like_html_table_continuation(
        self, current_table: dict, next_table: dict
    ) -> bool:
        """根据 HTML 表头和首行数据特征判断两张表是否相邻续接。"""
        current_rows = self._parse_html_table_rows(current_table)
        next_rows = self._parse_html_table_rows(next_table)
        if not current_rows or not next_rows:
            return False
        if self._html_table_contains_spare_parts(next_rows):
            return False
        if self._extract_html_header_row(next_rows):
            return False

        next_first_data = self._first_html_data_row(next_rows)
        if not next_first_data:
            return False
        next_text = " ".join(
            cell["text"] for cell in next_first_data if cell["text"]
        )
        return bool(self._extract_money_candidates(next_text))

    # 通用辅助
    def _get_section_text(self, section: dict) -> str:
        """优先返回结构化分区中的原始文本内容。"""
        text = section.get("raw_text") or section.get("text")
        return text.strip() if isinstance(text, str) and text.strip() else ""
