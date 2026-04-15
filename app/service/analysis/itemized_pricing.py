"""
分项报价明细检查模块
负责人：江宇
"""
from __future__ import annotations

import argparse
import html
import json
import re
import sys
from collections import Counter
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path


# ---------------------------------------------------------------------------
# HTML 表格解析辅助
# ---------------------------------------------------------------------------
# 将 HTML 表格片段解析成保留行列合并信息的二维单元格结构。
class _TableHTMLParser(HTMLParser):

    # 初始化解析过程中用于暂存行、单元格和结果的状态。
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[dict]] = []
        self._current_row: list[dict] | None = None
        self._current_cell: dict | None = None

    # 在遇到行或单元格起始标签时创建对应的缓存结构。
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._current_row = []
            return
        if tag != "td" or self._current_row is None:
            return

        attr_map = {key: value for key, value in attrs}
        self._current_cell = {
            "text_parts": [],
            "rowspan": self._safe_span(attr_map.get("rowspan")),
            "colspan": self._safe_span(attr_map.get("colspan")),
        }

    # 在遇到结束标签时落盘当前单元格或整行数据。
    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._current_row is not None and self._current_cell is not None:
            text = html.unescape("".join(self._current_cell["text_parts"]))
            text = re.sub(r"\s+", " ", text).strip()
            self._current_row.append(
                {
                    "text": text,
                    "rowspan": self._current_cell["rowspan"],
                    "colspan": self._current_cell["colspan"],
                }
            )
            self._current_cell = None
            return

        if tag == "tr" and self._current_row is not None:
            self.rows.append(self._current_row)
            self._current_row = None

    # 累积当前单元格内的文本内容。
    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell["text_parts"].append(data)

    @staticmethod
    # 将 rowspan/colspan 安全转换为正整数，异常时回退为 1。
    def _safe_span(value: str | None) -> int:
        try:
            return max(1, int(str(value or "1")))
        except ValueError:
            return 1


# ---------------------------------------------------------------------------
# 分项报价主检查器
# ---------------------------------------------------------------------------
# 分项报价表规则检查器，负责抽取报价项并输出可解释的校验结果。
class ItemizedPricingChecker:

    # 报价表/总价表锚点与关键词配置
    ITEM_SECTION_ANCHORS = (
        "分项报价表",
        "供应清单",
        "货物清单",
        "工程量清单",
        "报价表",
        "投标价格表",
    )
    PRIMARY_ITEM_SECTION_ANCHORS = (
        "分项报价表",
        "报价表",
        "投标价格表",
    )
    TOTAL_SECTION_ANCHORS = (
        "开标一览表",
        "报价一览表",
        "投标总价",
        "总报价",
    )
    TOTAL_KEYWORDS = (
        "合计",
        "总计",
        "总价",
        "总报价",
        "投标总价",
        "单价合计",
        "金额合计",
        "报价合计",
    )
    OPENING_TOTAL_KEYWORDS = (
        "投标总价",
        "总报价",
        "开标一览表",
        "报价一览表",
    )
    SUBTOTAL_KEYWORDS = ("小计",)
    PREFERENTIAL_TOTAL_KEYWORDS = (
        "最终优惠价",
        "优惠价",
        "折后",
        "优惠后",
        "让利后",
        "下浮后",
    )
    PREFERENTIAL_TOTAL_LINE_WINDOW = 5
    RATE_KEYWORDS = (
        "下浮率",
        "优惠率",
        "折扣率",
        "折让率",
        "下浮",
    )
    UNIT_KEYWORDS = (
        "台",
        "套",
        "项",
        "个",
        "批",
        "次",
        "人",
        "年",
        "月",
        "日",
        "米",
        "吨",
        "樘",
        "组",
        "m2",
        "㎡",
    )
    ZERO_AMOUNT_KEYWORDS = (
        "包含",
        "免费",
        "赠送",
        "无偿",
        "不收费",
    )
    STRUCTURED_COLUMN_ALIASES = {
        "serial": ("序号", "编号"),
        "model": ("型号", "规格型号", "项目", "品名", "设备名称"),
        "description": ("说明", "名称", "内容", "参数", "配置", "描述"),
        "brand": ("品牌", "厂家", "厂商", "制造商", "生产厂家", "产地"),
        "quantity": ("数量",),
        "unit_price": ("单价", "投标单价", "报价单价", "综合单价", "含税单价"),
        "line_total": ("合计", "总价", "金额", "小计", "总额", "分项总价", "单项总价"),
    }
    MONEY_TOLERANCE = Decimal("0.10")

    # 入口与输入整理
    # 执行分项报价检查，并按普通报价或下浮率报价模式分流处理。
    def check_itemized_logic(self, text: object, tender_text: object | None = None) -> dict:
        document = self._prepare_document(text)
        item_sections = document["item_sections"]
        total_sections = document["total_sections"]
        candidate_sections = document["candidate_sections"]

        if self._detect_downward_rate_mode(candidate_sections):
            tender_document = self._prepare_document(tender_text) if tender_text is not None else None
            return self._check_downward_rate_mode(candidate_sections, tender_document=tender_document)
        return self._check_normal_mode(item_sections, total_sections, candidate_sections, document=document)

    # 统一整理输入，抽取文本、分项段落、总价段落和候选检查区间。
    def _prepare_document(self, payload: object) -> dict:
        parsed_payload = self._parse_payload(payload)
        source_text = _extract_text_from_payload(parsed_payload) if parsed_payload is not None else str(payload or "")
        normalized_text = self._normalize_text(source_text)
        lines = self._split_lines(normalized_text)

        structured_item_sections = self._prioritize_item_sections(
            self._find_layout_table_sections(parsed_payload, self.ITEM_SECTION_ANCHORS)
        )
        structured_total_sections = self._find_layout_table_sections(parsed_payload, self.TOTAL_SECTION_ANCHORS)
        item_sections = structured_item_sections or self._prioritize_item_sections(
            self._find_sections(lines, self.ITEM_SECTION_ANCHORS)
        )
        total_sections = structured_total_sections or self._find_sections(lines, self.TOTAL_SECTION_ANCHORS)
        candidate_sections = self._dedupe_sections(item_sections + total_sections)
        if not candidate_sections:
            candidate_sections = [
                {
                    "anchor": "全文",
                    "lines": lines,
                    "source": "full_text",
                    "section_id": "full_text:0",
                }
            ]

        return {
            "payload": parsed_payload,
            "text": source_text,
            "normalized_text": normalized_text,
            "lines": lines,
            "item_sections": item_sections,
            "total_sections": total_sections,
            "candidate_sections": candidate_sections,
        }

    # 将字符串或对象输入解析为 OCR JSON 字典，失败时返回空。
    def _parse_payload(self, payload: object) -> dict | None:
        if isinstance(payload, dict):
            return payload
        if not isinstance(payload, str):
            return None
        raw_text = payload.strip()
        if not raw_text.startswith("{"):
            return None
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    # 规范换行和空白，便于后续按行切分和锚点识别。
    def _normalize_text(self, text: str) -> str:
        normalized = str(text or "")
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.replace("\u3000", " ").replace("\xa0", " ")
        normalized = re.sub(r"[ \t\f\v]+", " ", normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        if "\n" not in normalized:
            normalized = re.sub(r"(小写[:：]\s*[¥￥]?\s*\d[\d,]*(?:\.\d{1,2})?\s*元?)", r"\1\n", normalized)
            normalized = re.sub(r"(大写[:：][^\s]{2,30})", r"\1\n", normalized)
            normalized = re.sub(r"((?:合计|总计|总价|投标总价|单价合计)[^。；]{0,20})", r"\n\1", normalized)
        return normalized.strip()

    # 将标准化文本拆成去空白后的有效行。
    def _split_lines(self, text: str) -> list[str]:
        return [line.strip() for line in text.split("\n") if line and line.strip()]

    # 区段定位与结构化表格拼接
    # 在纯文本中按锚点截取可能的报价区段，并用简单评分过滤噪声。
    def _find_sections(
        self,
        lines: list[str],
        anchors: tuple[str, ...],
        window: int = 80,
        *,
        require_score: bool = True,
    ) -> list[dict]:
        sections = []
        for idx, line in enumerate(lines):
            matched_anchor = next((anchor for anchor in anchors if anchor in line), None)
            if not matched_anchor:
                continue
            if not self._is_anchor_line(line, matched_anchor):
                continue

            end = min(len(lines), idx + window)
            for cursor in range(idx + 5, end):
                if self._is_heading_line(lines[cursor]) and not any(anchor in lines[cursor] for anchor in anchors):
                    end = cursor
                    break

            section_lines = lines[idx:end]
            score = self._score_section(section_lines, matched_anchor)
            if require_score and score <= 0:
                continue

            sections.append(
                {
                    "anchor": matched_anchor,
                    "start": idx,
                    "end": end,
                    "score": score,
                    "lines": section_lines,
                }
            )

        sections.sort(key=lambda item: (-item["score"], item["start"]))
        deduped = []
        seen = set()
        for section in sections:
            key = (section["start"], section["end"], section["anchor"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(
                {
                    "anchor": section["anchor"],
                    "lines": section["lines"],
                    "start": section["start"],
                    "end": section["end"],
                    "source": "text_section",
                    "section_id": f"text:{section['anchor']}:{section['start']}:{section['end']}",
                }
            )
        return deduped

    def _extract_structured_amount_only_item(
        self,
        cells: list[str],
        *,
        section_context: dict,
        row_index: int,
        column_map: dict,
        title: str | None,
    ) -> dict | None:
        row_text = " ".join(cell for cell in cells if cell)
        serial = self._structured_cell_value(cells, column_map.get("serial"))
        label = self._build_structured_amount_only_label(
            cells=cells,
            label_columns=column_map.get("label_columns") or [],
            serial=serial,
            title=title,
        )
        amount_cell = self._structured_cell_value(cells, column_map.get("line_total"))
        amount_candidates = self._extract_row_amounts(amount_cell) if amount_cell else []
        if amount_candidates:
            amount = amount_candidates[-1]
            return {
                "label": label,
                "amount": amount,
                "source": "structured_amount_only_row",
                "declared_line_total": amount,
                "relation_type": "amount_only_row",
                **self._build_entry_context(section_context, serial=serial, line_index=row_index),
            }
        if label and serial and amount_cell:
            return {
                "_unresolved": True,
                "serial": serial,
                "label": label,
                "text": row_text[:200],
                "amount_cell": amount_cell,
                "reason": "amount_not_parsed",
                "reason_text": "该行已识别出分项标签和金额列，但金额列内容未能解析为合法金额。",
                **self._build_entry_context(section_context, serial=serial, line_index=row_index),
            }
        return None

    def _build_structured_amount_only_label(
        self,
        *,
        cells: list[str],
        label_columns: list[int],
        serial: str | None,
        title: str | None,
    ) -> str:
        parts = []
        for index in label_columns:
            cell = self._structured_cell_value(cells, index)
            if cell and cell not in parts:
                parts.append(cell)
        label = " / ".join(parts)
        if serial and label:
            return f"{serial}:{label}"[:120]
        if label:
            return label[:120]
        if serial and title:
            return f"{serial}:{title}"[:120]
        return (title or serial or "结构化分项")[:120]

    # 从 layout_sections 中定位锚点后的表格序列，优先使用结构化表格结果。
    def _find_layout_table_sections(self, payload: dict | None, anchors: tuple[str, ...]) -> list[dict]:
        layout_sections = self._get_layout_sections(payload)
        logical_tables = self._get_logical_tables(payload)
        if not layout_sections:
            return []

        sections = []
        for idx, section in enumerate(layout_sections):
            anchor_text = self._get_section_text(section)
            if not anchor_text:
                continue

            matched_anchor = next((anchor for anchor in anchors if anchor in anchor_text), None)
            if not matched_anchor or not self._is_anchor_line(anchor_text, matched_anchor):
                continue

            lines = []
            pages = []
            logical_table_refs = []
            table_started = False
            for follower in layout_sections[idx + 1:]:
                section_type = str(follower.get("type") or "").lower()
                section_text = self._get_section_text(follower)
                if not section_text:
                    continue

                if not table_started:
                    if section_type == "table":
                        table_started = True
                        table_payload = self._extract_layout_table_payload(follower, logical_tables)
                        lines.extend(table_payload["lines"])
                        pages.extend(table_payload["pages"])
                        logical_table_refs.extend(table_payload["logical_table_refs"])
                        continue
                    if self._matches_other_anchor(section_text, anchors):
                        break
                    if self._is_heading_line(section_text):
                        break
                    continue

                if section_type == "table":
                    if not self._should_attach_following_layout_table(section_text):
                        continue
                    table_payload = self._extract_layout_table_payload(follower, logical_tables)
                    lines.extend(table_payload["lines"])
                    pages.extend(table_payload["pages"])
                    logical_table_refs.extend(table_payload["logical_table_refs"])
                    continue
                if self._is_layout_bridge_text(section_text):
                    continue
                break

            if not lines:
                continue

            deduped_pages = []
            seen_pages = set()
            for page in pages:
                if page in seen_pages or page is None:
                    continue
                seen_pages.add(page)
                deduped_pages.append(page)
            deduped_table_refs = list(dict.fromkeys(str(ref) for ref in logical_table_refs if ref))

            sections.append(
                {
                    "anchor": matched_anchor,
                    "lines": lines,
                    "start": idx,
                    "end": idx + len(lines),
                    "score": len(lines),
                    "source": "layout_table_sequence",
                    "pages": deduped_pages,
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

    # 优先保留更像正式分项报价表的核心区段。
    def _prioritize_item_sections(self, sections: list[dict]) -> list[dict]:
        if not sections:
            return sections
        primary_sections = [
            section
            for section in sections
            if str(section.get("anchor") or "") in self.PRIMARY_ITEM_SECTION_ANCHORS
        ]
        return primary_sections or sections

    # 从 OCR 结果中提取版面分区列表。
    def _get_layout_sections(self, payload: dict | None) -> list[dict]:
        container = self._get_structured_container(payload)
        if not isinstance(container, dict):
            return []
        layout_sections = container.get("layout_sections")
        if not isinstance(layout_sections, list):
            return []
        return [section for section in layout_sections if isinstance(section, dict)]

    # 从 OCR 结果中提取逻辑表格列表。
    def _get_logical_tables(self, payload: dict | None) -> list[dict]:
        container = self._get_structured_container(payload)
        if not isinstance(container, dict):
            return []
        logical_tables = container.get("logical_tables")
        if not isinstance(logical_tables, list):
            return []
        return [table for table in logical_tables if isinstance(table, dict)]

    # 兼容直接字典和带 data 包裹的结构化 OCR 结果。
    def _get_structured_container(self, payload: dict | None) -> dict | None:
        if not isinstance(payload, dict):
            return None

        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload

    # 将 layout 表格区段转换为逐行文本，必要时绑定对应 logical table。
    def _extract_layout_table_lines(self, section: dict, logical_tables: list[dict]) -> list[str]:
        return self._extract_layout_table_payload(section, logical_tables)["lines"]

    def _extract_layout_table_payload(self, section: dict, logical_tables: list[dict]) -> dict:
        logical_table_index = self._match_logical_table_index(section, logical_tables)
        if logical_table_index is not None:
            logical_lines = []
            logical_table_refs = []
            logical_table_pages = []
            for offset, logical_table in enumerate(
                self._collect_logical_table_sequence(logical_tables, logical_table_index),
            ):
                table_index = logical_table_index + offset
                logical_lines.extend(self._logical_table_to_lines(logical_table, include_headers=(offset == 0)))
                logical_table_refs.append(self._logical_table_ref(logical_table, table_index))
                logical_table_pages.extend(self._get_logical_table_pages(logical_table))
            if logical_lines:
                return {
                    "lines": logical_lines,
                    "logical_table_refs": logical_table_refs,
                    "pages": logical_table_pages,
                }

        section_text = self._get_section_text(section)
        section_page = section.get("page")
        return {
            "lines": self._split_lines(self._normalize_text(section_text)),
            "logical_table_refs": [],
            "pages": [section_page] if isinstance(section_page, int) else [],
        }

    def _logical_table_ref(self, table: dict, index: int) -> str:
        return str(table.get("id") or f"table_index_{index}")

    # 优先从结构化 logical table 中抽取报价行关系，避免 OCR 展平后重复累计组总价。
    def _extract_structured_itemized_entries(
        self,
        document: dict | None,
        *,
        item_sections: list[dict] | None = None,
    ) -> dict:
        empty_result = {
            "items": [],
            "totals": [],
            "row_issues": [],
            "unresolved_rows": [],
            "relation_rows": [],
            "group_checks": [],
            "amount_only_item_count": 0,
            "used_tables": [],
        }
        if not isinstance(document, dict):
            return empty_result

        logical_tables = self._get_logical_tables(document.get("payload"))
        if not logical_tables:
            return empty_result

        allowed_table_refs = self._collect_itemized_logical_table_refs(item_sections)
        extracted_items = []
        extracted_totals = []
        row_issues = []
        unresolved_rows = []
        relation_rows = []
        group_checks = []
        amount_only_item_count = 0
        used_tables = []

        for table_index, table in enumerate(logical_tables):
            table_ref = self._logical_table_ref(table, table_index)
            if allowed_table_refs and table_ref not in allowed_table_refs:
                continue
            headers = self._get_logical_table_headers(table)
            column_map = self._resolve_structured_price_columns_for_table(table, headers=headers)
            if column_map is None:
                continue

            table_result = self._analyze_structured_itemized_table(
                table,
                table_index=table_index,
                column_map=column_map,
            )
            if not (
                table_result["items"]
                or table_result["totals"]
                or table_result["row_issues"]
                or table_result["unresolved_rows"]
            ):
                continue

            extracted_items.extend(table_result["items"])
            extracted_totals.extend(table_result["totals"])
            row_issues.extend(table_result["row_issues"])
            unresolved_rows.extend(table_result["unresolved_rows"])
            relation_rows.extend(table_result["relation_rows"])
            group_checks.extend(table_result["group_checks"])
            amount_only_item_count += int(table_result.get("amount_only_item_count") or 0)
            used_tables.append(
                {
                    "table_ref": table_ref,
                    "table_id": table.get("id"),
                    "title": table.get("title"),
                    "pages": self._get_logical_table_pages(table),
                    "headers": headers,
                    "row_count": len(table.get("rows") or []),
                }
            )

        return {
            "items": extracted_items,
            "totals": extracted_totals,
            "row_issues": row_issues,
            "unresolved_rows": unresolved_rows,
            "relation_rows": relation_rows,
            "group_checks": group_checks,
            "amount_only_item_count": amount_only_item_count,
            "used_tables": used_tables,
        }

    def _collect_itemized_logical_table_refs(self, sections: list[dict] | None) -> set[str]:
        refs = set()
        for section in sections or []:
            for ref in section.get("logical_table_refs") or []:
                if ref:
                    refs.add(str(ref))
        return refs

    # 根据表头识别结构化报价表中的关键列。
    def _resolve_structured_price_columns(self, headers: list[str]) -> dict | None:
        normalized_headers = [self._normalize_label_key(header) for header in headers]
        column_map = {}
        for field, aliases in self.STRUCTURED_COLUMN_ALIASES.items():
            alias_candidates = [self._normalize_label_key(alias) for alias in aliases]
            matched_index = self._match_structured_column_index(normalized_headers, alias_candidates)
            if matched_index is not None:
                column_map[field] = matched_index

        required_fields = {"quantity", "unit_price", "line_total"}
        if not required_fields.issubset(column_map):
            return None
        if not any(field in column_map for field in ("serial", "model", "description")):
            return None
        return column_map

    def _resolve_structured_price_columns_for_table(self, table: dict, *, headers: list[str] | None = None) -> dict | None:
        headers = headers or self._get_logical_table_headers(table)
        standard_map = self._resolve_structured_price_columns(headers)
        if standard_map is not None:
            standard_map = dict(standard_map)
            standard_map["mode"] = "arithmetic"
            standard_map["data_start_index"] = self._structured_table_data_start_index(table)
            return standard_map
        return self._infer_amount_only_column_map(table, headers=headers)

    def _structured_table_data_start_index(self, table: dict) -> int:
        rows = table.get("rows") or []
        declared_header_row_count = int(table.get("header_row_count") or 0)
        if declared_header_row_count > 0:
            return min(len(rows), declared_header_row_count)

        if rows:
            first_row = [str(cell).strip() for cell in rows[0] if str(cell).strip()]
            unique_first_row = []
            for value in first_row:
                if value not in unique_first_row:
                    unique_first_row.append(value)
            start_index = 1 if len(unique_first_row) == 1 and len(unique_first_row[0]) >= 4 else 0
            if self._extract_row_based_table_headers({"rows": rows[start_index : start_index + 2]}):
                start_index += 1
            if start_index > 0:
                return min(len(rows), start_index)

        html_rows = self._parse_html_table_rows(table)
        if not html_rows:
            return 0

        start_index = 0
        if self._extract_html_title_row(html_rows):
            start_index += 1
        if self._extract_html_header_row(html_rows[start_index:]):
            start_index += 1
        return min(len(rows), start_index)

    def _infer_amount_only_column_map(self, table: dict, *, headers: list[str]) -> dict | None:
        rows = table.get("rows") or []
        if not rows:
            return None

        data_start_index = self._structured_table_data_start_index(table)
        data_rows = [row for row in rows[data_start_index:] if isinstance(row, list)]
        if not data_rows:
            return None

        column_count = max(
            len(headers),
            max((len(row) for row in data_rows), default=0),
        )
        if column_count <= 1:
            return None

        serial_index = self._infer_structured_serial_column(data_rows, column_count)
        excluded_indexes = {index for index in (serial_index,) if index is not None}
        line_total_index = self._infer_structured_amount_column(
            data_rows,
            column_count,
            excluded_indexes=excluded_indexes,
        )
        if line_total_index is None:
            return None

        label_columns = self._infer_structured_label_columns(
            headers=headers,
            data_rows=data_rows,
            column_count=column_count,
            excluded_indexes=excluded_indexes | {line_total_index},
        )
        if not label_columns:
            return None

        amount_hits = 0
        serial_hits = 0
        for row in data_rows:
            if line_total_index < len(row) and self._extract_row_amounts(str(row[line_total_index]).strip()):
                amount_hits += 1
            if serial_index is not None and serial_index < len(row) and self._extract_row_serial(str(row[serial_index]).strip()):
                serial_hits += 1

        if amount_hits < 2:
            return None
        if serial_index is None and serial_hits == 0:
            return None

        return {
            "mode": "amount_only",
            "serial": serial_index,
            "line_total": line_total_index,
            "label_columns": label_columns,
            "data_start_index": data_start_index,
        }

    def _infer_structured_serial_column(self, rows: list[list[object]], column_count: int) -> int | None:
        best_index = None
        best_score = -1
        for index in range(column_count):
            serial_hits = 0
            nonempty_hits = 0
            for row in rows:
                if index >= len(row):
                    continue
                cell = str(row[index]).strip()
                if not cell:
                    continue
                nonempty_hits += 1
                if self._extract_row_serial(cell) or re.fullmatch(r"\d+(?:\.\d+)?", cell):
                    serial_hits += 1
            if serial_hits < 2:
                continue
            score = serial_hits * 10 - nonempty_hits
            if best_index is None or score > best_score:
                best_index = index
                best_score = score
        return best_index

    def _infer_structured_amount_column(
        self,
        rows: list[list[object]],
        column_count: int,
        *,
        excluded_indexes: set[int],
    ) -> int | None:
        best_index = None
        best_score = -1
        for index in range(column_count):
            if index in excluded_indexes:
                continue
            amount_hits = 0
            text_hits = 0
            for row in rows:
                if index >= len(row):
                    continue
                cell = str(row[index]).strip()
                if not cell:
                    continue
                if self._extract_row_amounts(cell):
                    amount_hits += 1
                elif re.search(r"[\u4e00-\u9fffA-Za-z]", cell):
                    text_hits += 1
            if amount_hits < 2 or amount_hits <= text_hits:
                continue
            score = amount_hits * 10 - text_hits + index
            if best_index is None or score > best_score:
                best_index = index
                best_score = score
        return best_index

    def _infer_structured_label_columns(
        self,
        *,
        headers: list[str],
        data_rows: list[list[object]],
        column_count: int,
        excluded_indexes: set[int],
    ) -> list[int]:
        label_columns = []
        header_hints = ("名称", "项目", "功能", "内容", "描述", "说明", "参数", "配置", "规格", "型号", "品牌", "厂家")
        for index in range(column_count):
            if index in excluded_indexes:
                continue

            header = headers[index] if index < len(headers) else ""
            normalized_header = self._normalize_label_key(header)
            header_hit = any(keyword in normalized_header for keyword in header_hints)

            text_hits = 0
            amount_hits = 0
            for row in data_rows:
                if index >= len(row):
                    continue
                cell = str(row[index]).strip()
                if not cell:
                    continue
                if self._extract_row_amounts(cell):
                    amount_hits += 1
                elif re.search(r"[\u4e00-\u9fffA-Za-z]", cell):
                    text_hits += 1

            if text_hits <= amount_hits and not header_hit:
                continue
            if text_hits == 0 and not header_hit:
                continue
            label_columns.append(index)
        return label_columns

    # 在规范化表头列表中查找最贴近目标语义的列。
    def _match_structured_column_index(self, headers: list[str], aliases: list[str]) -> int | None:
        for index, header in enumerate(headers):
            if not header:
                continue
            if any(alias == header or alias in header or header in alias for alias in aliases):
                return index
        return None

    # 对单张 logical table 做结构化行关系抽取，并处理组总价重复展示。
    def _analyze_structured_itemized_table(self, table: dict, *, table_index: int, column_map: dict) -> dict:
        headers = self._get_logical_table_headers(table)
        rows = table.get("rows") or []
        start_index = min(len(rows), int(column_map.get("data_start_index") or 0))
        pages = self._get_logical_table_pages(table)
        section_context = {
            "section_id": f"logical_table:{table.get('id') or table_index}",
            "anchor": "logical_table",
            "pages": pages,
        }
        carry = {"serial": None, "model": None, "brand": None}
        raw_relations = []
        direct_items = []
        totals = []
        unresolved_rows = []
        amount_only_item_count = 0

        for offset, row in enumerate(rows[start_index:], start=start_index):
            if not isinstance(row, list):
                continue
            cells = self._normalize_structured_row_cells(row, len(headers))
            if not any(cells):
                continue
            if self._is_structured_header_like_row(cells, headers):
                continue

            total_entry = self._extract_structured_total_entry(
                cells,
                section_context=section_context,
                row_index=offset,
                title=table.get("title"),
                column_map=column_map,
            )
            if total_entry is not None:
                totals.append(total_entry)
                continue

            if column_map.get("mode") == "amount_only":
                amount_only_item = self._extract_structured_amount_only_item(
                    cells,
                    section_context=section_context,
                    row_index=offset,
                    column_map=column_map,
                    title=table.get("title"),
                )
                if amount_only_item is None:
                    continue
                if amount_only_item.pop("_unresolved", False):
                    unresolved_rows.append(amount_only_item)
                    continue
                direct_items.append(amount_only_item)
                amount_only_item_count += 1
                continue

            relation = self._extract_structured_row_relation(
                cells,
                section_context=section_context,
                row_index=offset,
                column_map=column_map,
                title=table.get("title"),
                carry=carry,
            )
            if relation is None:
                continue
            if relation.pop("_unresolved", False):
                unresolved_rows.append(relation)
                continue
            raw_relations.append(relation)

        grouped_result = self._summarize_structured_relations(raw_relations)
        return {
            "items": direct_items + grouped_result["items"],
            "totals": totals,
            "row_issues": grouped_result["row_issues"],
            "unresolved_rows": unresolved_rows,
            "relation_rows": direct_items + grouped_result["relation_rows"],
            "group_checks": grouped_result["group_checks"],
            "amount_only_item_count": amount_only_item_count,
        }

    # 统一结构化表格行长度，避免缺列时后续索引越界。
    def _normalize_structured_row_cells(self, row: list[object], target_len: int) -> list[str]:
        normalized = [str(cell).strip() for cell in row[:target_len]]
        if len(normalized) < target_len:
            normalized.extend([""] * (target_len - len(normalized)))
        return normalized

    # 识别被 OCR 切到数据区中的表头残片，避免误作报价行。
    def _is_structured_header_like_row(self, cells: list[str], headers: list[str]) -> bool:
        nonempty_cells = [cell for cell in cells if cell]
        if not nonempty_cells:
            return False
        normalized_headers = {self._normalize_label_key(header) for header in headers if header}
        header_hits = [cell for cell in nonempty_cells if self._normalize_label_key(cell) in normalized_headers]
        if not header_hits:
            return False
        if len(header_hits) == len(nonempty_cells):
            return True
        return len(header_hits) >= 2 and not any(self._extract_money_candidates(cell) for cell in nonempty_cells)

    # 从结构化表格中提取小计/合计等汇总行。
    def _extract_structured_total_entry(
        self,
        cells: list[str],
        *,
        section_context: dict,
        row_index: int,
        title: str | None,
        column_map: dict,
    ) -> dict | None:
        row_text = " ".join(cell for cell in cells if cell)
        if not row_text or not self._looks_like_total_line(row_text):
            return None

        amount_candidates = []
        total_index = column_map.get("line_total")
        if total_index is not None and total_index < len(cells):
            total_text = cells[total_index]
            amount_candidates = self._extract_row_amounts(total_text)
            if not amount_candidates:
                cleaned_total_text = re.sub(r"[（(][^）)]*(?:税|税率)[^）)]*[）)]", "", total_text)
                amount_candidates = self._extract_row_amounts(cleaned_total_text)
        if not amount_candidates:
            amount_candidates = self._extract_row_amounts(row_text)
        if not amount_candidates:
            cleaned_row_text = re.sub(r"[（(][^）)]*(?:税|税率)[^）)]*[）)]", "", row_text)
            amount_candidates = self._extract_row_amounts(cleaned_row_text)
        if not amount_candidates:
            return None

        label_source = next((cell for cell in cells if self._looks_like_total_line(cell)), row_text)
        label = self._clean_label(label_source) or ("小计" if self._looks_like_subtotal_line(row_text) else "合计")
        if title and label in {"小计", "合计", "总计"}:
            label = f"{title} {label}"
        return {
            "label": label,
            "amount": amount_candidates[-1],
            "source": "structured_subtotal" if self._looks_like_subtotal_line(row_text) else "structured_total",
            "is_subtotal": self._looks_like_subtotal_line(row_text),
            **self._build_entry_context(section_context, line_index=row_index),
        }

    # 把结构化表格中的一行抽成数量-单价-总价关系，必要时继承前序组头。
    def _extract_structured_row_relation(
        self,
        cells: list[str],
        *,
        section_context: dict,
        row_index: int,
        column_map: dict,
        title: str | None,
        carry: dict,
    ) -> dict | None:
        row_text = " ".join(cell for cell in cells if cell)
        serial = self._structured_cell_value(cells, column_map.get("serial"))
        model = self._structured_cell_value(cells, column_map.get("model"))
        description = self._structured_cell_value(cells, column_map.get("description"))
        brand = self._structured_cell_value(cells, column_map.get("brand"))
        quantity = self._to_quantity_decimal(self._structured_cell_value(cells, column_map.get("quantity")))
        unit_price = self._to_decimal(self._structured_cell_value(cells, column_map.get("unit_price")))
        line_total = self._to_decimal(self._structured_cell_value(cells, column_map.get("line_total")))

        has_pricing_signal = quantity is not None or unit_price is not None or line_total is not None
        if has_pricing_signal and not serial:
            serial = carry.get("serial")
        if has_pricing_signal and not model:
            model = carry.get("model")
        if has_pricing_signal and not brand:
            brand = carry.get("brand")

        if serial:
            carry["serial"] = serial
        if model:
            carry["model"] = model
        if brand:
            carry["brand"] = brand

        label = self._build_structured_row_label(
            serial=serial,
            model=model,
            description=description,
            title=title,
        )
        if quantity is None or unit_price is None or line_total is None:
            if has_pricing_signal:
                return {
                    "_unresolved": True,
                    "serial": serial,
                    "label": label,
                    "text": row_text[:200],
                    "quantity_cell": self._structured_cell_value(cells, column_map.get("quantity")),
                    "unit_price_cell": self._structured_cell_value(cells, column_map.get("unit_price")),
                    "line_total_cell": self._structured_cell_value(cells, column_map.get("line_total")),
                    "reason": "pricing_fields_incomplete",
                    "reason_text": "该行存在报价字段痕迹，但数量、单价、总价至少有一项未能完整识别。",
                    **self._build_entry_context(section_context, serial=serial, line_index=row_index),
                }
            if label and (description or model or serial):
                return None
            return None

        expected_total = quantity * unit_price
        return {
            "label": label,
            "serial": serial,
            "model": model,
            "description": description,
            "brand": brand,
            "quantity": quantity,
            "unit_price": unit_price,
            "line_total": line_total,
            "expected_total": expected_total,
            "difference": expected_total - line_total,
            "table_title": title,
            "group_key": (
                str(section_context.get("section_id") or ""),
                str(serial or ""),
                str(model or ""),
            ),
            **self._build_entry_context(section_context, serial=serial, line_index=row_index),
        }

    # 读取指定列位的文本值。
    def _structured_cell_value(self, cells: list[str], index: int | None) -> str:
        if index is None or index < 0 or index >= len(cells):
            return ""
        return str(cells[index]).strip()

    # 组合结构化行的人类可读标签。
    def _build_structured_row_label(
        self,
        *,
        serial: str | None,
        model: str | None,
        description: str | None,
        title: str | None,
    ) -> str:
        parts = [part for part in (model, description) if part]
        label = " / ".join(parts)
        if serial and label:
            return f"{serial}:{label}"[:120]
        if label:
            return label[:120]
        if serial and title:
            return f"{serial}:{title}"[:120]
        return (title or serial or "结构化分项")[:120]

    # 按组识别“行内重复展示组总价”的情况，并据此生成有效汇总金额。
    def _summarize_structured_relations(self, relations: list[dict]) -> dict:
        if not relations:
            return {
                "items": [],
                "row_issues": [],
                "relation_rows": [],
                "group_checks": [],
            }

        groups = []
        current_group = []
        for relation in relations:
            if not current_group or relation["group_key"] == current_group[-1]["group_key"]:
                current_group.append(relation)
                continue
            groups.append(current_group)
            current_group = [relation]
        if current_group:
            groups.append(current_group)

        items = []
        row_issues = []
        relation_rows = []
        group_checks = []

        for group in groups:
            repeated_group_total = self._detect_repeated_group_total(group)
            if repeated_group_total is not None:
                expected_group_total = sum((item["expected_total"] for item in group), Decimal("0"))
                group_difference = expected_group_total - repeated_group_total
                representative = group[0]
                group_check = {
                    "group_key": list(representative["group_key"]),
                    "label": representative["label"],
                    "serial": representative.get("serial"),
                    "model": representative.get("model"),
                    "row_count": len(group),
                    "status": "pass" if abs(group_difference) <= self.MONEY_TOLERANCE else "fail",
                    "group_declared_total": repeated_group_total,
                    "group_expected_total": expected_group_total,
                    "difference": group_difference,
                    "pages": representative.get("section_pages"),
                }
                group_checks.append(group_check)
                if abs(group_difference) > self.MONEY_TOLERANCE:
                    row_issues.append(
                        {
                            "kind": "group_total_mismatch",
                            "label": representative["label"],
                            "serial": representative.get("serial"),
                            "declared_group_total": self._format_decimal(repeated_group_total),
                            "expected_total": self._format_decimal(expected_group_total),
                            "difference": self._format_decimal(group_difference),
                            "row_count": len(group),
                        }
                    )

                for relation in group:
                    normalized_relation = dict(relation)
                    normalized_relation["relation_type"] = "repeated_group_total"
                    normalized_relation["raw_line_total"] = relation["line_total"]
                    normalized_relation["difference"] = None
                    normalized_relation["effective_total"] = relation["expected_total"]
                    normalized_relation["group_declared_total"] = repeated_group_total
                    normalized_relation["group_expected_total"] = expected_group_total
                    normalized_relation["group_difference"] = group_difference
                    relation_rows.append(normalized_relation)
                    items.append(
                        {
                            "label": relation["label"],
                            "amount": relation["expected_total"],
                            "source": "structured_group_row",
                            "declared_line_total": relation["line_total"],
                            "expected_total": relation["expected_total"],
                            "quantity": relation["quantity"],
                            "unit_price": relation["unit_price"],
                            **self._build_entry_context(
                                {
                                    "section_id": relation.get("section_id"),
                                    "anchor": relation.get("section_anchor"),
                                    "pages": relation.get("section_pages"),
                                },
                                serial=relation.get("serial"),
                                line_index=relation.get("line_index"),
                            ),
                        }
                    )
                continue

            for relation in group:
                difference = relation["difference"]
                normalized_relation = dict(relation)
                normalized_relation["relation_type"] = "row_total"
                relation_rows.append(normalized_relation)
                items.append(
                    {
                        "label": relation["label"],
                        "amount": relation["expected_total"],
                        "source": "structured_row",
                        "declared_line_total": relation["line_total"],
                        "expected_total": relation["expected_total"],
                        "quantity": relation["quantity"],
                        "unit_price": relation["unit_price"],
                        **self._build_entry_context(
                            {
                                "section_id": relation.get("section_id"),
                                "anchor": relation.get("section_anchor"),
                                "pages": relation.get("section_pages"),
                            },
                            serial=relation.get("serial"),
                            line_index=relation.get("line_index"),
                        ),
                    }
                )
                if abs(difference) > self.MONEY_TOLERANCE:
                    row_issues.append(
                        {
                            "kind": "row_total_mismatch",
                            "label": relation["label"],
                            "serial": relation.get("serial"),
                            "quantity": self._format_decimal(relation["quantity"]),
                            "unit_price": self._format_decimal(relation["unit_price"]),
                            "line_total": self._format_decimal(relation["line_total"]),
                            "expected_total": self._format_decimal(relation["expected_total"]),
                            "difference": self._format_decimal(difference),
                        }
                    )

        return {
            "items": items,
            "row_issues": row_issues,
            "relation_rows": relation_rows,
            "group_checks": group_checks,
        }

    # 检测同一组内每行都重复展示同一个组总价的模式。
    def _detect_repeated_group_total(self, group: list[dict]) -> Decimal | None:
        if len(group) < 2:
            return None
        line_totals = [relation.get("line_total") for relation in group if relation.get("line_total") is not None]
        if len(line_totals) != len(group):
            return None
        unique_totals = {total for total in line_totals}
        if len(unique_totals) != 1:
            return None

        repeated_total = next(iter(unique_totals))
        max_expected_total = max((relation["expected_total"] for relation in group), default=Decimal("0"))
        if repeated_total <= max_expected_total + self.MONEY_TOLERANCE:
            return None
        return repeated_total

    # 为某个 layout 表格区段匹配最可能对应的 logical table 起点。
    def _match_logical_table_index(self, section: dict, logical_tables: list[dict]) -> int | None:
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
                if compact_header_text and compact_header_text in compact_section_text:
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

    # 从一张 logical table 开始收集其后连续的跨页续表。
    def _collect_logical_table_sequence(self, logical_tables: list[dict], start_index: int) -> list[dict]:
        collected = [logical_tables[start_index]]
        current_table = logical_tables[start_index]
        for next_table in logical_tables[start_index + 1:]:
            if not self._is_logical_table_continuation(current_table, next_table):
                break
            collected.append(next_table)
            current_table = next_table
        return collected

    # 判断后一张 logical table 是否属于当前表格的续页。
    def _is_logical_table_continuation(self, current_table: dict, next_table: dict) -> bool:
        if not isinstance(next_table, dict):
            return False
        if self._is_spare_parts_marker_text(" ".join(self._get_logical_table_headers(next_table))):
            return False
        if not bool(next_table.get("continued")) and not self._looks_like_html_table_continuation(current_table, next_table):
            return False

        current_pages = self._get_logical_table_pages(current_table)
        next_pages = self._get_logical_table_pages(next_table)
        if current_pages and next_pages and next_pages[0] - current_pages[-1] > 3:
            return False
        return True

    # 把逻辑表格统一展开为逐行文本，兼容 HTML 表与普通二维数组表。
    def _logical_table_to_lines(self, table: dict, *, include_headers: bool = True) -> list[str]:
        html_lines = self._logical_html_table_to_lines(table, include_headers=include_headers)
        if html_lines:
            return html_lines

        lines = []
        headers = self._get_logical_table_headers(table)
        if include_headers and headers and not all(re.fullmatch(r"col_\d+", header, re.IGNORECASE) for header in headers):
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

    # 读取 logical table 关联的页码列表。
    def _get_logical_table_pages(self, table: dict) -> list[int]:
        pages = table.get("pages")
        if isinstance(pages, list):
            return [page for page in pages if isinstance(page, int)]
        page = table.get("page")
        return [page] if isinstance(page, int) else []

    # 提取 logical table 表头，必要时从 HTML 表格内容里反推。
    def _get_logical_table_headers(self, table: dict) -> list[str]:
        headers = [str(header).strip() for header in (table.get("headers") or []) if str(header).strip()]
        if headers and not all(re.fullmatch(r"col_\d+", header, re.IGNORECASE) for header in headers):
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
        rows = [row for row in (table.get("rows") or [])[:3] if isinstance(row, list)]
        if not rows:
            return []

        start_index = 0
        first_row = [str(cell).strip() for cell in rows[0] if str(cell).strip()]
        unique_first_row = []
        for value in first_row:
            if value not in unique_first_row:
                unique_first_row.append(value)
        if len(unique_first_row) == 1 and len(rows) > 1:
            start_index = 1

        header_hints = ("序号", "编号", "名称", "项目", "功能", "内容", "描述", "说明", "参数", "规格", "型号", "金额", "总价", "合计")
        for row in rows[start_index : start_index + 2]:
            values = [str(cell).strip() for cell in row if str(cell).strip()]
            if len(values) < 2:
                continue
            compact_values = {re.sub(r"\s+", "", value) for value in values}
            header_hits = sum(1 for value in compact_values if any(hint in value for hint in header_hints))
            if header_hits >= 2 and not any(self._extract_money_candidates(value) for value in values):
                return values
        return []

    # 生成表格预览文本，用于 layout 表格与 logical table 的匹配评分。
    def _logical_table_preview_lines(self, table: dict) -> list[str]:
        html_lines = self._logical_html_table_to_lines(table, include_headers=True)
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

    # 根据 HTML 表头和首行数据特征判断两张表是否相邻续接。
    def _looks_like_html_table_continuation(self, current_table: dict, next_table: dict) -> bool:
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
        next_text = " ".join(cell["text"] for cell in next_first_data if cell["text"])
        return bool(self._extract_money_candidates(next_text))

    # 把 HTML 形式的逻辑表格展平成逐行文本，并合并被拆断的续行。
    def _logical_html_table_to_lines(self, table: dict, *, include_headers: bool = True) -> list[str]:
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
        data_rows = html_rows[data_start_index + header_offset :]
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
                1 for cell in row[:2] if not cell["text"] or bool(cell.get("inherited"))
            )
            trailing_cell = next((cell for cell in reversed(row) if cell["text"]), None)
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
            if leading_placeholders >= 2 and not has_own_total and previous_line_index is not None:
                lines[previous_line_index] = f"{lines[previous_line_index]} {rendered_line}".strip()
                continue

            lines.append(rendered_line)
            previous_line_index = len(lines) - 1

        return lines

    # 解析 logical table 中保存的 HTML 表格内容，并补齐跨行继承单元格。
    def _parse_html_table_rows(self, table: dict) -> list[list[dict]]:
        block_content = table.get("block_content")
        if not isinstance(block_content, str) or "<table" not in block_content.lower():
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
                        active_spans[column_index + offset] = {"text": text, "remaining": rowspan - 1}
                column_index += colspan

            extend_active_spans()
            max_columns = max(max_columns, len(row))
            expanded_rows.append(row)

        for row in expanded_rows:
            while len(row) < max_columns:
                row.append({"text": "", "inherited": False})
        return expanded_rows

    # 识别 HTML 表格顶部可能存在的单行标题。
    def _extract_html_title_row(self, rows: list[list[dict]]) -> str | None:
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

    # 识别 HTML 表格中真正的列表头。
    def _extract_html_header_row(self, rows: list[list[dict]]) -> list[str]:
        header_hints = ("序号", "编号", "名称", "项目", "功能", "内容", "描述", "说明", "参数", "规格", "型号", "金额", "总价", "合计")
        for row in rows[:3]:
            values = [cell["text"] for cell in row if cell["text"]]
            if len(values) < 2:
                continue
            compact_values = {re.sub(r"\s+", "", value) for value in values}
            if {"序号", "单价", "合计"}.issubset(compact_values):
                return values
            header_hits = sum(1 for value in compact_values if any(hint in value for hint in header_hints))
            if header_hits >= 2 and not any(self._extract_money_candidates(value) for value in values):
                return values
        return []

    # 返回第一行有效数据行，供续表识别逻辑使用。
    def _first_html_data_row(self, rows: list[list[dict]]) -> list[dict]:
        start_index = 1 if self._extract_html_title_row(rows) else 0
        if self._extract_html_header_row(rows[start_index:]):
            start_index += 1
        for row in rows[start_index:]:
            if any(cell["text"] for cell in row):
                if self._is_html_bridge_row(row):
                    continue
                return row
        return []

    # 判断 HTML 表格是否已经进入备件等无需参与主校验的区域。
    def _html_table_contains_spare_parts(self, rows: list[list[dict]]) -> bool:
        for row in rows[:3]:
            row_text = "".join(cell["text"] for cell in row if cell["text"])
            if self._is_spare_parts_marker_text(row_text):
                return True
        return False

    # 判断 HTML 行是否只是分页桥接文本而非真实报价行。
    def _is_html_bridge_row(self, row: list[dict]) -> bool:
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

    # 优先返回结构化分区中的原始文本内容。
    def _get_section_text(self, section: dict) -> str:
        text = section.get("raw_text") or section.get("text")
        return text.strip() if isinstance(text, str) and text.strip() else ""

    # layout 文本过滤与标题识别
    # 判断一段文本是否命中了当前扫描集合中的其他锚点。
    def _matches_other_anchor(self, text: str, anchors: tuple[str, ...]) -> bool:
        matched_anchor = next((anchor for anchor in anchors if anchor in text), None)
        return bool(matched_anchor and self._is_anchor_line(text, matched_anchor))

    # 判断 layout 文本是否全部由可忽略内容组成。
    def _is_skippable_layout_text(self, text: str) -> bool:
        lines = self._split_lines(self._normalize_text(text))
        return bool(lines) and all(self._should_skip_line(line) for line in lines)

    # 判断 layout 文本是否只是分页桥接、页码或印章噪声。
    def _is_layout_bridge_text(self, text: str) -> bool:
        return (
            self._is_skippable_layout_text(text)
            or self._is_spare_parts_marker_text(text)
            or self._is_layout_page_marker_text(text)
            or self._is_layout_seal_text(text)
        )

    # 识别分页页码或商务/技术部分页眉页脚。
    def _is_layout_page_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        if re.fullmatch(r"第\d+页", compact):
            return True
        return compact in {"投标文件-商务部分", "投标文件-技术部分", "商务部分", "技术部分"}

    # 判断锚点后的下一张 layout 表格是否应并入当前报价表。
    def _should_attach_following_layout_table(self, text: str) -> bool:
        if self._is_spare_parts_marker_text(text):
            return False

        lines = self._split_lines(self._normalize_text(text))
        if not lines:
            return False
        if any(self._extract_money_candidates(line) for line in lines):
            return True
        if any(self._extract_zero_amount_candidate(line) is not None for line in lines):
            return True
        if any(self._extract_row_serial(line) for line in lines) or any(self._looks_like_total_line(line) for line in lines):
            return False
        return True


    # 按锚点和内容对区段去重，避免重复校验同一块表格。
    def _dedupe_sections(self, sections: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for section in sections:
            key = (
                section.get("anchor"),
                tuple(section.get("lines") or []),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(section)
        return deduped

    # 判断某一行是否真的是标题锚点，而不是目录或说明文字。
    def _is_anchor_line(self, line: str, anchor: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        anchor_index = compact.find(anchor)
        if anchor_index < 0:
            return False
        if "目录" in compact or "..." in line or ".." in line:
            return False
        if len(compact) > 40 and anchor_index > 6:
            return False
        if len(compact) > 30 and any(
            hint in compact
            for hint in (
                "内容与",
                "须与",
                "不一致",
                "应为",
                "填写",
                "计入",
                "中标价",
                "最高价",
                "量化",
                "修正",
            )
        ):
            return False
        return True

    # 根据金额命中数和总价特征对候选区段打分。
    def _score_section(self, lines: list[str], anchor: str) -> int:
        text = "\n".join(lines)
        amount_hits = sum(len(self._extract_money_candidates(line)) for line in lines)
        total_hits = sum(1 for line in lines if any(keyword in line for keyword in self.TOTAL_KEYWORDS))
        score = amount_hits + total_hits
        if anchor in ("开标一览表", "报价一览表"):
            score += 2
        if "目录" in text and amount_hits == 0:
            return 0
        return score

    # 判断文本中是否出现数量和计量单位组合。
    def _contains_quantity_unit(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        unit_pattern = "|".join(re.escape(unit) for unit in self.UNIT_KEYWORDS)
        return bool(
            re.search(
                rf"(?:\d+(?:\.\d+)?\s*(?:{unit_pattern})|(?:{unit_pattern})\s*\d+(?:\.\d+)?)",
                compact,
                re.IGNORECASE,
            )
        )

    # 识别章节标题行，避免被误当成报价明细。
    def _is_heading_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        serial = self._extract_row_serial(line)
        if serial and (self._extract_money_candidates(line) or self._contains_quantity_unit(compact)):
            return False
        if self._looks_like_frequency_range_line(line) and self._extract_money_candidates(line):
            return False
        return bool(
            re.match(r"^(第[一二三四五六七八九十百]+章|[一二三四五六七八九十]+、|\d+\.[\d\.]*|（[一二三四五六七八九十]+）)", compact)
        )

    # 识别形如 GHz/MHz 范围的技术参数行。
    def _looks_like_frequency_range_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return bool(
            re.match(
                r"^\d+(?:\.\d+)?(?:GHz|Ghz|MHz|kHz|Hz)[~～\-至]\d+(?:\.\d+)?(?:GHz|Ghz|MHz|kHz|Hz)?",
                compact,
                re.IGNORECASE,
            )
        )

    # 判断当前文档是否属于下浮率/优惠率报价模式。
    def _detect_downward_rate_mode(self, sections: list[dict]) -> bool:
        for section in sections:
            section_text = "\n".join(section["lines"])
            if not any(keyword in section_text for keyword in self.RATE_KEYWORDS):
                continue
            if "%" in section_text or "％" in section_text:
                return True
            if any(keyword in line for line in section["lines"] for keyword in self.RATE_KEYWORDS):
                return True
        return False

    def _empty_section_analysis(self) -> dict:
        return {
            "items": [],
            "totals": [],
            "row_issues": [],
            "unresolved_rows": [],
        }

    def _collect_section_analysis(self, sections: list[dict]) -> dict:
        aggregated = self._empty_section_analysis()
        for section in sections or []:
            section_items, section_totals, section_row_issues, section_unresolved_rows = self._extract_section_entries(
                section["lines"],
                section_context=section,
            )
            aggregated["items"].extend(section_items)
            aggregated["totals"].extend(section_totals)
            aggregated["row_issues"].extend(section_row_issues)
            aggregated["unresolved_rows"].extend(section_unresolved_rows)
        return aggregated

    def _collect_section_totals(self, sections: list[dict]) -> list[dict]:
        return self._collect_section_analysis(sections)["totals"]

    def _build_normal_details(
        self,
        *,
        structured_analysis: dict,
        extracted_items: list[dict],
        extracted_totals: list[dict],
        sum_check: dict,
        row_issues: list[dict],
        duplicate_items: list[dict],
        unresolved_rows: list[dict],
        serial_gap_hints: list[str],
    ) -> list[str]:
        details = []
        if structured_analysis["used_tables"]:
            details.append(f"优先基于 {len(structured_analysis['used_tables'])} 张结构化表格进行分项金额与总价校验。")
        if structured_analysis.get("amount_only_item_count"):
            details.append(
                f"其中 {structured_analysis['amount_only_item_count']} 条分项仅声明金额，已参与汇总校验，不执行单价乘数量校验。"
            )
        if structured_analysis["group_checks"]:
            details.append(
                f"识别到 {len(structured_analysis['group_checks'])} 组因合并单元格导致的组总价重复展示，汇总时已按分项明细去重。"
            )
        if extracted_items:
            details.append(f"识别到 {len(extracted_items)} 个分项金额。")
        if extracted_totals:
            details.append(f"识别到 {len(extracted_totals)} 个合计/总价候选值。")

        if sum_check["status"] == "pass":
            if sum_check.get("total_mode") == "preferential_total":
                if sum_check.get("opening_total") is not None:
                    details.append("检测到最终优惠价模式，分项金额汇总、分项小计与开标一览表总价一致。")
                else:
                    details.append("检测到最终优惠价模式，分项金额汇总与分项小计一致。")
                if sum_check.get("preferential_total") is not None:
                    details.append(
                        f"文档同时声明最终优惠价 {sum_check['preferential_total']}（{sum_check.get('preferential_total_label') or '最终优惠价'}）。"
                    )
            else:
                details.append("分项金额汇总与声明总价一致。")
        elif sum_check["status"] == "fail":
            if sum_check.get("total_mode") == "preferential_total":
                if (
                    sum_check.get("subtotal_status") == "pass"
                    and sum_check.get("opening_total_status") == "fail"
                    and sum_check.get("opening_total") is not None
                ):
                    details.append(
                        f"分项金额汇总与分项小计一致，但与开标一览表总价不一致：分项小计 {sum_check.get('subtotal_total')}，开标一览表总价 {sum_check.get('opening_total')}。"
                    )
                elif sum_check.get("subtotal_status") == "fail":
                    details.append(
                        f"检测到最终优惠价模式，但分项金额汇总与分项小计不一致：计算值 {sum_check['calculated_total']}，小计 {sum_check.get('subtotal_total') or sum_check['declared_total']}。"
                    )
                    if sum_check.get("opening_total") is not None:
                        details.append(
                            f"同时，开标一览表总价为 {sum_check.get('opening_total')}（{sum_check.get('opening_total_label')}）。"
                        )
                else:
                    details.append(
                        f"检测到最终优惠价模式，但总价口径存在不一致：计算值 {sum_check['calculated_total']}，声明值 {sum_check['declared_total']}。"
                    )
            else:
                details.append(
                    f"分项金额汇总与声明总价不一致：计算值 {sum_check['calculated_total']}，声明值 {sum_check['declared_total']}。"
                )
        elif sum_check["status"] == "unknown":
            details.append("已识别到报价内容，但暂时无法可靠完成汇总校验。")
        else:
            details.append("未识别到足够的分项金额或总价信息。")

        if unresolved_rows:
            details.append(f"发现 {len(unresolved_rows)} 条未完整识别的分项行，当前结果可能受 OCR 拆行影响。")
        if row_issues:
            details.append(f"发现 {len(row_issues)} 条逐项算术疑点。")
        if duplicate_items:
            details.append(f"发现 {len(duplicate_items)} 组疑似重项。")
        if serial_gap_hints:
            details.append(
                f"提示：检测到序号可能跳号：{', '.join(serial_gap_hints)}。该提示仅供人工复核，不影响当前金额校验结论。"
            )
        return details

    def _check_normal_mode(
        self,
        item_sections: list[dict],
        total_sections: list[dict],
        candidate_sections: list[dict],
        *,
        document: dict | None = None,
    ) -> dict:
        item_source_sections = item_sections or total_sections or candidate_sections
        structured_analysis = self._extract_structured_itemized_entries(
            document,
            item_sections=item_sections,
        )
        extracted_items = list(structured_analysis["items"])
        extracted_totals = list(structured_analysis["totals"])
        row_issues = list(structured_analysis["row_issues"])
        unresolved_rows = list(structured_analysis["unresolved_rows"])
        preferential_mode = self._detect_preferential_total_mode(document)

        if structured_analysis["used_tables"]:
            extracted_totals.extend(self._collect_section_totals(total_sections or candidate_sections))
        else:
            section_analysis = self._collect_section_analysis(item_source_sections)
            extracted_items.extend(section_analysis["items"])
            extracted_totals.extend(section_analysis["totals"])
            row_issues.extend(section_analysis["row_issues"])
            unresolved_rows.extend(section_analysis["unresolved_rows"])

        if not extracted_items and total_sections:
            fallback_analysis = self._collect_section_analysis(total_sections)
            extracted_items.extend(fallback_analysis["items"])
            extracted_totals.extend(fallback_analysis["totals"])
            row_issues.extend(fallback_analysis["row_issues"])
            unresolved_rows.extend(fallback_analysis["unresolved_rows"])

        if preferential_mode and document is not None:
            extracted_totals.extend(self._extract_preferential_total_entries(document.get("lines") or []))

        if not extracted_totals or all(entry.get("is_subtotal") for entry in extracted_totals):
            extracted_totals.extend(self._collect_section_totals(total_sections or candidate_sections))

        extracted_items = self._dedupe_entries(extracted_items)
        extracted_totals = self._dedupe_entries(extracted_totals)
        row_issues = self._dedupe_row_issues(row_issues)
        unresolved_rows = self._dedupe_unresolved_rows(unresolved_rows)
        duplicate_items = self._extract_duplicate_items(extracted_items)
        serial_gap_hints = self._extract_serial_gap_hints(item_sections) if item_sections else []

        table_detected = bool(item_sections or total_sections or extracted_items or extracted_totals)
        sum_check = self._evaluate_sum_check(extracted_items, extracted_totals, preferential_mode=preferential_mode)
        status = self._resolve_normal_status(
            table_detected,
            sum_check["status"],
            row_issues,
            duplicate_items,
            unresolved_rows,
        )
        passed = self._status_to_passed(status)
        serialized_total_candidates = self._serialize_entries(extracted_totals)
        serialized_unresolved_rows = self._serialize_entries(unresolved_rows)
        details = self._build_normal_details(
            structured_analysis=structured_analysis,
            extracted_items=extracted_items,
            extracted_totals=extracted_totals,
            sum_check=sum_check,
            row_issues=row_issues,
            duplicate_items=duplicate_items,
            unresolved_rows=unresolved_rows,
            serial_gap_hints=serial_gap_hints,
        )
        manual_review = self._build_manual_review_payload(
            status=status,
            sum_check=sum_check,
            total_candidates=serialized_total_candidates,
            unresolved_rows=serialized_unresolved_rows,
            row_issues=row_issues,
        )

        return {
            "itemized_table_detected": table_detected,
            "mode": "normal",
            "status": status,
            "passed": passed,
            "summary": self._build_normal_summary(status, sum_check["status"], row_issues, duplicate_items, unresolved_rows),
            "checks": {
                "row_arithmetic": {
                    "status": (
                        "fail"
                        if row_issues
                        else (
                            "unknown"
                            if unresolved_rows
                            else (
                                "skipped"
                                if (
                                    structured_analysis.get("amount_only_item_count")
                                    and not structured_analysis["group_checks"]
                                    and not any(
                                        entry.get("relation_type") == "row_total"
                                        for entry in (structured_analysis.get("relation_rows") or [])
                                    )
                                )
                                else ("not_detected" if not table_detected else "pass")
                            )
                        )
                    ),
                    "issue_count": len(row_issues),
                    "issues": row_issues,
                    "unresolved_count": len(unresolved_rows),
                    "unresolved_rows": serialized_unresolved_rows,
                    "skipped_count": int(structured_analysis.get("amount_only_item_count") or 0),
                    "group_check_count": len(structured_analysis["group_checks"]),
                    "group_checks": self._serialize_entries(structured_analysis["group_checks"]),
                },
                "sum_consistency": {
                    "status": "unknown" if unresolved_rows and sum_check["status"] == "pass" else sum_check["status"],
                    "calculated_total": sum_check["calculated_total"],
                    "declared_total": sum_check["declared_total"],
                    "difference": sum_check["difference"],
                    "matched_total_label": sum_check["matched_total_label"],
                    "total_mode": sum_check.get("total_mode"),
                    "preferential_total": sum_check.get("preferential_total"),
                    "preferential_total_label": sum_check.get("preferential_total_label"),
                    "subtotal_total": sum_check.get("subtotal_total"),
                    "subtotal_label": sum_check.get("subtotal_label"),
                    "subtotal_difference": sum_check.get("subtotal_difference"),
                    "subtotal_status": sum_check.get("subtotal_status"),
                    "opening_total": sum_check.get("opening_total"),
                    "opening_total_label": sum_check.get("opening_total_label"),
                    "opening_total_difference": sum_check.get("opening_total_difference"),
                    "opening_total_status": sum_check.get("opening_total_status"),
                },
                "duplicate_items": {
                    "status": "fail" if duplicate_items else ("not_detected" if not table_detected else "pass"),
                    "issue_count": len(duplicate_items),
                    "issues": duplicate_items,
                },
                "missing_item": {
                    "status": "not_applicable",
                    "missing_items": [],
                    "comparison_basis": None,
                    "hints": serial_gap_hints,
                    "hint_level": "info" if serial_gap_hints else None,
                },
            },
            "evidence": {
                "analysis_basis": "structured_logical_tables" if structured_analysis["used_tables"] else "text_sections",
                "structured_tables": structured_analysis["used_tables"],
                "structured_relation_count": len(structured_analysis["relation_rows"]),
                "structured_relations": self._serialize_entries(structured_analysis["relation_rows"]),
                "structured_group_checks": self._serialize_entries(structured_analysis["group_checks"]),
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": serialized_total_candidates,
                "unresolved_rows": serialized_unresolved_rows,
            },
            "manual_review": manual_review,
            "details": details,
        }

    # 执行下浮率报价模式下的列项抽取和删减项比对。
    def _check_downward_rate_mode(self, candidate_sections: list[dict], tender_document: dict | None = None) -> dict:
        relevant_sections = [
            section for section in candidate_sections if any(keyword in "\n".join(section["lines"]) for keyword in self.RATE_KEYWORDS)
        ]
        if not relevant_sections:
            relevant_sections = candidate_sections

        serials = []
        extracted_items = []
        for section in relevant_sections:
            serials.extend(self._extract_serials(section["lines"]))
            extracted_items.extend(self._extract_rate_items(section["lines"], section_context=section))

        extracted_items = self._dedupe_entries(extracted_items)
        serial_gap_hints = self._find_missing_serials(serials)
        comparison_items = self._extract_comparison_items_from_sections(relevant_sections, rate_mode=True)
        reference_items = self._extract_reference_items(tender_document) if tender_document else []
        comparison_result = self._compare_reference_items(reference_items, comparison_items) if reference_items else None

        if comparison_result is None:
            missing_items = []
            missing_item_status = "unknown"
            comparison_basis = None
            status = "unknown"
        else:
            missing_items = comparison_result["missing_items"]
            missing_item_status = "fail" if missing_items else "pass"
            comparison_basis = comparison_result["comparison_basis"]
            status = "fail" if missing_items else "pass"

        details = [
            "检测到下浮率模式，按业务规则跳过下浮率数值本身的校验。",
        ]
        if comparison_result is None:
            details.append("当前未提供招标文件，无法完成招标列项与投标列项的删减项比对。")
        elif missing_items:
            details.append(f"对比招标列项后发现疑似删减项：{', '.join(missing_items)}。")
        else:
            details.append("已对比招标文件与投标文件列项，暂未发现明显删减项。")
        if serial_gap_hints:
            details.append(
                f"提示：投标文件内部检测到序号可能跳号：{', '.join(serial_gap_hints)}。该提示仅供人工复核，不直接作为删减项判定依据。"
            )

        return {
            "itemized_table_detected": bool(relevant_sections or extracted_items or comparison_items),
            "mode": "downward_rate",
            "status": status,
            "passed": self._status_to_passed(status),
            "summary": self._build_downward_rate_summary(missing_item_status),
            "checks": {
                "row_arithmetic": {
                    "status": "skipped",
                    "issue_count": 0,
                    "issues": [],
                },
                "sum_consistency": {
                    "status": "skipped",
                    "calculated_total": None,
                    "declared_total": None,
                    "difference": None,
                    "matched_total_label": None,
                },
                "duplicate_items": {
                    "status": "skipped",
                    "issue_count": 0,
                    "issues": [],
                },
                "missing_item": {
                    "status": missing_item_status,
                    "missing_items": missing_items,
                    "comparison_basis": comparison_basis,
                    "hints": serial_gap_hints,
                    "hint_level": "info" if serial_gap_hints else None,
                },
            },
            "evidence": {
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": [],
                "comparison_items": self._serialize_entries(comparison_items),
                "reference_item_count": len(reference_items),
                "reference_items": self._serialize_entries(reference_items),
            },
            "details": details,
        }

    # 优惠价/小计模式识别
    # 识别文档是否存在“小计 + 最终优惠价”的特殊总价模式。
    def _detect_preferential_total_mode(self, document: dict | None) -> bool:
        if not document:
            return False
        lines = document.get("lines") or []
        preferential_entries = self._extract_preferential_total_entries(lines)
        if not preferential_entries:
            return False

        subtotal_entries = self._extract_document_subtotal_entries(lines)
        if not subtotal_entries:
            return False

        for preferential_entry in preferential_entries:
            for subtotal_entry in subtotal_entries:
                if abs(preferential_entry["line_index"] - subtotal_entry["line_index"]) > self.PREFERENTIAL_TOTAL_LINE_WINDOW:
                    continue
                if preferential_entry["amount"] <= subtotal_entry["amount"] + self.MONEY_TOLERANCE:
                    return True
        return False

    # 从全文中提取“最终优惠价/优惠价”等特殊总价声明。
    def _extract_preferential_total_entries(self, lines: list[str]) -> list[dict]:
        entries = []
        for idx, line in enumerate(lines):
            if not self._looks_like_preferential_total_line(line):
                continue
            amounts = self._extract_money_candidates(line)
            if len(amounts) != 1:
                continue
            entries.append(
                {
                    "label": self._clean_label(line) or "最终优惠价",
                    "amount": amounts[0],
                    "source": "preferential_total",
                    "is_total": True,
                    "is_preferential_total": True,
                    "line_index": idx,
                }
            )
        return entries

    # 从全文中提取小计行，供优惠价模式下对账使用。
    def _extract_document_subtotal_entries(self, lines: list[str]) -> list[dict]:
        entries = []
        for idx, line in enumerate(lines):
            if not self._looks_like_subtotal_line(line):
                continue
            amounts = self._extract_money_candidates(line)
            if not amounts:
                continue
            entries.append(
                {
                    "label": self._clean_label(line) or "小计",
                    "amount": amounts[-1],
                    "source": "document_subtotal",
                    "is_total": True,
                    "is_subtotal": True,
                    "line_index": idx,
                }
            )
        return entries

    # 判断某一行是否像“最终优惠价”声明，而不是普通分项或表头。
    def _looks_like_preferential_total_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not compact or self._is_table_header_line(line):
            return False
        if len(self._extract_money_candidates(line)) != 1:
            return False
        if self._looks_like_item_row(line):
            return False

        strong_keywords = ("最终优惠价", "优惠价")
        weak_keywords = tuple(keyword for keyword in self.PREFERENTIAL_TOTAL_KEYWORDS if keyword not in strong_keywords)
        has_strong_keyword = any(keyword in compact for keyword in strong_keywords)
        has_weak_keyword = any(keyword in compact for keyword in weak_keywords)
        if not (has_strong_keyword or has_weak_keyword):
            return False
        if has_strong_keyword:
            return True
        return any(keyword in compact for keyword in self.TOTAL_KEYWORDS)

    # 判断总价标签是否更像开标一览表/总报价口径的声明总价。
    def _looks_like_opening_total_label(self, label: str | None) -> bool:
        compact = re.sub(r"\s+", "", str(label or ""))
        if not compact:
            return False
        return any(keyword in compact for keyword in self.OPENING_TOTAL_KEYWORDS)

    # 从总价候选值中优先挑出开标一览表或投标总价口径的声明总价。
    def _select_opening_total_candidate(self, totals: list[dict]) -> dict | None:
        preferred_candidates = [
            item
            for item in totals
            if not item.get("is_subtotal") and self._looks_like_opening_total_label(item.get("label"))
        ]
        if preferred_candidates:
            return min(
                preferred_candidates,
                key=lambda item: (
                    0 if "投标总价" in str(item.get("label") or "") else 1,
                    0 if item.get("source") == "explicit_amount" else 1 if item.get("source") == "table_total" else 2,
                    len(str(item.get("label") or "")),
                ),
            )

        preferential_candidates = [item for item in totals if item.get("is_preferential_total")]
        if preferential_candidates:
            return min(
                preferential_candidates,
                key=lambda item: (
                    0 if item.get("source") == "explicit_amount" else 1,
                    len(str(item.get("label") or "")),
                ),
            )
        return None

    # 分项抽取与表格行重建
    # 从一个候选区段中抽取分项、总价、算术疑点和未完整识别的行。
    def _extract_section_entries(
        self,
        lines: list[str],
        *,
        section_context: dict | None = None,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        explicit_entries = self._extract_explicit_amount_entries(lines, section_context=section_context)
        table_items = []
        table_totals = []
        row_issues = []
        unresolved_rows = []
        row_blocks = self._build_table_row_blocks(lines)
        parent_serials = self._collect_parent_serials(row_blocks)

        for idx, line in enumerate(lines):
            if self._should_skip_line(line):
                continue

            if self._is_table_header_line(line):
                continue
            if self._looks_like_total_line(line):
                amounts = self._extract_money_candidates(line)
                if amounts:
                    is_subtotal = self._looks_like_subtotal_line(line)
                    table_totals.append(
                        {
                            "label": self._clean_label(line) or ("小计" if is_subtotal else "合计"),
                            "amount": amounts[-1],
                            "source": "table_subtotal" if is_subtotal else "table_total",
                            "is_subtotal": is_subtotal,
                            **self._build_entry_context(section_context, line_index=idx),
                        }
                    )
                break

        for block in row_blocks:
            block_text = " ".join(block["lines"])
            amounts = self._extract_row_amounts(block_text)
            if not amounts:
                if block.get("serial") in parent_serials:
                    continue
                if self._is_unresolved_item_block(block_text):
                    unresolved_rows.append(
                        {
                            "serial": block.get("serial"),
                            "label": self._extract_block_label(block),
                            "text": block_text[:160],
                            "reason": "item_amount_missing",
                            "reason_text": "该行看起来像分项行，但未识别到可用金额。",
                        }
                    )
                continue

            table_items.append(
                {
                    "label": self._extract_block_label(block),
                    "amount": amounts[-1],
                    "source": "table_row",
                    **self._build_entry_context(
                        section_context,
                        serial=block.get("serial"),
                        line_index=block.get("start_index"),
                    ),
                }
            )

            arithmetic_info = self._extract_row_arithmetic(block_text)
            if arithmetic_info is None:
                continue

            expected_total = arithmetic_info["quantity"] * arithmetic_info["unit_price"]
            difference = expected_total - arithmetic_info["line_total"]
            if abs(difference) > self.MONEY_TOLERANCE:
                row_issues.append(
                    {
                        "label": self._extract_block_label(block),
                        "quantity": self._format_decimal(arithmetic_info["quantity"]),
                        "unit_price": self._format_decimal(arithmetic_info["unit_price"]),
                        "line_total": self._format_decimal(arithmetic_info["line_total"]),
                        "expected_total": self._format_decimal(expected_total),
                        "difference": self._format_decimal(difference),
                    }
                )

        items = [entry for entry in explicit_entries if not entry["is_total"]]
        totals = [entry for entry in explicit_entries if entry["is_total"]]
        items.extend(table_items)
        totals.extend(table_totals)
        return items, totals, row_issues, unresolved_rows

    # 收集层级序号中的父级编号，用于忽略只有标题作用的父行。
    def _collect_parent_serials(self, row_blocks: list[dict]) -> set[str]:
        parent_serials: set[str] = set()
        for block in row_blocks:
            serial = str(block.get("serial") or "").strip()
            if "." not in serial:
                continue
            parts = serial.split(".")
            for index in range(1, len(parts)):
                parent_serials.add(".".join(parts[:index]))
        return parent_serials

    # 把连续多行文本重新拼装成按报价项分组的表格行块。
    def _build_table_row_blocks(self, lines: list[str]) -> list[dict]:
        blocks = []
        current_block = None

        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if self._should_skip_line(line):
                continue
            if compact.startswith("随机备品备件") or ("备件名称" in compact and "规格型号" in compact):
                current_block = self._flush_table_row_block(blocks, current_block)
                break
            if self._is_table_header_line(line):
                current_block = self._flush_table_row_block(blocks, current_block)
                continue
            if self._looks_like_total_line(line):
                current_block = self._flush_table_row_block(blocks, current_block)
                break
            if self._is_row_start_line(line):
                current_block = self._flush_table_row_block(blocks, current_block)
                current_block = {
                    "start_index": idx,
                    "serial": self._extract_row_serial(line),
                    "lines": [line],
                }
                continue
            if self._should_split_amount_continuation(current_block, line):
                inherited_line = line
                inherited_serial = str((current_block or {}).get("serial") or "").strip()
                if inherited_serial and not self._extract_row_serial(line):
                    inherited_line = f"{inherited_serial} {line}".strip()
                current_block = self._flush_table_row_block(blocks, current_block)
                current_block = {
                    "start_index": idx,
                    "serial": inherited_serial or self._extract_row_serial(inherited_line),
                    "lines": [inherited_line],
                }
                continue
            if current_block is not None and self._is_row_continuation_line(line):
                current_block["lines"].append(line)
                continue
            current_block = self._flush_table_row_block(blocks, current_block)

        self._flush_table_row_block(blocks, current_block)
        return blocks

    # 将当前构建中的行块写入结果列表。
    def _flush_table_row_block(self, blocks: list[dict], block: dict | None) -> None:
        if block and block.get("lines"):
            blocks.append(block)
        return None

    # 判断续行是否应拆成新分项，处理 OCR 把金额拆到下一行的情况。
    def _should_split_amount_continuation(self, current_block: dict | None, line: str) -> bool:
        if current_block is None:
            return False
        if not self._is_row_continuation_line(line):
            return False
        if not self._extract_row_amounts(line):
            return False
        return bool(current_block.get("serial"))

    # 判断一行是否像新的分项起始行。
    def _is_row_start_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not re.search(r"[\u4e00-\u9fff]", compact):
            return False
        if self._looks_like_total_line(compact) or self._is_heading_line(line) or self._is_table_header_line(line):
            return False
        if self._extract_row_serial(line):
            return True
        if re.match(r"^\s*\d", line):
            return False
        return bool(self._looks_like_item_row(line) and self._extract_money_candidates(line))

    # 判断一行是否像当前分项的续行。
    def _is_row_continuation_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not compact:
            return False
        if self._should_skip_line(line) or self._looks_like_total_line(line) or self._is_heading_line(line):
            return False
        if self._is_table_header_line(line) or self._is_row_start_line(line):
            return False
        return bool(re.search(r"[\u4e00-\u9fff]", compact) or self._extract_money_candidates(line))

    # 从一个行块的首行提取分项名称。
    def _extract_block_label(self, block: dict) -> str:
        first_line = (block.get("lines") or [""])[0]
        start_index = int(block.get("start_index", 0))
        return self._extract_row_label(first_line, start_index)

    # 判断行块是否像分项，但因缺少金额而只能标记为未完整识别。
    def _is_unresolved_item_block(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        if not compact or "免费" in compact:
            return False
        if not self._extract_row_serial(text):
            return False
        return bool(re.search(r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡|设备|系统|服务|子系统)", compact))

    # 提取“小写金额/报价金额”这类显式金额声明行。
    def _extract_explicit_amount_entries(self, lines: list[str], *, section_context: dict | None = None) -> list[dict]:
        entries = []
        for idx, line in enumerate(lines):
            if "小写" not in line and "金额" not in line and "报价" not in line:
                continue

            amounts = self._extract_money_candidates(line)
            if len(amounts) != 1:
                continue

            if self._looks_like_total_line(line) and ("合计" in line or "总计" in line):
                label = "合计"
            else:
                label = self._resolve_neighbor_label(lines, idx)
            if not label:
                continue

            entries.append(
                {
                    "label": label,
                    "amount": amounts[0],
                    "source": "explicit_amount",
                    "is_total": self._looks_like_total_line(label) or self._looks_like_total_line(line),
                    **self._build_entry_context(
                        section_context,
                        serial=self._extract_row_serial(line),
                        line_index=idx,
                    ),
                }
            )
        return entries

    # 为显式金额行回溯或前瞻寻找对应的业务标签。
    def _resolve_neighbor_label(self, lines: list[str], index: int) -> str | None:
        current_label = self._clean_label(lines[index])
        if current_label and "小写" not in current_label and "大写" not in current_label:
            return current_label

        for offset in (1, -1, 2, -2):
            cursor = index + offset
            if cursor < 0 or cursor >= len(lines):
                continue
            candidate = lines[cursor]
            if self._should_skip_line(candidate):
                continue
            if "大写" in candidate or "投标人名称" in candidate or "日期" in candidate:
                continue
            if not re.search(r"[\u4e00-\u9fff]", candidate):
                continue
            cleaned = self._clean_label(candidate)
            if cleaned:
                return cleaned
        return None

    # 过滤明显无意义的空行、注释行、落款行和纯符号行。
    def _should_skip_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not compact:
            return True
        if compact in {"注：", "注"}:
            return True
        if re.fullmatch(r"[-—_·\.0-9/（）()]+", compact):
            return True
        if compact.startswith("投标人名称") or compact.startswith("日期"):
            return True
        if compact.startswith("大写"):
            return True
        return False

    # 金额、标签与算术校验
    # 判断一行是否属于合计、总计、总价等汇总行。
    def _looks_like_total_line(self, line: str) -> bool:
        if self._is_table_header_line(line):
            return False
        compact = re.sub(r"\s+", "", line)
        return any(keyword in compact for keyword in self.TOTAL_KEYWORDS) or self._looks_like_subtotal_line(compact)

    # 判断一行是否属于小计行。
    def _looks_like_subtotal_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return any(keyword in compact for keyword in self.SUBTOTAL_KEYWORDS)

    # 判断一行是否具有分项名称、单位、数量等明细行特征。
    def _looks_like_item_row(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not re.search(r"[\u4e00-\u9fff]", compact):
            return False
        if self._looks_like_total_line(compact):
            return False
        if re.match(r"^\d+(?:\.\d+)?", compact):
            return True
        unit_pattern = "|".join(re.escape(unit) for unit in self.UNIT_KEYWORDS)
        return bool(re.search(rf"(?:{unit_pattern})\s*\d+(?:\.\d+)?", compact))

    # 从单行文本中清理出分项名称标签。
    def _extract_row_label(self, line: str, index: int) -> str:
        unit_pattern = "|".join(re.escape(unit) for unit in self.UNIT_KEYWORDS)
        label = re.sub(r"^\s*\d+(?:\.\d+)*\s*[\.、．）)]?\s*", "", line)
        label = re.sub(r"\s*\d+(?:\.\d+)*(?:[\.、．）)])\s*$", "", label)
        label = re.sub(
            rf"\s+(?:￥|¥)?\s*\d[\d,]*(?:\.\d{{1,2}})?\s+\d+(?:\.\d+)?(?:\s*(?:{unit_pattern}))?\s+(?:￥|¥)?\s*\d[\d,]*(?:\.\d{{1,2}})?\s*$",
            "",
            label,
        )
        label = re.sub(r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label)
        label = re.sub(r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$", "", label)
        label = re.sub(r"\s+", " ", label).strip()
        return label[:60] if label else f"第{index + 1}行"

    # 从一行中提取数量、单价、行总价，用于逐项算术校验。
    def _extract_row_arithmetic(self, line: str) -> dict | None:
        money_pattern = r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?"
        unit_pattern = "|".join(re.escape(unit) for unit in self.UNIT_KEYWORDS)
        candidate_patterns = (
            re.finditer(
                rf"(?P<unit_price>{money_pattern})\s+(?P<qty>\d+(?:\.\d+)?)(?:\s*(?:{unit_pattern}))?\s+(?P<total>{money_pattern})",
                line,
            ),
            re.finditer(
                rf"(?P<qty>\d+(?:\.\d+)?)(?:\s*(?:{unit_pattern}))?\s+(?P<unit_price>{money_pattern})\s+(?P<total>{money_pattern})",
                line,
            ),
        )
        for matches in candidate_patterns:
            for match in reversed(list(matches)):
                quantity = self._to_quantity_decimal(match.group("qty"))
                unit_price = self._to_decimal(match.group("unit_price"))
                line_total = self._to_decimal(match.group("total"))
                if quantity is None or unit_price is None or line_total is None:
                    continue
                if quantity <= 0 or quantity > Decimal("100000"):
                    continue
                if not self._looks_like_money_value(unit_price) or not self._looks_like_money_value(line_total):
                    continue
                return {
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
        return None

    # 提取一行里的金额候选值，并过滤百分比和技术参数数字。
    def _extract_money_candidates(self, line: str) -> list[Decimal]:
        candidates = []
        for match in re.finditer(r"(?:￥|¥)?\s*((?:\d+,\d{4,}|\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)", line):
            value = self._to_decimal(match.group(1))
            if value is None:
                continue
            around = line[max(0, match.start() - 3): min(len(line), match.end() + 4)]
            suffix = line[match.end(): min(len(line), match.end() + 5)]
            if "%" in around or "％" in around:
                continue
            if re.match(r"\s*(?:℃|°C|mm|cm|kg|g|GHz|MHz|kW|dB)(?:\b|$)", suffix, re.IGNORECASE):
                continue
            if not self._looks_like_money_value(value):
                continue
            if re.search(r"(年|月|日|页|GHz|MHz|kW|dB|mm|cm)", around, re.IGNORECASE) and value < Decimal("1000"):
                continue
            candidates.append(value)
        return candidates

    # 识别“免费/包含”等应被视为 0 金额的分项。
    def _extract_zero_amount_candidate(self, line: str) -> Decimal | None:
        normalized = re.sub(r"\s+", " ", line).strip()
        if not normalized or not self._extract_row_serial(normalized):
            return None
        if not any(keyword in normalized for keyword in self.ZERO_AMOUNT_KEYWORDS):
            return None

        unit_pattern = "|".join(re.escape(unit) for unit in self.UNIT_KEYWORDS)
        if re.search(rf"(?:{unit_pattern})\s*\d+(?:\.\d+)?\s+0(?:\.\d{{1,2}})?(?:\s|$)", normalized, re.IGNORECASE):
            return Decimal("0")
        if "免费" in normalized:
            return Decimal("0")
        return None

    # 抽取一行或行块中的金额，必要时回退到零金额识别。
    def _extract_row_amounts(self, line: str) -> list[Decimal]:
        amounts = self._extract_money_candidates(line)
        if amounts:
            return amounts
        zero_amount = self._extract_zero_amount_candidate(line)
        return [zero_amount] if zero_amount is not None else []

    # 判断一个数字是否足够像报价金额而不是普通参数值。
    def _looks_like_money_value(self, value: Decimal) -> bool:
        return value >= Decimal("100")

    def _sum_entry_amounts(self, entries: list[dict]) -> Decimal:
        return sum((entry["amount"] for entry in entries if entry.get("amount") is not None), Decimal("0"))

    def _build_sum_check_result(
        self,
        *,
        status: str,
        calculated_total: Decimal | None,
        declared_total: Decimal | None = None,
        difference: Decimal | None = None,
        matched_total_label: str | None = None,
        total_mode: str = "standard",
        preferential_total: Decimal | None = None,
        preferential_total_label: str | None = None,
        subtotal_total: Decimal | None = None,
        subtotal_label: str | None = None,
        subtotal_difference: Decimal | None = None,
        subtotal_status: str | None = None,
        opening_total: Decimal | None = None,
        opening_total_label: str | None = None,
        opening_total_difference: Decimal | None = None,
        opening_total_status: str | None = None,
    ) -> dict:
        return {
            "status": status,
            "calculated_total": self._format_decimal(calculated_total),
            "declared_total": self._format_decimal(declared_total),
            "difference": self._format_decimal(difference),
            "matched_total_label": matched_total_label,
            "total_mode": total_mode,
            "preferential_total": self._format_decimal(preferential_total),
            "preferential_total_label": preferential_total_label,
            "subtotal_total": self._format_decimal(subtotal_total),
            "subtotal_label": subtotal_label,
            "subtotal_difference": self._format_decimal(subtotal_difference),
            "subtotal_status": subtotal_status,
            "opening_total": self._format_decimal(opening_total),
            "opening_total_label": opening_total_label,
            "opening_total_difference": self._format_decimal(opening_total_difference),
            "opening_total_status": opening_total_status,
        }

    def _evaluate_preferential_sum_check(self, calculated_total: Decimal, totals: list[dict]) -> dict | None:
        subtotal_candidates = [item for item in totals if item.get("is_subtotal")]
        preferential_candidates = [item for item in totals if item.get("is_preferential_total")]
        if not subtotal_candidates:
            return None

        best_subtotal = min(
            subtotal_candidates,
            key=lambda item: abs(item["amount"] - calculated_total),
        )
        subtotal_difference = calculated_total - best_subtotal["amount"]
        subtotal_status = "pass" if abs(subtotal_difference) <= self.MONEY_TOLERANCE else "fail"

        best_preferential_total = None
        if preferential_candidates:
            best_preferential_total = min(
                preferential_candidates,
                key=lambda item: abs(item["amount"] - calculated_total),
            )

        best_opening_total = self._select_opening_total_candidate(totals)
        opening_total_difference = (
            calculated_total - best_opening_total["amount"]
            if best_opening_total is not None
            else None
        )
        opening_total_status = (
            "pass"
            if best_opening_total is not None and abs(opening_total_difference) <= self.MONEY_TOLERANCE
            else ("fail" if best_opening_total is not None else None)
        )

        matched_total = best_subtotal
        matched_difference = subtotal_difference
        overall_status = subtotal_status
        if best_opening_total is not None:
            matched_total = best_opening_total
            matched_difference = opening_total_difference
            if opening_total_status == "fail":
                overall_status = "fail"

        return self._build_sum_check_result(
            status=overall_status,
            calculated_total=calculated_total,
            declared_total=matched_total["amount"],
            difference=matched_difference,
            matched_total_label=matched_total["label"],
            total_mode="preferential_total",
            preferential_total=(best_preferential_total or {}).get("amount"),
            preferential_total_label=(best_preferential_total or {}).get("label"),
            subtotal_total=best_subtotal["amount"],
            subtotal_label=best_subtotal["label"],
            subtotal_difference=subtotal_difference,
            subtotal_status=subtotal_status,
            opening_total=(best_opening_total or {}).get("amount"),
            opening_total_label=(best_opening_total or {}).get("label"),
            opening_total_difference=opening_total_difference,
            opening_total_status=opening_total_status,
        )

    def _evaluate_sum_check(self, items: list[dict], totals: list[dict], *, preferential_mode: bool = False) -> dict:
        calculated_total = self._sum_entry_amounts(items)
        if len(items) < 2 or not totals:
            return self._build_sum_check_result(
                status="unknown" if items else "not_detected",
                calculated_total=calculated_total if items else None,
                total_mode="preferential_total" if preferential_mode else "standard",
            )

        if preferential_mode:
            preferential_result = self._evaluate_preferential_sum_check(calculated_total, totals)
            if preferential_result is not None:
                return preferential_result

        best_total = min(
            totals,
            key=lambda item: (
                1 if item.get("is_subtotal") else 0,
                abs(item["amount"] - calculated_total),
                0 if "总价" in item["label"] or "合计" in item["label"] else 1,
            ),
        )
        difference = calculated_total - best_total["amount"]
        return self._build_sum_check_result(
            status="pass" if abs(difference) <= self.MONEY_TOLERANCE else "fail",
            calculated_total=calculated_total,
            declared_total=best_total["amount"],
            difference=difference,
            matched_total_label=best_total["label"],
        )

    # 下浮率模式列项抽取与对比
    # 从候选区段中抽取所有可识别的序号。
    def _extract_serials(self, lines: list[str]) -> list[str]:
        serials = []
        for line in lines:
            compact = re.sub(r"\s+", "", line)
            if not re.search(r"[\u4e00-\u9fff]", compact):
                continue
            if self._is_heading_line(compact):
                continue
            if not (
                self._looks_like_item_row(line)
                or any(keyword in line for keyword in self.RATE_KEYWORDS)
                or bool(self._extract_money_candidates(line))
            ):
                continue
            serial = self._extract_row_serial(line)
            if serial:
                serials.append(serial)
        return serials

    # 汇总多个区段中的序号，并给出可能的跳号提示。
    def _extract_serial_gap_hints(self, sections: list[dict]) -> list[str]:
        serials = []
        for section in sections:
            serials.extend(self._extract_serials(section["lines"]))
        return self._find_missing_serials(serials)

    # 在下浮率模式下提取可用于比对的列项标签。
    def _extract_rate_items(self, lines: list[str], *, section_context: dict | None = None) -> list[dict]:
        items = []
        for idx, line in enumerate(lines):
            if not any(keyword in line for keyword in self.RATE_KEYWORDS) and "%" not in line and "％" not in line:
                continue
            if "序号" in line and "项目名称" in line:
                continue
            label = self._extract_row_label(line, idx)
            if not label:
                continue
            items.append(
                {
                    "label": label,
                    "amount": None,
                    "source": "downward_rate",
                    **self._build_entry_context(
                        section_context,
                        serial=self._extract_row_serial(line),
                        line_index=idx,
                    ),
                }
            )
        return items

    # 从招标参考文档中提取标准列项集合。
    def _extract_reference_items(self, document: dict | None) -> list[dict]:
        if not document:
            return []
        item_sections = document.get("item_sections") or []
        if not item_sections:
            lines = document.get("lines") or []
            item_sections = self._find_sections(lines, self.ITEM_SECTION_ANCHORS, require_score=False)
        return self._extract_comparison_items_from_sections(item_sections, rate_mode=False)

    # 从多个区段中提取列项，并按序号与标签去重。
    def _extract_comparison_items_from_sections(self, sections: list[dict], *, rate_mode: bool) -> list[dict]:
        items = []
        for section in sections:
            items.extend(self._extract_comparison_items(section["lines"], rate_mode=rate_mode))

        deduped = []
        seen = set()
        for item in items:
            key = (item.get("serial"), item.get("label_key"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    # 从单个区段中提取用于招投标比对的列项。
    def _extract_comparison_items(self, lines: list[str], *, rate_mode: bool) -> list[dict]:
        items = []
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if not compact:
                continue
            if compact.startswith("随机备品备件") or ("备件名称" in compact and "规格型号" in compact):
                break
            if self._should_skip_line(line) or self._looks_like_total_line(line):
                if self._looks_like_total_line(line):
                    break
                continue
            if "序号" in line and "名称" in line:
                continue
            if not re.search(r"[\u4e00-\u9fff]", compact):
                continue

            serial = self._extract_row_serial(line)
            has_rate = any(keyword in line for keyword in self.RATE_KEYWORDS) or "%" in line or "％" in line
            if not (self._looks_like_item_row(line) or serial or has_rate):
                continue
            if rate_mode and not has_rate and not serial:
                continue

            label = self._extract_comparison_label(line, idx, rate_mode=rate_mode)
            if not label:
                continue
            items.append(
                {
                    "serial": serial,
                    "label": label,
                    "label_key": self._normalize_label_key(label),
                    "source": "rate_item" if rate_mode else "reference_item",
                }
            )
        return items

    # 清洗比较用标签，去掉金额、单位和下浮率尾巴。
    def _extract_comparison_label(self, line: str, index: int, *, rate_mode: bool) -> str:
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s*", "", line)
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s+", "", label)
        if rate_mode:
            label = re.split(r"(?:下浮率|优惠率|折扣率|折让率|下浮|%|％)", label, maxsplit=1)[0]
        label = re.sub(r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label)
        label = re.sub(r"\b(?:免费)\b.*$", "", label)
        label = re.sub(r"\s*(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$", "", label)
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label[:80] if label else f"第{index + 1}行"

    # 比较招标与投标列项，输出疑似缺失项。
    def _compare_reference_items(self, reference_items: list[dict], bid_items: list[dict]) -> dict:
        reference_with_serial = [item for item in reference_items if item.get("serial")]
        bid_serials = {item["serial"] for item in bid_items if item.get("serial")}
        missing_items = []
        comparison_basis = "tender_vs_bid_label"

        if reference_with_serial and bid_serials:
            comparison_basis = "tender_vs_bid_serial"
            for item in reference_with_serial:
                if item["serial"] in bid_serials:
                    continue
                missing_items.append(self._format_comparison_item(item))
        else:
            bid_label_keys = {item["label_key"] for item in bid_items if item.get("label_key")}
            for item in reference_items:
                label_key = item.get("label_key")
                if not label_key or label_key in bid_label_keys:
                    continue
                missing_items.append(self._format_comparison_item(item))

        deduped_missing = []
        seen = set()
        for item in missing_items:
            if item in seen:
                continue
            seen.add(item)
            deduped_missing.append(item)
        return {
            "comparison_basis": comparison_basis,
            "missing_items": deduped_missing,
        }

    # 把列项格式化成便于展示和人工复核的字符串。
    def _format_comparison_item(self, item: dict) -> str:
        serial = item.get("serial")
        label = item.get("label")
        if serial and label:
            return f"{serial}:{label}"
        return label or str(serial or "")

    # 识别分项金额中标签、金额和上下文完全重复的疑似重项。
    def _extract_duplicate_items(self, entries: list[dict]) -> list[dict]:
        duplicate_keys = []
        for entry in entries:
            key = self._entry_duplicate_key(entry)
            if not key[0]:
                continue
            duplicate_keys.append(key)

        duplicates = []
        for duplicate_key, count in Counter(duplicate_keys).items():
            if count <= 1:
                continue
            duplicates.append(
                {
                    "label": duplicate_key[0],
                    "amount": duplicate_key[1],
                    "context": duplicate_key[2],
                    "count": count,
                }
            )
        return duplicates

    # 根据整数序号和子序号推断可能缺失的编号。
    def _find_missing_serials(self, serials: list[str]) -> list[str]:
        if not serials:
            return []

        missing = []
        int_serials = sorted({int(serial) for serial in serials if serial.isdigit()})
        if len(int_serials) >= 3 and int_serials[-1] - int_serials[0] > len(int_serials) + 5:
            int_serials = []
        for left, right in zip(int_serials, int_serials[1:]):
            if right - left <= 1:
                continue
            missing.extend([str(number) for number in range(left + 1, right)])

        grouped_children = {}
        for serial in serials:
            if "." not in serial:
                continue
            prefix, child = serial.split(".", 1)
            if not prefix.isdigit() or not child.isdigit():
                continue
            grouped_children.setdefault(prefix, []).append(int(child))

        for prefix, children in grouped_children.items():
            ordered_children = sorted(set(children))
            for left, right in zip(ordered_children, ordered_children[1:]):
                if right - left <= 1:
                    continue
                missing.extend([f"{prefix}.{number}" for number in range(left + 1, right)])
        return missing

    # 状态汇总与结果格式化
    # 综合汇总校验、算术疑点和 OCR 完整度，给出普通模式总状态。
    def _resolve_normal_status(
        self,
        table_detected: bool,
        sum_status: str,
        row_issues: list[dict],
        duplicate_items: list[dict],
        unresolved_rows: list[dict],
    ) -> str:
        if not table_detected:
            return "not_detected"
        if row_issues or sum_status == "fail" or duplicate_items:
            return "fail"
        if unresolved_rows:
            return "unknown"
        if sum_status == "pass":
            return "pass"
        return "unknown"

    # 把字符串状态转换为布尔 passed 标记。
    def _status_to_passed(self, status: str) -> bool | None:
        if status == "pass":
            return True
        if status == "fail":
            return False
        return None

    # 生成普通报价模式下的摘要结论。
    def _build_normal_summary(
        self,
        status: str,
        sum_status: str,
        row_issues: list[dict],
        duplicate_items: list[dict],
        unresolved_rows: list[dict],
    ) -> str:
        if status == "not_detected":
            return "未识别到可用于校验的分项报价表或报价一览表。"
        if status == "pass":
            return "分项报价检查通过。"
        if row_issues and sum_status == "fail":
            return "发现逐项算术错误，且分项汇总与声明总价不一致。"
        if row_issues:
            return "发现逐项算术错误。"
        if duplicate_items:
            return "发现疑似重项。"
        if sum_status == "fail":
            return "分项汇总与声明总价不一致。"
        if unresolved_rows:
            return "已识别到报价内容，但存在未完整识别的分项行，暂无法完成可靠校验。"
        return "已识别到报价内容，但当前证据不足以完成完整校验。"

    def _build_manual_review_payload(
        self,
        *,
        status: str,
        sum_check: dict,
        total_candidates: list[dict],
        unresolved_rows: list[dict],
        row_issues: list[dict],
    ) -> dict:
        recognized_total = None
        if sum_check.get("declared_total") is not None or sum_check.get("matched_total_label"):
            recognized_total = {
                "amount": sum_check.get("declared_total"),
                "label": sum_check.get("matched_total_label"),
                "difference": sum_check.get("difference"),
                "total_mode": sum_check.get("total_mode"),
            }

        return {
            "required": bool(status in {"fail", "unknown"} or unresolved_rows or row_issues),
            "recognized_total": recognized_total,
            "calculated_total": sum_check.get("calculated_total"),
            "difference": sum_check.get("difference"),
            "total_candidates": total_candidates,
            "unclear_content_count": len(unresolved_rows),
            "unclear_contents": self._build_manual_review_unclear_contents(unresolved_rows),
            "row_issue_count": len(row_issues),
            "row_issues": self._serialize_entries(row_issues),
        }

    def _build_manual_review_unclear_contents(self, unresolved_rows: list[dict]) -> list[dict]:
        unclear_contents = []
        for row in unresolved_rows:
            content = (
                row.get("amount_cell")
                or row.get("quantity_cell")
                or row.get("unit_price_cell")
                or row.get("line_total_cell")
                or row.get("text")
                or row.get("label")
            )
            if content:
                unclear_contents.append(str(content))
        return unclear_contents

    # 生成下浮率模式下的摘要结论。
    def _build_downward_rate_summary(self, missing_item_status: str) -> str:
        if missing_item_status == "fail":
            return "检测到下浮率模式，并发现疑似删减项。"
        if missing_item_status == "pass":
            return "检测到下浮率模式，已完成招标列项与投标列项比对，暂未发现删减项。"
        return "检测到下浮率模式，但当前缺少足够参考信息，无法完成删减项比对。"

    # 为抽取结果附带区段、页码、序号等上下文信息。
    def _build_entry_context(
        self,
        section_context: dict | None,
        *,
        serial: str | None = None,
        line_index: int | None = None,
    ) -> dict:
        context = {}
        normalized_serial = str(serial or "").strip()
        if normalized_serial:
            context["serial"] = normalized_serial
        if line_index is not None:
            context["line_index"] = int(line_index)
        if not isinstance(section_context, dict):
            return context

        section_id = section_context.get("section_id")
        if section_id:
            context["section_id"] = str(section_id)

        anchor = section_context.get("anchor")
        if anchor:
            context["section_anchor"] = anchor

        pages = section_context.get("pages")
        if isinstance(pages, list):
            normalized_pages = [page for page in pages if isinstance(page, int)]
            if normalized_pages:
                context["section_pages"] = normalized_pages
        return context

    # 构造仅由上下文决定的唯一键。
    def _entry_context_key(self, entry: dict) -> tuple | None:
        serial = str(entry.get("serial") or "").strip()
        section_id = str(entry.get("section_id") or "").strip()
        section_anchor = self._normalize_label_key(entry.get("section_anchor"))
        section_pages = tuple(page for page in (entry.get("section_pages") or []) if isinstance(page, int))
        if serial:
            return ("serial", serial, section_id, section_anchor, section_pages)

        line_index = entry.get("line_index")
        if line_index is not None:
            return ("line", section_id, section_anchor, section_pages, int(line_index))

        if section_id or section_anchor or section_pages:
            return ("section", section_id, section_anchor, section_pages)
        return None

    # 构造用于抽取结果去重的完整键。
    def _entry_dedupe_key(self, entry: dict) -> tuple:
        amount = entry.get("amount")
        return (
            self._normalize_label_key(entry.get("label")),
            self._format_decimal(amount) if isinstance(amount, Decimal) else amount,
            entry.get("source"),
            bool(entry.get("is_total")),
            bool(entry.get("is_subtotal")),
            bool(entry.get("is_preferential_total")),
            self._entry_context_key(entry),
        )

    # 构造用于识别疑似重项的比对键。
    def _entry_duplicate_key(self, entry: dict) -> tuple:
        amount = entry.get("amount")
        return (
            self._normalize_label_key(entry.get("label")),
            self._format_decimal(amount) if isinstance(amount, Decimal) else amount,
            self._entry_context_key(entry),
        )

    # 按标签、金额、来源和上下文对抽取结果去重。
    def _dedupe_entries(self, entries: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for entry in entries:
            key = self._entry_dedupe_key(entry)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(dict(entry))
        return deduped

    # 按标签和数值组合去重逐项算术疑点。
    def _dedupe_row_issues(self, issues: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for issue in issues:
            key = (
                self._normalize_label_key(issue.get("label")),
                issue.get("quantity"),
                issue.get("unit_price"),
                issue.get("line_total"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(issue)
        return deduped

    # 按序号和标签去重未完整识别的分项行。
    def _dedupe_unresolved_rows(self, rows: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for row in rows:
            key = (row.get("serial"), self._normalize_label_key(row.get("label")))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    # 将 Decimal 金额转为字符串，便于接口输出。
    def _serialize_entries(self, entries: list[dict]) -> list[dict]:
        serialized = []
        for entry in entries:
            normalized_entry = dict(entry)
            for key, value in list(normalized_entry.items()):
                if isinstance(value, Decimal):
                    normalized_entry[key] = self._format_decimal(value)
            serialized.append(normalized_entry)
        return serialized

    # 将标签归一化为适合比较和去重的键。
    def _normalize_label_key(self, label: str | None) -> str:
        normalized = re.sub(r"\s+", "", str(label or ""))
        return normalized.strip("：: /")

    # 从原始文本中移除金额和固定前缀，保留可读标签。
    def _clean_label(self, line: str) -> str:
        label = re.sub(r"(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*元?", "", line)
        label = label.replace("小写：", "").replace("小写:", "")
        label = label.replace("金额：", "").replace("金额:", "")
        label = label.replace("报价：", "").replace("报价:", "")
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label.strip()

    # 安全地把字符串金额转换为 Decimal。
    def _to_decimal(self, value: str | Decimal | None) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value).replace(",", "").replace("￥", "").replace("¥", "").strip())
        except (InvalidOperation, ValueError):
            return None

    def _to_quantity_decimal(self, value: str | Decimal | None) -> Decimal | None:
        quantity = self._to_decimal(value)
        if quantity is not None:
            return quantity
        if value is None:
            return None

        normalized = str(value).replace(",", "").strip()
        if not normalized:
            return None

        unit_pattern = "|".join(
            sorted((re.escape(unit) for unit in self.UNIT_KEYWORDS if unit), key=len, reverse=True)
        )
        if not unit_pattern:
            return None

        unit_chunk = rf"(?:(?:{unit_pattern})+|[A-Za-z]+(?:\d+)?)"
        match = re.fullmatch(
            rf"(?P<number>[+-]?(?:\d+(?:\.\d+)?))\s*(?P<unit>{unit_chunk}(?:\s*(?:/|每)?\s*{unit_chunk})*)",
            normalized,
            re.IGNORECASE,
        )
        if not match:
            return None
        return self._to_decimal(match.group("number"))

    # 把 Decimal 规范化为保留两位小数的字符串。
    def _format_decimal(self, value: Decimal | None) -> str | None:
        if value is None:
            return None
        normalized = value.quantize(Decimal("0.01"))
        return format(normalized, "f")

    # 底层兼容辅助
    # Canonical helper implementations used by both clean Chinese OCR output
    # and legacy mojibake-style payloads.
    # 识别随机备品备件等应停止主表抽取的标记文本。
    def _is_spare_parts_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return (
            compact.startswith("随机备品备件")
            or ("备件名称" in compact and "规格型号" in compact)
            or compact.startswith("闅忔満澶囧搧澶囦欢")
            or ("澶囦欢鍚嶇О" in compact and "瑙勬牸鍨嬪彿" in compact)
        )

    # 识别 layout 中可能由公司印章或页脚带来的噪声文本。
    def _is_layout_seal_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return bool(compact) and ("公司" in compact or "有限" in compact or "鍏徃" in compact or "鏈夐檺" in compact) and bool(re.search(r"\d{6,}", compact))

    # 从行首或行尾提取报价序号，并排除技术参数数字误判。
    def _extract_row_serial(self, line: str) -> str | None:
        leading_match = re.match(r"^\s*(\d+(?:\.\d+)*)(?:\s+|[\.、．])", line)
        if leading_match:
            serial = leading_match.group(1)
            remain = line[leading_match.end() :].strip()
            frequency_probe = f"{serial}{remain}"
            if not re.match(r"^\d+(?:\.\d+)?\s*(?:GHz|Ghz|MHz|kHz|Hz|mm|cm|kg|g|dB)\b", frequency_probe, re.IGNORECASE):
                return serial

        trailing_match = re.search(r"(?:^|\s)(\d+(?:\.\d+)*)(?:[\.、．])\s*$", line)
        if trailing_match and re.search(r"[\u4e00-\u9fff]", line):
            return trailing_match.group(1)
        return None

    # 识别中英文/乱码混杂场景下的表头行。
    def _is_table_header_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return (
            ("序号" in compact and "单价" in compact and "合计" in compact)
            or ("序号" in compact and ("名称" in compact or "项目名称" in compact or "服务内容" in compact or "人员类型" in compact))
            or ("规格型号" in compact and "单位" in compact and "数量" in compact)
            or ("搴忓彿" in compact and "鍗曚环" in compact and "鎬讳环" in compact)
            or ("搴忓彿" in compact and ("鍚嶇О" in compact or "椤圭洰鍚嶇О" in compact or "鏈嶅姟鍐呭" in compact or "浜哄憳绫诲瀷" in compact))
            or ("瑙勬牸鍨嬪彿" in compact and "鍗曚綅" in compact and "鏁伴噺" in compact)
        )


# ---------------------------------------------------------------------------
# 本地调试与命令行入口
# ---------------------------------------------------------------------------
# 模拟服务层把多余空白压缩后的输入形态。
def _service_style_preprocess(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()) if text else ""


# 从纯文本或 OCR JSON 载荷中提取可供分析的正文文本。
def _extract_text_from_payload(payload: object) -> str:
    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        container = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        layout_sections = container.get("layout_sections")
        if isinstance(layout_sections, list):
            lines = []
            for section in layout_sections:
                if not isinstance(section, dict):
                    continue
                text = section.get("raw_text") or section.get("text")
                if isinstance(text, str) and text.strip():
                    lines.append(text.strip())
            if lines:
                return "\n".join(lines)

        recognition = container.get("recognition")
        if isinstance(recognition, dict):
            for key in ("content", "raw_text", "text", "full_text"):
                value = recognition.get(key)
                if isinstance(value, str) and value.strip():
                    return value

        for key in ("content", "raw_text", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return value

        for key in ("content", "raw_text", "text", "full_text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value

    return str(payload or "")


# 为本地调试读取文本或 JSON 文件，并自动解析 JSON。
def _load_input_for_local_test(file_path: Path) -> object:
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"读取文件失败: {exc}") from exc

    if file_path.suffix.lower() != ".json":
        return text

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


# 收集疑似因 OCR 拆行而缺失金额的分项文本，便于本地排查。
def _collect_missing_amount_lines(checker: ItemizedPricingChecker, payload: object) -> list[str]:
    document = checker._prepare_document(payload)
    item_sections = document["item_sections"]
    candidate_sections = item_sections or document["total_sections"]

    missing_lines = []
    for section in candidate_sections:
        _, _, _, unresolved_rows = checker._extract_section_entries(section["lines"])
        for row in unresolved_rows:
            label = row.get("text") or row.get("label")
            if label:
                missing_lines.append(label)

    deduped = []
    seen = set()
    for line in missing_lines:
        key = re.sub(r"\s+", " ", line).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


# 打印本地调试报告，便于离线验证抽取和校验效果。
def _print_local_test_report(
    path: Path,
    checker: ItemizedPricingChecker,
    *,
    simulate_service: bool,
    tender_path: Path | None = None,
) -> int:
    analysis_input = _load_input_for_local_test(path)
    analysis_text = (
        _service_style_preprocess(_extract_text_from_payload(analysis_input))
        if simulate_service
        else analysis_input
    )
    tender_text = None
    if tender_path is not None:
        loaded_tender_input = _load_input_for_local_test(tender_path)
        tender_text = (
            _service_style_preprocess(_extract_text_from_payload(loaded_tender_input))
            if simulate_service
            else loaded_tender_input
        )
    result = checker.check_itemized_logic(analysis_text, tender_text=tender_text)
    display_text = _extract_text_from_payload(analysis_text)

    print(f"\n=== {path} ===")
    if tender_path is not None:
        print(f"reference_tender: {tender_path}")
    print(f"text_length: {len(display_text)}")
    print(f"mode: {result.get('mode')}")
    print(f"status: {result.get('status')}")
    print(f"passed: {result.get('passed')}")
    print(f"summary: {result.get('summary')}")

    details = result.get("details") or []
    if details:
        print("details:")
        for detail in details:
            print(f"  - {detail}")

    checks = result.get("checks") or {}
    sum_check = checks.get("sum_consistency") or {}
    print("checks:")
    print(
        "  - sum_consistency: "
        f"{sum_check.get('status')} "
        f"(calc={sum_check.get('calculated_total')}, declared={sum_check.get('declared_total')}, diff={sum_check.get('difference')})"
    )
    print(
        "  - row_arithmetic: "
        f"{(checks.get('row_arithmetic') or {}).get('status')} "
        f"(issues={(checks.get('row_arithmetic') or {}).get('issue_count')})"
    )
    print(
        "  - duplicate_items: "
        f"{(checks.get('duplicate_items') or {}).get('status')} "
        f"(issues={(checks.get('duplicate_items') or {}).get('issue_count')})"
    )
    print(
        "  - missing_item: "
        f"{(checks.get('missing_item') or {}).get('status')} "
        f"(items={(checks.get('missing_item') or {}).get('missing_items')})"
    )

    evidence = result.get("evidence") or {}
    extracted_items = evidence.get("extracted_items") or []
    total_candidates = evidence.get("total_candidates") or []
    print(f"evidence: items={len(extracted_items)}, totals={len(total_candidates)}")
    for entry in extracted_items:
        print(f"  - item: {entry.get('label')} => {entry.get('amount')} ({entry.get('source')})")
    for entry in total_candidates:
        print(f"  - total: {entry.get('label')} => {entry.get('amount')} ({entry.get('source')})")

    missing_amount_lines = _collect_missing_amount_lines(checker, analysis_text)
    if missing_amount_lines:
        print("missing_amount_candidates:")
        for line in missing_amount_lines:
            print(f"  - {line}")

    return 0


# 提供命令行入口，支持单文件或招投标配对测试。
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="本地测试分项报价检查器。")
    parser.add_argument(
        "paths",
        nargs="*",
        default=["tender.json", "bid.json"],
        help="待测试的文本或 OCR JSON 文件路径。",
    )
    parser.add_argument(
        "--simulate-service",
        action="store_true",
        help="模拟 analysis_service 中压缩空白后的输入效果。",
    )
    parser.add_argument(
        "--bid",
        help="按业务模式指定待检查的投标文件路径。",
    )
    parser.add_argument(
        "--tender",
        help="在下浮率模式下指定对照用的招标文件路径。",
    )
    args = parser.parse_args(argv)

    checker = ItemizedPricingChecker()
    exit_code = 0
    if args.bid:
        bid_path = Path(args.bid).expanduser()
        if not bid_path.is_absolute():
            bid_path = Path.cwd() / bid_path
        tender_path = None
        if args.tender:
            tender_path = Path(args.tender).expanduser()
            if not tender_path.is_absolute():
                tender_path = Path.cwd() / tender_path
        try:
            _print_local_test_report(
                bid_path,
                checker,
                simulate_service=args.simulate_service,
                tender_path=tender_path,
            )
        except Exception as exc:  # pragma: no cover - local debug entrypoint
            print(f"\n=== {bid_path} ===")
            print(f"error: {exc}")
            return 1
        return 0

    for raw_path in args.paths:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            print(f"\n=== {path} ===")
            print("error: 文件不存在。")
            exit_code = 1
            continue
        try:
            _print_local_test_report(path, checker, simulate_service=args.simulate_service)
        except Exception as exc:  # pragma: no cover - local debug entrypoint
            print(f"\n=== {path} ===")
            print(f"error: {exc}")
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
