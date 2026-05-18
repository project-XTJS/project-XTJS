# itemized/structured_extractor.py
"""
分项报价 - 结构化表格抽取 Mixin

负责从逻辑表格中识别报价列，提取分项条目、总价、算术关系，
并处理组总价重复展示的情况。
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any


class StructuredExtractorMixin:

    # 需要由使用该 Mixin 的类提供的常量
    STRUCTURED_COLUMN_ALIASES: dict
    MONEY_TOLERANCE: Decimal

    # 结构化条目抽取主入口
    def _extract_structured_itemized_entries(
        self,
        document: dict | None,
        *,
        item_sections: list[dict] | None = None,
    ) -> dict:
        """优先从结构化 logical table 中抽取报价行关系。"""
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
            column_map = self._resolve_structured_price_columns_for_table(
                table, headers=headers
            )
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

    def _collect_itemized_logical_table_refs(
        self, sections: list[dict] | None
    ) -> set[str]:
        """收集已在区段中引用的逻辑表格引用集合。"""
        refs = set()
        for section in sections or []:
            for ref in section.get("logical_table_refs") or []:
                if ref:
                    refs.add(str(ref))
        return refs

    # 结构化列映射与推断
    def _resolve_structured_price_columns(self, headers: list[str]) -> dict | None:
        """根据表头识别结构化报价表中的关键列（数量、单价、总价等）。"""
        normalized_headers = [self._normalize_label_key(header) for header in headers]
        column_map = {}
        for field, aliases in self.STRUCTURED_COLUMN_ALIASES.items():
            alias_candidates = [self._normalize_label_key(alias) for alias in aliases]
            matched_index = self._match_structured_column_index(
                normalized_headers, alias_candidates
            )
            if matched_index is not None:
                column_map[field] = matched_index

        required_fields = {"quantity", "unit_price", "line_total"}
        if not required_fields.issubset(column_map):
            return None
        if not any(field in column_map for field in ("serial", "model", "description")):
            return None
        return column_map

    def _resolve_structured_price_columns_for_table(
        self, table: dict, *, headers: list[str] | None = None
    ) -> dict | None:
        """为单张逻辑表格解析列映射，含算术模式与仅金额模式回退。"""
        headers = headers or self._get_logical_table_headers(table)
        standard_map = self._resolve_structured_price_columns(headers)
        if standard_map is not None:
            standard_map = dict(standard_map)
            standard_map["mode"] = "arithmetic"
            standard_map["data_start_index"] = self._structured_table_data_start_index(table)
            return standard_map
        return self._infer_amount_only_column_map(table, headers=headers)

    def _structured_table_data_start_index(self, table: dict) -> int:
        """确定结构化表格中数据行起始行号。"""
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

    def _infer_amount_only_column_map(
        self, table: dict, *, headers: list[str]
    ) -> dict | None:
        """当表格无明确数量、单价列时，尝试推断仅含总价的列映射。"""
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
            headers=headers,
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

    def _infer_structured_serial_column(
        self, rows: list[list[object]], column_count: int
    ) -> int | None:
        """推断序号列位置。"""
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
        headers: list[str] | None = None,
    ) -> int | None:
        """推断总价列位置。"""
        best_index = None
        best_score = -1
        total_aliases = {
            self._normalize_label_key(alias)
            for alias in self.STRUCTURED_COLUMN_ALIASES.get("line_total", ())
        }
        text_aliases = {
            self._normalize_label_key(alias)
            for field in ("brand", "model", "description")
            for alias in self.STRUCTURED_COLUMN_ALIASES.get(field, ())
        }
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
            header = headers[index] if headers and index < len(headers) else ""
            normalized_header = self._normalize_label_key(header)
            score = amount_hits * 10 - text_hits + index
            if normalized_header and any(
                alias in normalized_header or normalized_header in alias
                for alias in total_aliases
            ):
                score += 12
            if normalized_header and any(
                alias in normalized_header or normalized_header in alias
                for alias in text_aliases
            ):
                score -= 12
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
        """推断标签列（名称/型号/描述）的位置。"""
        label_columns = []
        header_hints = (
            "名称", "项目", "功能", "内容", "描述", "说明",
            "参数", "配置", "规格", "型号", "品牌", "厂家",
        )
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

    def _match_structured_column_index(
        self, headers: list[str], aliases: list[str]
    ) -> int | None:
        """在规范化表头列表中查找最贴近目标语义的列。"""
        for index, header in enumerate(headers):
            if not header:
                continue
            if any(alias == header or alias in header or header in alias for alias in aliases):
                return index
        return None

    # 单张表格逐行分析
    def _analyze_structured_itemized_table(
        self, table: dict, *, table_index: int, column_map: dict
    ) -> dict:
        """对单张 logical table 做结构化行关系抽取。"""
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

    def _normalize_structured_row_cells(
        self, row: list[object], target_len: int
    ) -> list[str]:
        """统一结构化表格行长度，避免缺列时后续索引越界。"""
        normalized = [str(cell).strip() for cell in row[:target_len]]
        if len(normalized) < target_len:
            normalized.extend([""] * (target_len - len(normalized)))
        return normalized

    def _is_structured_header_like_row(
        self, cells: list[str], headers: list[str]
    ) -> bool:
        """识别被 OCR 切到数据区中的表头残片，避免误作报价行。"""
        nonempty_cells = [cell for cell in cells if cell]
        if not nonempty_cells:
            return False
        normalized_headers = {
            self._normalize_label_key(header) for header in headers if header
        }
        header_hits = [
            cell
            for cell in nonempty_cells
            if self._normalize_label_key(cell) in normalized_headers
        ]
        if not header_hits:
            return False
        if len(header_hits) == len(nonempty_cells):
            return True
        return len(header_hits) >= 2 and not any(
            self._extract_money_candidates(cell) for cell in nonempty_cells
        )

    def _extract_structured_total_entry(
        self,
        cells: list[str],
        *,
        section_context: dict,
        row_index: int,
        title: str | None,
        column_map: dict,
    ) -> dict | None:
        """从结构化表格中提取小计/合计等汇总行。"""
        row_text = " ".join(cell for cell in cells if cell)
        if not row_text or not self._looks_like_total_line(row_text):
            return None

        amount_candidates = []
        total_index = column_map.get("line_total")
        if total_index is not None and total_index < len(cells):
            total_text = cells[total_index]
            amount_candidates = self._extract_row_amounts(total_text)
            if not amount_candidates:
                cleaned_total_text = re.sub(
                    r"[（(][^）)]*(?:税|税率)[^）)]*[）)]", "", total_text
                )
                amount_candidates = self._extract_row_amounts(cleaned_total_text)
        if not amount_candidates:
            amount_candidates = self._extract_row_amounts(row_text)
        if not amount_candidates:
            cleaned_row_text = re.sub(
                r"[（(][^）)]*(?:税|税率)[^）)]*[）)]", "", row_text
            )
            amount_candidates = self._extract_row_amounts(cleaned_row_text)
        if not amount_candidates:
            return None

        label_source = next(
            (cell for cell in cells if self._looks_like_total_line(cell)), row_text
        )
        label = self._clean_label(label_source) or (
            "小计" if self._looks_like_subtotal_line(row_text) else "合计"
        )
        if title and label in {"小计", "合计", "总计"}:
            label = f"{title} {label}"
        return {
            "label": label,
            "amount": amount_candidates[-1],
            "source": (
                "structured_subtotal"
                if self._looks_like_subtotal_line(row_text)
                else "structured_total"
            ),
            "is_subtotal": self._looks_like_subtotal_line(row_text),
            **self._build_entry_context(section_context, line_index=row_index),
        }

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
        """把结构化表格中的一行抽成数量-单价-总价关系。"""
        row_text = " ".join(cell for cell in cells if cell)
        serial = self._structured_cell_value(cells, column_map.get("serial"))
        model = self._structured_cell_value(cells, column_map.get("model"))
        description = self._structured_cell_value(cells, column_map.get("description"))
        brand = self._structured_cell_value(cells, column_map.get("brand"))
        quantity_cell = self._structured_cell_value(cells, column_map.get("quantity"))
        unit_price_cell = self._structured_cell_value(cells, column_map.get("unit_price"))
        line_total_cell = self._structured_cell_value(cells, column_map.get("line_total"))
        quantity = self._to_quantity_decimal(quantity_cell)
        unit_price = self._to_decimal(unit_price_cell)
        line_total = self._to_decimal(line_total_cell)
        zero_amount = self._extract_structured_zero_amount(
            row_text=row_text,
            amount_cell=line_total_cell,
            unit_price_cell=unit_price_cell,
            quantity_cell=quantity_cell,
        )
        if zero_amount is not None:
            if quantity is None:
                quantity = Decimal("0")
            if unit_price is None:
                unit_price = Decimal("0")
            if line_total is None:
                line_total = zero_amount

        if quantity is None or unit_price is None or line_total is None:
            repaired = self._repair_shifted_structured_row_relation(
                cells,
                serial=serial,
                model=model,
                description=description,
                brand=brand,
                quantity_cell=quantity_cell,
                quantity=quantity,
                unit_price=unit_price,
                line_total=line_total,
                carry=carry,
            )
            if repaired is not None:
                serial = repaired["serial"]
                model = repaired["model"]
                description = repaired["description"]
                brand = repaired["brand"]
                quantity = repaired["quantity"]
                unit_price = repaired["unit_price"]
                line_total = repaired["line_total"]

        has_pricing_signal = quantity is not None or unit_price is not None or line_total is not None
        if has_pricing_signal and not serial:
            serial = carry.get("serial")
        if has_pricing_signal and not model:
            model = carry.get("model")
        if has_pricing_signal and not brand:
            brand = carry.get("brand")

        # 说明性零价行即使数量写成文本，也不应阻断分项汇总校验。
        if (
            quantity is None
            and unit_price == Decimal("0")
            and line_total == Decimal("0")
        ):
            quantity = Decimal("0")

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
                    "quantity_cell": quantity_cell,
                    "unit_price_cell": unit_price_cell,
                    "line_total_cell": line_total_cell,
                    "reason": "pricing_fields_incomplete",
                    "reason_text": "该行存在报价字段痕迹，但数量、单价、总价至少有一项未能完整识别。",
                    **self._build_entry_context(
                        section_context, serial=serial, line_index=row_index
                    ),
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
            **self._build_entry_context(
                section_context, serial=serial, line_index=row_index
            ),
        }

    def _structured_cell_value(self, cells: list[str], index: int | None) -> str:
        """读取指定列位的文本值。"""
        if index is None or index < 0 or index >= len(cells):
            return ""
        return str(cells[index]).strip()

    def _build_structured_row_label(
        self,
        *,
        serial: str | None,
        model: str | None,
        description: str | None,
        title: str | None,
    ) -> str:
        """组合结构化行的人类可读标签。"""
        parts = [part for part in (model, description) if part]
        label = " / ".join(parts)
        if serial and label:
            return f"{serial}:{label}"[:120]
        if label:
            return label[:120]
        if serial and title:
            return f"{serial}:{title}"[:120]
        return (title or serial or "结构化分项")[:120]

    def _extract_structured_amount_only_item(
        self,
        cells: list[str],
        *,
        section_context: dict,
        row_index: int,
        column_map: dict,
        title: str | None,
    ) -> dict | None:
        """提取仅含总价的分项条目（无数量、单价）。"""
        row_text = " ".join(cell for cell in cells if cell)
        serial = self._structured_cell_value(cells, column_map.get("serial"))
        label = self._build_structured_amount_only_label(
            cells=cells,
            label_columns=column_map.get("label_columns") or [],
            serial=serial,
            title=title,
        )
        amount_cell = self._structured_cell_value(cells, column_map.get("line_total"))
        amount = self._resolve_structured_amount_only_amount(
            cells,
            preferred_index=column_map.get("line_total"),
            serial_index=column_map.get("serial"),
        )
        if amount is not None:
            return {
                "label": label,
                "amount": amount,
                "source": "structured_amount_only_row",
                "declared_line_total": amount,
                "relation_type": "amount_only_row",
                **self._build_entry_context(
                    section_context, serial=serial, line_index=row_index
                ),
            }
        zero_amount = self._extract_structured_zero_amount(
            row_text=row_text,
            amount_cell=amount_cell,
        )
        if zero_amount is not None:
            return {
                "label": label,
                "amount": zero_amount,
                "source": "structured_amount_only_row",
                "declared_line_total": zero_amount,
                "relation_type": "amount_only_row",
                **self._build_entry_context(
                    section_context, serial=serial, line_index=row_index
                ),
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
                **self._build_entry_context(
                    section_context, serial=serial, line_index=row_index
                ),
            }
        return None

    def _extract_structured_zero_amount(
        self,
        *,
        row_text: str,
        amount_cell: str | None = None,
        unit_price_cell: str | None = None,
        quantity_cell: str | None = None,
    ) -> Decimal | None:
        """识别结构化表中的免费/包含/包干项，按零金额处理。"""
        combined_text = " ".join(
            str(value).strip()
            for value in (row_text, amount_cell, unit_price_cell, quantity_cell)
            if str(value or "").strip()
        )
        if not self._contains_zero_amount_hint(combined_text):
            return None
        if any(
            self._extract_money_candidates(str(value))
            for value in (amount_cell, unit_price_cell)
            if str(value or "").strip()
        ):
            return None

        normalized_text = re.sub(r"\s+", "", combined_text)
        amount_placeholder = self._is_placeholder_amount_text(amount_cell)
        unit_price_placeholder = self._is_placeholder_amount_text(unit_price_cell)

        strong_zero_keywords = ("免费", "赠送", "无偿", "不收费")
        if any(keyword in normalized_text for keyword in strong_zero_keywords):
            return Decimal("0")
        if "包含" in normalized_text and (amount_placeholder or unit_price_placeholder):
            return Decimal("0")
        return None

    def _resolve_structured_amount_only_amount(
        self,
        cells: list[str],
        *,
        preferred_index: int | None,
        serial_index: int | None,
    ) -> Decimal | None:
        """鍦ㄤ粎鍚噾棰濇ā寮忎笅锛屼紭鍏堜娇鐢ㄦ帹鏂噾棰濆垪锛屽け璐ユ椂鍥為€€鍒拌鍐呭彸渚ч噾棰濄€?"""
        preferred_cell = self._structured_cell_value(cells, preferred_index)
        preferred_amounts = self._extract_row_amounts(preferred_cell) if preferred_cell else []
        if preferred_amounts:
            return preferred_amounts[-1]

        fallback_candidates: list[tuple[int, Decimal]] = []
        for index, cell in enumerate(cells):
            if index == serial_index:
                continue
            cell_text = str(cell).strip()
            if not cell_text or self._is_placeholder_amount_text(cell_text):
                continue
            amounts = self._extract_row_amounts(cell_text)
            if not amounts:
                continue
            fallback_candidates.append((index, amounts[-1]))

        if not fallback_candidates:
            return None
        fallback_candidates.sort(key=lambda item: item[0], reverse=True)
        return fallback_candidates[0][1]

    def _repair_shifted_structured_row_relation(
        self,
        cells: list[str],
        *,
        serial: str | None,
        model: str | None,
        description: str | None,
        brand: str | None,
        quantity_cell: str | None,
        quantity: Decimal | None,
        unit_price: Decimal | None,
        line_total: Decimal | None,
        carry: dict,
    ) -> dict | None:
        """淇宸﹀彸閿欎綅鐨勫垎椤规姤浠疯锛屽悜鍙虫悳绱㈤噾棰濆苟鍥炲～鏂囨湰鍒椼€?"""
        money_cells = self._collect_structured_money_cells(cells)
        if not money_cells:
            return None

        quantity_value = (
            quantity
            if (
                self._extract_structured_repair_quantity_value(quantity_cell) is not None
                and self._is_reasonable_repair_quantity(quantity)
            )
            else None
        )
        quantity_index = None
        if quantity_value is None:
            quantity_index, quantity_value = self._find_structured_repair_quantity(
                cells, before_index=money_cells[0][0]
            )
        if quantity_value is None:
            return None

        if len(money_cells) >= 2:
            repaired_unit_price = money_cells[0][1]
            repaired_line_total = money_cells[-1][1]
        elif quantity_value == Decimal("1"):
            repaired_unit_price = money_cells[0][1]
            repaired_line_total = money_cells[0][1]
        else:
            return None

        leading_texts = self._collect_structured_leading_texts(
            cells,
            before_index=quantity_index if quantity_index is not None else money_cells[0][0],
        )
        trusted_model = self._structured_repair_trusted_text(model)
        trusted_description = self._structured_repair_trusted_text(description)
        trusted_brand = self._structured_repair_trusted_text(brand)

        serial_is_reliable = self._looks_like_structured_serial_value(serial)
        repaired_serial = serial if serial_is_reliable else carry.get("serial")
        repaired_model = (
            trusted_model
            if serial_is_reliable and trusted_model
            else carry.get("model") or trusted_model
        )
        repaired_description = trusted_description
        repaired_brand = trusted_brand

        if leading_texts:
            primary_text = leading_texts[0]
            if repaired_model:
                if not repaired_description and primary_text != repaired_model:
                    repaired_description = primary_text
            else:
                repaired_model = primary_text
            if not repaired_brand and len(leading_texts) >= 2:
                brand_candidate = leading_texts[-1]
                if brand_candidate not in {repaired_model, repaired_description}:
                    repaired_brand = brand_candidate
            if not repaired_description:
                for text in leading_texts:
                    if text not in {repaired_model, repaired_brand}:
                        repaired_description = text
                        break

        if not repaired_model:
            repaired_model = carry.get("model")
        if not repaired_brand:
            repaired_brand = carry.get("brand")

        return {
            "serial": repaired_serial,
            "model": repaired_model,
            "description": repaired_description,
            "brand": repaired_brand,
            "quantity": quantity_value,
            "unit_price": repaired_unit_price,
            "line_total": repaired_line_total,
        }

    def _collect_structured_money_cells(
        self, cells: list[str]
    ) -> list[tuple[int, Decimal]]:
        """鏀堕泦缁撴瀯鍖栬涓殑閲戦鍊欓€夈€?"""
        matches: list[tuple[int, Decimal]] = []
        for index, cell in enumerate(cells):
            cell_text = str(cell).strip()
            if not cell_text or self._is_placeholder_amount_text(cell_text):
                continue
            amounts = self._extract_row_amounts(cell_text)
            if not amounts:
                continue
            matches.append((index, amounts[-1]))
        return matches

    def _find_structured_repair_quantity(
        self, cells: list[str], *, before_index: int
    ) -> tuple[int | None, Decimal | None]:
        """浠庨噾棰濆墠鐨勫崟鍏冩牸涓弽鍚戞壘鍒版渶鍍忔暟閲忕殑鍊笺€?"""
        search_limit = min(len(cells), max(before_index, 0))
        for index in range(search_limit - 1, -1, -1):
            quantity = self._extract_structured_repair_quantity_value(cells[index])
            if quantity is not None:
                return index, quantity
        return None, None

    def _extract_structured_repair_quantity_value(
        self, value: str | None
    ) -> Decimal | None:
        """鍙妸鈥滅函鏁板瓧/鏁板瓧+鍗曚綅鈥濈殑鍗曞厓鏍艰涓烘暟閲忥紝閬垮厤璇妸 5P 绛夋枃鏈綋鎴愭暟閲忋€?"""
        normalized = str(value or "").replace(",", "").strip()
        if not normalized:
            return None
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", normalized):
            return self._to_decimal(normalized)

        unit_pattern = "|".join(
            sorted(
                (re.escape(unit) for unit in self.UNIT_KEYWORDS if unit),
                key=len,
                reverse=True,
            )
        )
        if not unit_pattern:
            return None
        if not re.fullmatch(
            rf"[+-]?\d+(?:\.\d+)?\s*(?:{unit_pattern})(?:\s*(?:/|每)\s*(?:{unit_pattern}))?",
            normalized,
            re.IGNORECASE,
        ):
            return None
        return self._to_quantity_decimal(normalized)

    def _is_reasonable_repair_quantity(self, value: Decimal | None) -> bool:
        """鍒ゆ柇鏁伴噺鍊兼槸鍚﹁惤鍦ㄥ悎鐞嗚寖鍥村唴锛岄伩鍏嶆妸閲戦褰撴垚鏁伴噺銆?"""
        if value is None:
            return False
        return Decimal("0") <= value <= Decimal("10000")

    def _collect_structured_leading_texts(
        self, cells: list[str], *, before_index: int
    ) -> list[str]:
        """鏀堕泦鏁伴噺鍓嶇殑鍙俊鏂囨湰鍒椼€?"""
        texts = []
        for cell in cells[: max(before_index, 0)]:
            text = self._structured_repair_trusted_text(cell)
            if text and text not in texts:
                texts.append(text)
        return texts

    def _structured_repair_trusted_text(self, value: str | None) -> str | None:
        """杩囨护鎺夐噾棰濄€佹暟閲忓拰鍗犱綅绗﹀悗锛屼繚鐣欏彲鐢ㄤ簬鍥炲～鐨勬枃鏈€?"""
        text = str(value or "").strip()
        if not text or self._is_placeholder_amount_text(text):
            return None
        if self._extract_row_amounts(text):
            return None
        if self._extract_structured_repair_quantity_value(text) is not None:
            return None
        return text

    def _looks_like_structured_serial_value(self, value: str | None) -> bool:
        """鍒ゆ柇鏄惁涓虹函搴忓彿鍗曞厓鏍笺€?"""
        text = str(value or "").strip()
        return bool(text) and bool(re.fullmatch(r"\d+(?:\.\d+)*", text))

    def _build_structured_amount_only_label(
        self,
        *,
        cells: list[str],
        label_columns: list[int],
        serial: str | None,
        title: str | None,
    ) -> str:
        """构建仅含金额的分项标签。"""
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

    # 行关系汇总与组总价重复检测
    def _summarize_structured_relations(self, relations: list[dict]) -> dict:
        """按组识别“行内重复展示组总价”的情况，并生成有效汇总金额。"""
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
                expected_group_total = sum(
                    (item["expected_total"] for item in group), Decimal("0")
                )
                group_difference = expected_group_total - repeated_group_total
                representative = group[0]
                group_check = {
                    "group_key": list(representative["group_key"]),
                    "label": representative["label"],
                    "serial": representative.get("serial"),
                    "model": representative.get("model"),
                    "row_count": len(group),
                    "status": (
                        "pass"
                        if abs(group_difference) <= self.MONEY_TOLERANCE
                        else "fail"
                    ),
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
                            **self._build_entry_context(
                                {
                                    "section_id": representative.get("section_id"),
                                    "anchor": representative.get("section_anchor"),
                                    "pages": representative.get("section_pages"),
                                },
                                serial=representative.get("serial"),
                                line_index=representative.get("line_index"),
                            ),
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

            cover_total_indexes = (
                {0} if self._group_has_cover_total_row(group) else set()
            )
            for relation_index, relation in enumerate(group):
                difference = relation["difference"]
                normalized_relation = dict(relation)
                normalized_relation["relation_type"] = (
                    "group_cover_total_row"
                    if relation_index in cover_total_indexes
                    else "row_total"
                )
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
                if (
                    relation_index not in cover_total_indexes
                    and abs(difference) > self.MONEY_TOLERANCE
                ):
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

        return {
            "items": items,
            "row_issues": row_issues,
            "relation_rows": relation_rows,
            "group_checks": group_checks,
        }

    def _group_has_cover_total_row(self, group: list[dict]) -> bool:
        """识别“首行声明组总价，后续行为拆分子项”的场景，避免把封面总价行误判成算术错误。"""
        if len(group) <= 1:
            return False
        first_relation = group[0]
        first_declared_total = first_relation.get("line_total")
        first_expected_total = first_relation.get("expected_total")
        if first_declared_total is None or first_expected_total is None:
            return False
        if first_declared_total <= first_expected_total + self.MONEY_TOLERANCE:
            return False
        expected_group_total = sum(
            (item.get("expected_total") or Decimal("0")) for item in group
        )
        return abs(first_declared_total - expected_group_total) <= self.MONEY_TOLERANCE

    def _detect_repeated_group_total(self, group: list[dict]) -> Decimal | None:
        """检测同一组内每行都重复展示同一个组总价的模式。"""
        if len(group) < 2:
            return None
        line_totals = [
            relation.get("line_total")
            for relation in group
            if relation.get("line_total") is not None
        ]
        if len(line_totals) != len(group):
            return None
        unique_totals = {total for total in line_totals}
        if len(unique_totals) != 1:
            return None

        repeated_total = next(iter(unique_totals))
        max_expected_total = max(
            (relation["expected_total"] for relation in group), default=Decimal("0")
        )
        if repeated_total <= max_expected_total + self.MONEY_TOLERANCE:
            return None
        return repeated_total
