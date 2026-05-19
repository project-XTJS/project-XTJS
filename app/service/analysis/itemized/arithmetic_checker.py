# itemized/arithmetic_checker.py
"""
分项报价 - 算术校验 Mixin

负责逐项（数量×单价=总价）的算术一致性检查、
金额提取、标签清洗及行块重建。
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any


class ArithmeticCheckerMixin:
    def _is_total_only_section(self, section_context: dict | None) -> bool:
        """判断当前区段是否属于总价页，避免把开标一览表误当成分项表。"""
        if not isinstance(section_context, dict):
            return False
        anchor = re.sub(r"\s+", "", str(section_context.get("anchor") or ""))
        if not anchor:
            return False
        return any(
            re.sub(r"\s+", "", str(keyword or "")) in anchor
            for keyword in self.TOTAL_SECTION_ANCHORS
        )
    # 逐项算术校验
    def _extract_row_arithmetic(self, line: str) -> dict | None:
        """从一行文本中提取数量、单价、行总价，用于逐项算术校验。"""
        money_pattern = r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?"
        unit_pattern = "|".join(
            re.escape(unit) for unit in sorted(self.UNIT_KEYWORDS, key=len, reverse=True)
        )
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

    # 行标签提取
    def _extract_row_label(self, line: str, index: int) -> str:
        """从单行文本中清理出分项名称标签。"""
        unit_pattern = "|".join(
            re.escape(unit)
            for unit in sorted(self.UNIT_KEYWORDS, key=len, reverse=True)
        )
        money_pattern = r"(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?"
        quantity_pattern = r"\d+(?:\.\d+)?"
        label = re.sub(r"^\s*\d+(?:\.\d+)*\s*[\.、．）]?\s*", "", line)
        label = re.sub(r"\s*\d+(?:\.\d+)*(?:[\.、．）])\s*$", "", label)
        label = re.sub(
            rf"\s+{money_pattern}\s+{quantity_pattern}(?:\s*(?:{unit_pattern}))?\s+{money_pattern}\s*$",
            "",
            label,
        )
        label = re.sub(
            rf"\s+{quantity_pattern}(?:\s*(?:{unit_pattern}))?\s+{money_pattern}\s+{money_pattern}\s*$",
            "",
            label,
        )
        label = re.sub(
            rf"\s+{quantity_pattern}(?:\s*(?:{unit_pattern}))?\s+{money_pattern}\s*$",
            "",
            label,
        )
        label = re.sub(r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label)
        label = re.sub(
            r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$",
            "",
            label,
        )
        label = re.sub(r"\s+", " ", label).strip()
        return label[:60] if label else f"第{index + 1}行"

    def _extract_block_label(self, block: dict) -> str:
        """从一个行块的首行提取分项名称。"""
        first_line = (block.get("lines") or [""])[0]
        start_index = int(block.get("start_index", 0))
        return self._extract_row_label(first_line, start_index)

    # 行块重建（把连续多行文本重新拼装成按报价项分组的表格行块）
    def _build_table_row_blocks(self, lines: list[str]) -> list[dict]:
        """把连续多行文本重新拼装成按报价项分组的表格行块。"""
        blocks = []
        current_block = None

        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if self._should_skip_line(line):
                continue
            if compact.startswith("随机备品备件") or (
                "备件名称" in compact and "规格型号" in compact
            ):
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
                inherited_serial = str(
                    (current_block or {}).get("serial") or ""
                ).strip()
                if inherited_serial and not self._extract_row_serial(line):
                    inherited_line = f"{inherited_serial} {line}".strip()
                current_block = self._flush_table_row_block(blocks, current_block)
                current_block = {
                    "start_index": idx,
                    "serial": inherited_serial
                    or self._extract_row_serial(inherited_line),
                    "lines": [inherited_line],
                }
                continue
            if current_block is not None and self._is_row_continuation_line(line):
                current_block["lines"].append(line)
                continue
            current_block = self._flush_table_row_block(blocks, current_block)

        self._flush_table_row_block(blocks, current_block)
        return blocks

    def _flush_table_row_block(
        self, blocks: list[dict], block: dict | None
    ) -> None:
        """将当前构建中的行块写入结果列表。"""
        if block and block.get("lines"):
            blocks.append(block)
        return None

    def _should_split_amount_continuation(
        self, current_block: dict | None, line: str
    ) -> bool:
        """判断续行是否应拆成新分项，处理 OCR 把金额拆到下一行的情况。"""
        if current_block is None:
            return False
        if not self._is_row_continuation_line(line):
            return False
        if not self._extract_row_amounts(line):
            return False
        return bool(current_block.get("serial"))

    def _is_row_start_line(self, line: str) -> bool:
        """判断一行是否像新的分项起始行。"""
        compact = re.sub(r"\s+", "", line)
        if not re.search(r"[\u4e00-\u9fff]", compact):
            return False
        if (
            self._looks_like_total_line(compact)
            or self._is_heading_line(line)
            or self._is_table_header_line(line)
        ):
            return False
        if self._extract_row_serial(line):
            return True
        if re.match(r"^\s*\d", line):
            return False
        return bool(
            self._looks_like_item_row(line)
            and self._extract_money_candidates(line)
        )

    def _is_row_continuation_line(self, line: str) -> bool:
        """判断一行是否像当前分项的续行。"""
        compact = re.sub(r"\s+", "", line)
        if not compact:
            return False
        if (
            self._should_skip_line(line)
            or self._looks_like_total_line(line)
            or self._is_heading_line(line)
        ):
            return False
        if self._is_table_header_line(line) or self._is_row_start_line(line):
            return False
        return bool(
            re.search(r"[\u4e00-\u9fff]", compact)
            or self._extract_money_candidates(line)
        )

    def _is_unresolved_item_block(self, text: str) -> bool:
        """判断行块是否像分项，但因缺少金额而只能标记为未完整识别。"""
        compact = re.sub(r"\s+", "", text)
        if not compact or "免费" in compact:
            return False
        if not self._extract_row_serial(text):
            return False
        return bool(
            re.search(
                r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡|设备|系统|服务|子系统)",
                compact,
            )
        )

    # 显式金额声明行提取
    def _extract_explicit_amount_entries(
        self,
        lines: list[str],
        *,
        section_context: dict | None = None,
        force_total: bool = False,
    ) -> list[dict]:
        """提取“小写金额/报价金额”这类显式金额声明行。"""
        entries = []
        multiline_amount_indexes = set()
        if force_total:
            for idx in range(len(lines) - 1):
                current_line = lines[idx]
                next_line = lines[idx + 1]
                if self._should_skip_line(current_line) or self._is_table_header_line(
                    current_line
                ):
                    continue
                if self._extract_money_candidates(current_line):
                    continue
                if (
                    not self._looks_like_total_line(current_line)
                    and "报价" not in current_line
                ):
                    continue
                if self._should_skip_line(next_line) or self._is_table_header_line(
                    next_line
                ):
                    continue
                amounts = self._extract_money_candidates(next_line)
                if len(amounts) != 1:
                    continue

                label = (
                    str((section_context or {}).get("anchor") or "").strip()
                    or self._clean_label(current_line)
                    or "总价"
                )
                entries.append(
                    {
                        "label": label,
                        "amount": amounts[0],
                        "source": "explicit_amount",
                        "is_total": True,
                        **self._build_entry_context(
                            section_context,
                            serial=self._extract_row_serial(next_line),
                            line_index=idx + 1,
                        ),
                    }
                )
                multiline_amount_indexes.add(idx + 1)

        for idx, line in enumerate(lines):
            if idx in multiline_amount_indexes:
                continue
            if (
                "小写" not in line
                and "金额" not in line
                and "报价" not in line
            ):
                continue

            amounts = self._extract_money_candidates(line)
            if len(amounts) != 1:
                continue

            if force_total:
                label = (
                    self._clean_label(line)
                    or str((section_context or {}).get("anchor") or "").strip()
                    or "总价"
                )
            elif self._looks_like_total_line(line) and (
                "合计" in line or "总计" in line
            ):
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
                    "is_total": force_total
                    or self._looks_like_total_line(label)
                    or self._looks_like_total_line(line),
                    **self._build_entry_context(
                        section_context,
                        serial=self._extract_row_serial(line),
                        line_index=idx,
                    ),
                }
            )
        return entries

    def _resolve_neighbor_label(
        self, lines: list[str], index: int
    ) -> str | None:
        """为显式金额行回溯或前瞻寻找对应的业务标签。"""
        current_label = self._clean_label(lines[index])
        if (
            current_label
            and "小写" not in current_label
            and "大写" not in current_label
        ):
            return current_label

        for offset in (1, -1, 2, -2):
            cursor = index + offset
            if cursor < 0 or cursor >= len(lines):
                continue
            candidate = lines[cursor]
            if self._should_skip_line(candidate):
                continue
            if (
                "大写" in candidate
                or "投标人名称" in candidate
                or "日期" in candidate
            ):
                continue
            if not re.search(r"[\u4e00-\u9fff]", candidate):
                continue
            cleaned = self._clean_label(candidate)
            if cleaned:
                return cleaned
        return None

    # 逐区段分项条目抽取（普通模式下使用）
    def _extract_section_entries(
        self,
        lines: list[str],
        *,
        section_context: dict | None = None,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """
        从一个候选区段中抽取分项、总价、算术疑点和未完整识别的行。
        返回: (items, totals, row_issues, unresolved_rows)
        """
        total_only_section = self._is_total_only_section(section_context)
        explicit_entries = self._extract_explicit_amount_entries(
            lines,
            section_context=section_context,
            force_total=total_only_section,
        )
        table_items = []
        table_totals = []
        row_issues = []
        unresolved_rows = []
        row_blocks = self._build_table_row_blocks(lines)
        parent_serials = self._collect_parent_serials(row_blocks)

        # 寻找显式的合计/小计行
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
                            "label": self._clean_label(line)
                            or ("小计" if is_subtotal else "合计"),
                            "amount": amounts[-1],
                            "source": (
                                "table_subtotal" if is_subtotal else "table_total"
                            ),
                            "is_subtotal": is_subtotal,
                            **self._build_entry_context(section_context, line_index=idx),
                        }
                    )
                break

        for block in row_blocks:
            if total_only_section:
                continue
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
                            **self._build_entry_context(
                                section_context,
                                serial=block.get("serial"),
                                line_index=block.get("start_index"),
                            ),
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

            expected_total = (
                arithmetic_info["quantity"] * arithmetic_info["unit_price"]
            )
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
                        **self._build_entry_context(
                            section_context,
                            serial=block.get("serial"),
                            line_index=block.get("start_index"),
                        ),
                    }
                )

        items = [entry for entry in explicit_entries if not entry["is_total"]]
        totals = [entry for entry in explicit_entries if entry["is_total"]]
        items.extend(table_items)
        totals.extend(table_totals)
        return items, totals, row_issues, unresolved_rows

    def _collect_parent_serials(self, row_blocks: list[dict]) -> set[str]:
        """收集层级序号中的父级编号，用于忽略只有标题作用的父行。"""
        parent_serials: set[str] = set()
        for block in row_blocks:
            serial = str(block.get("serial") or "").strip()
            if "." not in serial:
                continue
            parts = serial.split(".")
            for index in range(1, len(parts)):
                parent_serials.add(".".join(parts[:index]))
        return parent_serials

    def _empty_section_analysis(self) -> dict:
        """返回空的区段分析结果。"""
        return {
            "items": [],
            "totals": [],
            "row_issues": [],
            "unresolved_rows": [],
        }

    def _collect_section_analysis(self, sections: list[dict]) -> dict:
        """汇总多个区段的抽取结果。"""
        aggregated = self._empty_section_analysis()
        for section in sections or []:
            (
                section_items,
                section_totals,
                section_row_issues,
                section_unresolved_rows,
            ) = self._extract_section_entries(
                section["lines"], section_context=section
            )
            aggregated["items"].extend(section_items)
            aggregated["totals"].extend(section_totals)
            aggregated["row_issues"].extend(section_row_issues)
            aggregated["unresolved_rows"].extend(section_unresolved_rows)
        return aggregated

    def _collect_section_totals(self, sections: list[dict]) -> list[dict]:
        """仅收集区段中的总价条目。"""
        return self._collect_section_analysis(sections)["totals"]

    # 汇总校验结果构建
    def _build_sum_check_result(
        self,
        *,
        status: str,
        calculated_total: Decimal | None,
        declared_total: Decimal | None = None,
        difference: Decimal | None = None,
        matched_total_label: str | None = None,
        total_mode: str = "standard",
        subtotal_total: Decimal | None = None,
        subtotal_label: str | None = None,
        subtotal_difference: Decimal | None = None,
        subtotal_status: str | None = None,
        opening_total: Decimal | None = None,
        opening_total_label: str | None = None,
        opening_total_difference: Decimal | None = None,
        opening_total_status: str | None = None,
    ) -> dict:
        """构建汇总校验结果字典。"""
        return {
            "status": status,
            "calculated_total": self._format_decimal(calculated_total),
            "declared_total": self._format_decimal(declared_total),
            "difference": self._format_decimal(difference),
            "matched_total_label": matched_total_label,
            "total_mode": total_mode,
            "subtotal_total": self._format_decimal(subtotal_total),
            "subtotal_label": subtotal_label,
            "subtotal_difference": self._format_decimal(subtotal_difference),
            "subtotal_status": subtotal_status,
            "opening_total": self._format_decimal(opening_total),
            "opening_total_label": opening_total_label,
            "opening_total_difference": self._format_decimal(opening_total_difference),
            "opening_total_status": opening_total_status,
        }
