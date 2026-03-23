"""
分项报价明细检查模块
负责人：江宇
"""
from __future__ import annotations

import re
from collections import Counter
from decimal import Decimal, InvalidOperation


class ItemizedPricingChecker:
    ITEM_SECTION_ANCHORS = (
        "分项报价表",
        "供应清单",
        "货物清单",
        "工程量清单",
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
    MONEY_TOLERANCE = Decimal("0.10")

    def check_itemized_logic(self, text: str) -> dict:
        normalized_text = self._normalize_text(text)
        lines = self._split_lines(normalized_text)

        item_sections = self._find_sections(lines, self.ITEM_SECTION_ANCHORS)
        total_sections = self._find_sections(lines, self.TOTAL_SECTION_ANCHORS)
        candidate_sections = item_sections + total_sections
        if not candidate_sections:
            candidate_sections = [{"anchor": "全文", "lines": lines}]

        if self._detect_downward_rate_mode(candidate_sections):
            return self._check_downward_rate_mode(candidate_sections)
        return self._check_normal_mode(item_sections, total_sections, candidate_sections)

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

    def _split_lines(self, text: str) -> list[str]:
        return [line.strip() for line in text.split("\n") if line and line.strip()]

    def _find_sections(self, lines: list[str], anchors: tuple[str, ...], window: int = 80) -> list[dict]:
        sections = []
        for idx, line in enumerate(lines):
            matched_anchor = next((anchor for anchor in anchors if anchor in line), None)
            if not matched_anchor:
                continue

            end = min(len(lines), idx + window)
            for cursor in range(idx + 5, end):
                if self._is_heading_line(lines[cursor]) and not any(anchor in lines[cursor] for anchor in anchors):
                    end = cursor
                    break

            section_lines = lines[idx:end]
            score = self._score_section(section_lines, matched_anchor)
            if score <= 0:
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
            deduped.append({"anchor": section["anchor"], "lines": section["lines"]})
        return deduped

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

    def _is_heading_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return bool(
            re.match(r"^(第[一二三四五六七八九十百]+章|[一二三四五六七八九十]+、|\d+\.[\d\.]*|（[一二三四五六七八九十]+）)", compact)
        )

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

    def _check_normal_mode(
        self,
        item_sections: list[dict],
        total_sections: list[dict],
        candidate_sections: list[dict],
    ) -> dict:
        item_source_sections = item_sections or total_sections or candidate_sections
        extracted_items = []
        extracted_totals = []
        row_issues = []

        for section in item_source_sections:
            section_items, section_totals, section_row_issues = self._extract_section_entries(section["lines"])
            extracted_items.extend(section_items)
            extracted_totals.extend(section_totals)
            row_issues.extend(section_row_issues)

        if not extracted_items and total_sections:
            for section in total_sections:
                section_items, section_totals, section_row_issues = self._extract_section_entries(section["lines"])
                extracted_items.extend(section_items)
                extracted_totals.extend(section_totals)
                row_issues.extend(section_row_issues)

        if not extracted_totals:
            for section in total_sections or candidate_sections:
                _, section_totals, _ = self._extract_section_entries(section["lines"])
                extracted_totals.extend(section_totals)

        extracted_items = self._dedupe_entries(extracted_items)
        extracted_totals = self._dedupe_entries(extracted_totals)
        row_issues = self._dedupe_row_issues(row_issues)
        duplicate_items = self._extract_duplicate_items(extracted_items)
        serial_gap_hints = self._extract_serial_gap_hints(item_sections) if item_sections else []

        table_detected = bool(item_sections or total_sections or extracted_items or extracted_totals)
        sum_check = self._evaluate_sum_check(extracted_items, extracted_totals)
        status = self._resolve_normal_status(table_detected, sum_check["status"], row_issues, duplicate_items)
        passed = self._status_to_passed(status)

        details = []
        if extracted_items:
            details.append(f"识别到 {len(extracted_items)} 个分项金额。")
        if extracted_totals:
            details.append(f"识别到 {len(extracted_totals)} 个合计/总价候选值。")
        if sum_check["status"] == "pass":
            details.append("分项金额汇总与声明总价一致。")
        elif sum_check["status"] == "fail":
            details.append(
                f"分项金额汇总与声明总价不一致：计算值 {sum_check['calculated_total']}，声明值 {sum_check['declared_total']}。"
            )
        elif sum_check["status"] == "unknown":
            details.append("已识别到报价内容，但暂时无法可靠完成汇总校验。")
        else:
            details.append("未识别到足够的分项金额或总价信息。")

        if row_issues:
            details.append(f"发现 {len(row_issues)} 条逐项算术疑点。")
        if duplicate_items:
            details.append(f"发现 {len(duplicate_items)} 组疑似重项。")
        if serial_gap_hints:
            details.append(f"发现疑似序号缺口：{', '.join(serial_gap_hints)}。")

        return {
            "itemized_table_detected": table_detected,
            "mode": "normal",
            "status": status,
            "passed": passed,
            "summary": self._build_normal_summary(status, sum_check["status"], row_issues, duplicate_items),
            "checks": {
                "row_arithmetic": {
                    "status": "fail" if row_issues else ("not_detected" if not table_detected else "pass"),
                    "issue_count": len(row_issues),
                    "issues": row_issues,
                },
                "sum_consistency": {
                    "status": sum_check["status"],
                    "calculated_total": sum_check["calculated_total"],
                    "declared_total": sum_check["declared_total"],
                    "difference": sum_check["difference"],
                    "matched_total_label": sum_check["matched_total_label"],
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
                },
            },
            "evidence": {
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": self._serialize_entries(extracted_totals),
            },
            "details": details,
        }

    def _check_downward_rate_mode(self, candidate_sections: list[dict]) -> dict:
        relevant_sections = [
            section for section in candidate_sections if any(keyword in "\n".join(section["lines"]) for keyword in self.RATE_KEYWORDS)
        ]
        if not relevant_sections:
            relevant_sections = candidate_sections

        serials = []
        extracted_items = []
        for section in relevant_sections:
            serials.extend(self._extract_serials(section["lines"]))
            extracted_items.extend(self._extract_rate_items(section["lines"]))

        extracted_items = self._dedupe_entries(extracted_items)
        missing_serials = self._find_missing_serials(serials)
        missing_item_status = "fail" if missing_serials else "unknown"
        status = "fail" if missing_serials else "unknown"

        details = [
            "检测到下浮率模式，按业务规则跳过下浮率数值本身的校验。",
        ]
        if missing_serials:
            details.append(f"发现疑似删减项，序号存在缺口：{', '.join(missing_serials)}。")
        else:
            details.append("当前仅根据单份文本做序号连续性检查，暂未发现明显缺口。")

        return {
            "itemized_table_detected": bool(relevant_sections or extracted_items),
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
                    "missing_items": missing_serials,
                    "comparison_basis": "serial_continuity",
                    "hints": [],
                },
            },
            "evidence": {
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": [],
            },
            "details": details,
        }

    def _extract_section_entries(self, lines: list[str]) -> tuple[list[dict], list[dict], list[dict]]:
        explicit_entries = self._extract_explicit_amount_entries(lines)
        table_items = []
        table_totals = []
        row_issues = []

        for idx, line in enumerate(lines):
            if self._should_skip_line(line):
                continue

            if self._looks_like_total_line(line):
                amounts = self._extract_money_candidates(line)
                if amounts:
                    table_totals.append(
                        {
                            "label": self._clean_label(line) or "合计",
                            "amount": amounts[-1],
                            "source": "table_total",
                        }
                    )
                continue

            if not self._looks_like_item_row(line):
                continue

            amounts = self._extract_money_candidates(line)
            if not amounts:
                continue

            table_items.append(
                {
                    "label": self._extract_row_label(line, idx),
                    "amount": amounts[-1],
                    "source": "table_row",
                }
            )

            arithmetic_info = self._extract_row_arithmetic(line)
            if arithmetic_info is None:
                continue

            expected_total = arithmetic_info["quantity"] * arithmetic_info["unit_price"]
            difference = expected_total - arithmetic_info["line_total"]
            if abs(difference) > self.MONEY_TOLERANCE:
                row_issues.append(
                    {
                        "label": self._extract_row_label(line, idx),
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
        return items, totals, row_issues

    def _extract_explicit_amount_entries(self, lines: list[str]) -> list[dict]:
        entries = []
        for idx, line in enumerate(lines):
            if "小写" not in line and "金额" not in line and "报价" not in line:
                continue

            amounts = self._extract_money_candidates(line)
            if len(amounts) != 1:
                continue

            label = self._resolve_neighbor_label(lines, idx)
            if not label:
                continue

            entries.append(
                {
                    "label": label,
                    "amount": amounts[0],
                    "source": "explicit_amount",
                    "is_total": self._looks_like_total_line(label) or self._looks_like_total_line(line),
                }
            )
        return entries

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

    def _looks_like_total_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return any(keyword in compact for keyword in self.TOTAL_KEYWORDS)

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

    def _extract_row_label(self, line: str, index: int) -> str:
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s*", "", line)
        label = re.sub(r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label)
        label = re.sub(r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$", "", label)
        label = re.sub(r"\s+", " ", label).strip()
        return label[:60] if label else f"第{index + 1}行"

    def _extract_row_arithmetic(self, line: str) -> dict | None:
        matches = list(
            re.finditer(
                r"(?P<qty>\d+(?:\.\d+)?)\s+(?P<unit>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)\s+(?P<total>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)",
                line,
            )
        )
        for match in reversed(matches):
            quantity = self._to_decimal(match.group("qty"))
            unit_price = self._to_decimal(match.group("unit"))
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

    def _extract_money_candidates(self, line: str) -> list[Decimal]:
        candidates = []
        for match in re.finditer(r"(?:￥|¥)?\s*((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)", line):
            value = self._to_decimal(match.group(1))
            if value is None:
                continue
            around = line[max(0, match.start() - 3): min(len(line), match.end() + 3)]
            if "%" in around or "％" in around:
                continue
            if not self._looks_like_money_value(value):
                continue
            if re.search(r"(年|月|日|页|GHz|MHz|kW|dB|mm|cm)", around, re.IGNORECASE) and value < Decimal("1000"):
                continue
            candidates.append(value)
        return candidates

    def _looks_like_money_value(self, value: Decimal) -> bool:
        return value >= Decimal("100")

    def _evaluate_sum_check(self, items: list[dict], totals: list[dict]) -> dict:
        if len(items) < 2 or not totals:
            calculated_total = sum((item["amount"] for item in items if item.get("amount") is not None), Decimal("0"))
            return {
                "status": "unknown" if items else "not_detected",
                "calculated_total": self._format_decimal(calculated_total) if items else None,
                "declared_total": None,
                "difference": None,
                "matched_total_label": None,
            }

        calculated_total = sum((item["amount"] for item in items if item.get("amount") is not None), Decimal("0"))
        best_total = min(
            totals,
            key=lambda item: (abs(item["amount"] - calculated_total), 0 if "总价" in item["label"] or "合计" in item["label"] else 1),
        )
        difference = calculated_total - best_total["amount"]
        status = "pass" if abs(difference) <= self.MONEY_TOLERANCE else "fail"
        return {
            "status": status,
            "calculated_total": self._format_decimal(calculated_total),
            "declared_total": self._format_decimal(best_total["amount"]),
            "difference": self._format_decimal(difference),
            "matched_total_label": best_total["label"],
        }

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
            match = re.match(r"^(\d+(?:\.\d+)?)", compact)
            if match:
                serials.append(match.group(1))
        return serials

    def _extract_serial_gap_hints(self, sections: list[dict]) -> list[str]:
        serials = []
        for section in sections:
            serials.extend(self._extract_serials(section["lines"]))
        return self._find_missing_serials(serials)

    def _extract_rate_items(self, lines: list[str]) -> list[dict]:
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
                }
            )
        return items

    def _extract_duplicate_items(self, entries: list[dict]) -> list[dict]:
        normalized_labels = []
        for entry in entries:
            label = self._normalize_label_key(entry.get("label"))
            if not label:
                continue
            normalized_labels.append(label)

        duplicates = []
        for label, count in Counter(normalized_labels).items():
            if count <= 1:
                continue
            duplicates.append({"label": label, "count": count})
        return duplicates

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

    def _resolve_normal_status(
        self,
        table_detected: bool,
        sum_status: str,
        row_issues: list[dict],
        duplicate_items: list[dict],
    ) -> str:
        if not table_detected:
            return "not_detected"
        if row_issues or sum_status == "fail" or duplicate_items:
            return "fail"
        if sum_status == "pass":
            return "pass"
        return "unknown"

    def _status_to_passed(self, status: str) -> bool | None:
        if status == "pass":
            return True
        if status == "fail":
            return False
        return None

    def _build_normal_summary(
        self,
        status: str,
        sum_status: str,
        row_issues: list[dict],
        duplicate_items: list[dict],
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
        return "已识别到报价内容，但当前证据不足以完成完整校验。"

    def _build_downward_rate_summary(self, missing_item_status: str) -> str:
        if missing_item_status == "fail":
            return "检测到下浮率模式，并发现疑似删减项。"
        return "检测到下浮率模式，已跳过下浮率数值校验，当前仅完成序号连续性检查。"

    def _dedupe_entries(self, entries: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for entry in entries:
            amount = entry.get("amount")
            key = (
                self._normalize_label_key(entry.get("label")),
                self._format_decimal(amount) if isinstance(amount, Decimal) else amount,
                entry.get("source"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(dict(entry))
        return deduped

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

    def _serialize_entries(self, entries: list[dict]) -> list[dict]:
        serialized = []
        for entry in entries:
            normalized_entry = dict(entry)
            if isinstance(normalized_entry.get("amount"), Decimal):
                normalized_entry["amount"] = self._format_decimal(normalized_entry["amount"])
            serialized.append(normalized_entry)
        return serialized

    def _normalize_label_key(self, label: str | None) -> str:
        normalized = re.sub(r"\s+", "", str(label or ""))
        return normalized.strip("：: /")

    def _clean_label(self, line: str) -> str:
        label = re.sub(r"(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*元?", "", line)
        label = label.replace("小写：", "").replace("小写:", "")
        label = label.replace("金额：", "").replace("金额:", "")
        label = label.replace("报价：", "").replace("报价:", "")
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label.strip()

    def _to_decimal(self, value: str | Decimal | None) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value).replace(",", "").replace("￥", "").replace("¥", "").strip())
        except (InvalidOperation, ValueError):
            return None

    def _format_decimal(self, value: Decimal | None) -> str | None:
        if value is None:
            return None
        normalized = value.quantize(Decimal("0.01"))
        return format(normalized, "f")
