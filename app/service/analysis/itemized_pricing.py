"""
分项报价明细检查模块
负责人：江宇
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path


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
    ZERO_AMOUNT_KEYWORDS = (
        "包含",
        "免费",
        "赠送",
        "无偿",
        "不收费",
    )
    MONEY_TOLERANCE = Decimal("0.10")

    def check_itemized_logic(self, text: object, tender_text: object | None = None) -> dict:
        document = self._prepare_document(text)
        item_sections = document["item_sections"]
        total_sections = document["total_sections"]
        candidate_sections = document["candidate_sections"]

        if self._detect_downward_rate_mode(candidate_sections):
            tender_document = self._prepare_document(tender_text) if tender_text is not None else None
            return self._check_downward_rate_mode(candidate_sections, tender_document=tender_document)
        return self._check_normal_mode(item_sections, total_sections, candidate_sections)

    def _prepare_document(self, payload: object) -> dict:
        parsed_payload = self._parse_payload(payload)
        source_text = _extract_text_from_payload(parsed_payload) if parsed_payload is not None else str(payload or "")
        normalized_text = self._normalize_text(source_text)
        lines = self._split_lines(normalized_text)

        structured_item_sections = self._find_layout_table_sections(parsed_payload, self.ITEM_SECTION_ANCHORS)
        structured_total_sections = self._find_layout_table_sections(parsed_payload, self.TOTAL_SECTION_ANCHORS)
        item_sections = structured_item_sections or self._find_sections(lines, self.ITEM_SECTION_ANCHORS)
        total_sections = structured_total_sections or self._find_sections(lines, self.TOTAL_SECTION_ANCHORS)
        candidate_sections = self._dedupe_sections(item_sections + total_sections)
        if not candidate_sections:
            candidate_sections = [{"anchor": "全文", "lines": lines, "source": "full_text"}]

        return {
            "payload": parsed_payload,
            "text": source_text,
            "normalized_text": normalized_text,
            "lines": lines,
            "item_sections": item_sections,
            "total_sections": total_sections,
            "candidate_sections": candidate_sections,
        }

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
            deduped.append({"anchor": section["anchor"], "lines": section["lines"]})
        return deduped

    def _find_layout_table_sections(self, payload: dict | None, anchors: tuple[str, ...]) -> list[dict]:
        layout_sections = self._get_layout_sections(payload)
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
            table_started = False
            for follower in layout_sections[idx + 1:]:
                section_type = str(follower.get("type") or "").lower()
                section_text = self._get_section_text(follower)
                if not section_text:
                    continue

                if not table_started:
                    if section_type == "table":
                        table_started = True
                        lines.extend(self._split_lines(self._normalize_text(section_text)))
                        pages.append(follower.get("page"))
                        continue
                    if self._matches_other_anchor(section_text, anchors):
                        break
                    if self._is_heading_line(section_text):
                        break
                    continue

                if section_type == "table":
                    if not self._should_attach_following_layout_table(section_text):
                        continue
                    lines.extend(self._split_lines(self._normalize_text(section_text)))
                    pages.append(follower.get("page"))
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

            sections.append(
                {
                    "anchor": matched_anchor,
                    "lines": lines,
                    "start": idx,
                    "end": idx + len(lines),
                    "score": len(lines),
                    "source": "layout_table_sequence",
                    "pages": deduped_pages,
                }
            )

        sections.sort(key=lambda item: item.get("start", 0))
        return self._dedupe_sections(sections)

    def _get_layout_sections(self, payload: dict | None) -> list[dict]:
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, dict):
            return []
        layout_sections = data.get("layout_sections")
        if not isinstance(layout_sections, list):
            return []
        return [section for section in layout_sections if isinstance(section, dict)]

    def _get_section_text(self, section: dict) -> str:
        text = section.get("raw_text") or section.get("text")
        return text.strip() if isinstance(text, str) and text.strip() else ""

    def _matches_other_anchor(self, text: str, anchors: tuple[str, ...]) -> bool:
        matched_anchor = next((anchor for anchor in anchors if anchor in text), None)
        return bool(matched_anchor and self._is_anchor_line(text, matched_anchor))

    def _is_skippable_layout_text(self, text: str) -> bool:
        lines = self._split_lines(self._normalize_text(text))
        return bool(lines) and all(self._should_skip_line(line) for line in lines)

    def _is_spare_parts_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return compact.startswith("随机备品备件") or ("备件名称" in compact and "规格型号" in compact)

    def _is_layout_bridge_text(self, text: str) -> bool:
        return (
            self._is_skippable_layout_text(text)
            or self._is_spare_parts_marker_text(text)
            or self._is_layout_page_marker_text(text)
        )

    def _is_layout_page_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        if re.fullmatch(r"第\d+页", compact):
            return True
        return compact in {"投标文件-商务部分", "投标文件-技术部分", "商务部分", "技术部分"}

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

    def _extract_row_serial(self, line: str) -> str | None:
        leading_match = re.match(r"^\s*(\d+(?:\.\d+)*)(?:\s+|[\.、．）)])", line)
        if leading_match:
            return leading_match.group(1)

        trailing_match = re.search(r"(?:^|\s)(\d+(?:\.\d+)*)(?:[\.、．）)])\s*$", line)
        if trailing_match and re.search(r"[\u4e00-\u9fff]", line):
            return trailing_match.group(1)
        return None

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

    def _is_heading_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        serial = self._extract_row_serial(line)
        if serial and (self._extract_money_candidates(line) or self._contains_quantity_unit(compact)):
            return False
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
        unresolved_rows = []

        for section in item_source_sections:
            section_items, section_totals, section_row_issues, section_unresolved_rows = self._extract_section_entries(
                section["lines"]
            )
            extracted_items.extend(section_items)
            extracted_totals.extend(section_totals)
            row_issues.extend(section_row_issues)
            unresolved_rows.extend(section_unresolved_rows)

        if not extracted_items and total_sections:
            for section in total_sections:
                section_items, section_totals, section_row_issues, section_unresolved_rows = self._extract_section_entries(
                    section["lines"]
                )
                extracted_items.extend(section_items)
                extracted_totals.extend(section_totals)
                row_issues.extend(section_row_issues)
                unresolved_rows.extend(section_unresolved_rows)

        if not extracted_totals:
            for section in total_sections or candidate_sections:
                _, section_totals, _, _ = self._extract_section_entries(section["lines"])
                extracted_totals.extend(section_totals)

        extracted_items = self._dedupe_entries(extracted_items)
        extracted_totals = self._dedupe_entries(extracted_totals)
        row_issues = self._dedupe_row_issues(row_issues)
        unresolved_rows = self._dedupe_unresolved_rows(unresolved_rows)
        duplicate_items = self._extract_duplicate_items(extracted_items)
        serial_gap_hints = self._extract_serial_gap_hints(item_sections) if item_sections else []

        table_detected = bool(item_sections or total_sections or extracted_items or extracted_totals)
        sum_check = self._evaluate_sum_check(extracted_items, extracted_totals)
        status = self._resolve_normal_status(
            table_detected,
            sum_check["status"],
            row_issues,
            duplicate_items,
            unresolved_rows,
        )
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

        if unresolved_rows:
            details.append(f"发现 {len(unresolved_rows)} 条未完整识别的分项行，当前结果可能受 OCR 拆行影响。")
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
            "summary": self._build_normal_summary(status, sum_check["status"], row_issues, duplicate_items, unresolved_rows),
            "checks": {
                "row_arithmetic": {
                    "status": (
                        "fail"
                        if row_issues
                        else ("unknown" if unresolved_rows else ("not_detected" if not table_detected else "pass"))
                    ),
                    "issue_count": len(row_issues),
                    "issues": row_issues,
                    "unresolved_count": len(unresolved_rows),
                    "unresolved_rows": unresolved_rows,
                },
                "sum_consistency": {
                    "status": "unknown" if unresolved_rows and sum_check["status"] in {"pass", "fail"} else sum_check["status"],
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
                "unresolved_rows": unresolved_rows,
            },
            "details": details,
        }

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
            extracted_items.extend(self._extract_rate_items(section["lines"]))

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
            details.append(f"投标文件内部还存在疑似序号缺口：{', '.join(serial_gap_hints)}。")

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

    def _extract_section_entries(self, lines: list[str]) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        explicit_entries = self._extract_explicit_amount_entries(lines)
        table_items = []
        table_totals = []
        row_issues = []
        unresolved_rows = []
        row_blocks = self._build_table_row_blocks(lines)

        for idx, line in enumerate(lines):
            if self._should_skip_line(line):
                continue

            if self._is_table_header_line(line):
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
                break

        for block in row_blocks:
            block_text = " ".join(block["lines"])
            amounts = self._extract_row_amounts(block_text)
            if not amounts:
                if self._is_unresolved_item_block(block_text):
                    unresolved_rows.append(
                        {
                            "serial": block.get("serial"),
                            "label": self._extract_block_label(block),
                            "text": block_text[:160],
                        }
                    )
                continue

            table_items.append(
                {
                    "label": self._extract_block_label(block),
                    "amount": amounts[-1],
                    "source": "table_row",
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
            if current_block is not None and self._is_row_continuation_line(line):
                current_block["lines"].append(line)
                continue
            current_block = self._flush_table_row_block(blocks, current_block)

        self._flush_table_row_block(blocks, current_block)
        return blocks

    def _flush_table_row_block(self, blocks: list[dict], block: dict | None) -> None:
        if block and block.get("lines"):
            blocks.append(block)
        return None

    def _is_table_header_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return (
            ("序号" in compact and "单价" in compact and "总价" in compact)
            or ("序号" in compact and ("名称" in compact or "项目名称" in compact or "服务内容" in compact or "人员类型" in compact))
            or ("规格型号" in compact and "单位" in compact and "数量" in compact)
        )

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

    def _is_row_continuation_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        if not compact:
            return False
        if self._should_skip_line(line) or self._looks_like_total_line(line) or self._is_heading_line(line):
            return False
        if self._is_table_header_line(line) or self._is_row_start_line(line):
            return False
        return bool(re.search(r"[\u4e00-\u9fff]", compact) or self._extract_money_candidates(line))

    def _extract_block_label(self, block: dict) -> str:
        first_line = (block.get("lines") or [""])[0]
        start_index = int(block.get("start_index", 0))
        return self._extract_row_label(first_line, start_index)

    def _is_unresolved_item_block(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        if not compact or "免费" in compact:
            return False
        if not self._extract_row_serial(text):
            return False
        return bool(re.search(r"(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡|设备|系统|服务|子系统)", compact))

    def _extract_explicit_amount_entries(self, lines: list[str]) -> list[dict]:
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
        if self._is_table_header_line(line):
            return False
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
                quantity = self._to_decimal(match.group("qty"))
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

    def _extract_row_amounts(self, line: str) -> list[Decimal]:
        amounts = self._extract_money_candidates(line)
        if amounts:
            return amounts
        zero_amount = self._extract_zero_amount_candidate(line)
        return [zero_amount] if zero_amount is not None else []

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
            serial = self._extract_row_serial(line)
            if serial:
                serials.append(serial)
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

    def _extract_reference_items(self, document: dict | None) -> list[dict]:
        if not document:
            return []
        item_sections = document.get("item_sections") or []
        if not item_sections:
            lines = document.get("lines") or []
            item_sections = self._find_sections(lines, self.ITEM_SECTION_ANCHORS, require_score=False)
        return self._extract_comparison_items_from_sections(item_sections, rate_mode=False)

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

    def _format_comparison_item(self, item: dict) -> str:
        serial = item.get("serial")
        label = item.get("label")
        if serial and label:
            return f"{serial}:{label}"
        return label or str(serial or "")

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
        unresolved_rows: list[dict],
    ) -> str:
        if not table_detected:
            return "not_detected"
        if row_issues or sum_status == "fail" or duplicate_items:
            if unresolved_rows and not (row_issues or duplicate_items):
                return "unknown"
            return "fail"
        if unresolved_rows:
            return "unknown"
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
        unresolved_rows: list[dict],
    ) -> str:
        if status == "not_detected":
            return "未识别到可用于校验的分项报价表或报价一览表。"
        if status == "pass":
            return "分项报价检查通过。"
        if unresolved_rows:
            return "已识别到报价内容，但存在未完整识别的分项行，暂无法完成可靠校验。"
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
        if missing_item_status == "pass":
            return "检测到下浮率模式，已完成招标列项与投标列项比对，暂未发现删减项。"
        return "检测到下浮率模式，但当前缺少足够参考信息，无法完成删减项比对。"

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


def _service_style_preprocess(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()) if text else ""


def _extract_text_from_payload(payload: object) -> str:
    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            content = data.get("content")
            if isinstance(content, str) and content.strip():
                return content

            layout_sections = data.get("layout_sections")
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

            recognition = data.get("recognition")
            if isinstance(recognition, dict):
                for key in ("content", "raw_text", "text", "full_text"):
                    value = recognition.get(key)
                    if isinstance(value, str) and value.strip():
                        return value

        for key in ("content", "raw_text", "text", "full_text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value

    return str(payload or "")


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
