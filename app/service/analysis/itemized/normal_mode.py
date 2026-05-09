# itemized/normal_mode.py
"""
分项报价 - 普通模式 Mixin

包含普通报价模式（非下浮率）下的汇总校验、优惠价模式检测、
结果总结与人工复核提示生成等逻辑。
"""

from __future__ import annotations

import re
from collections import Counter
from decimal import Decimal
from typing import Any


class NormalModeMixin:

    # 依赖常量与其它 Mixin 提供的方法
    MONEY_TOLERANCE: Decimal
    SUBTOTAL_KEYWORDS: tuple
    PREFERENTIAL_TOTAL_KEYWORDS: tuple
    PREFERENTIAL_TOTAL_LINE_WINDOW: int
    TOTAL_KEYWORDS: tuple
    OPENING_TOTAL_KEYWORDS: tuple

    # 普通模式主入口
    def _check_normal_mode(
        self,
        item_sections: list[dict],
        total_sections: list[dict],
        candidate_sections: list[dict],
        *,
        document: dict | None = None,
    ) -> dict:
        """执行普通报价模式下的分项检查。"""
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
            extracted_totals.extend(
                self._collect_section_totals(total_sections or candidate_sections)
            )
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
            extracted_totals.extend(
                self._extract_preferential_total_entries(document.get("lines") or [])
            )

        if not extracted_totals or all(
            entry.get("is_subtotal") for entry in extracted_totals
        ):
            extracted_totals.extend(
                self._collect_section_totals(total_sections or candidate_sections)
            )

        extracted_items = self._dedupe_entries(extracted_items)
        extracted_totals = self._dedupe_entries(extracted_totals)
        row_issues = self._dedupe_row_issues(row_issues)
        unresolved_rows = self._dedupe_unresolved_rows(unresolved_rows)
        duplicate_items = self._extract_duplicate_items(extracted_items)
        serial_gap_hints = (
            self._extract_serial_gap_hints(item_sections) if item_sections else []
        )

        table_detected = bool(
            item_sections or total_sections or extracted_items or extracted_totals
        )
        sum_check = self._evaluate_sum_check(
            extracted_items, extracted_totals, preferential_mode=preferential_mode
        )
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
            "summary": self._build_normal_summary(
                status, sum_check["status"], row_issues, duplicate_items, unresolved_rows
            ),
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
                                        for entry in (
                                            structured_analysis.get("relation_rows") or []
                                        )
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
                    "skipped_count": int(
                        structured_analysis.get("amount_only_item_count") or 0
                    ),
                    "group_check_count": len(structured_analysis["group_checks"]),
                    "group_checks": self._serialize_entries(
                        structured_analysis["group_checks"]
                    ),
                },
                "sum_consistency": {
                    "status": (
                        "unknown"
                        if unresolved_rows and sum_check["status"] == "pass"
                        else sum_check["status"]
                    ),
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
                    "opening_total_difference": sum_check.get(
                        "opening_total_difference"
                    ),
                    "opening_total_status": sum_check.get("opening_total_status"),
                },
                "duplicate_items": {
                    "status": (
                        "fail"
                        if duplicate_items
                        else ("not_detected" if not table_detected else "pass")
                    ),
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
                "analysis_basis": (
                    "structured_logical_tables"
                    if structured_analysis["used_tables"]
                    else "text_sections"
                ),
                "structured_tables": structured_analysis["used_tables"],
                "structured_relation_count": len(
                    structured_analysis["relation_rows"]
                ),
                "structured_relations": self._serialize_entries(
                    structured_analysis["relation_rows"]
                ),
                "structured_group_checks": self._serialize_entries(
                    structured_analysis["group_checks"]
                ),
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": serialized_total_candidates,
                "unresolved_rows": serialized_unresolved_rows,
            },
            "manual_review": manual_review,
            "details": details,
        }

    # 优惠价/小计模式识别
    def _detect_preferential_total_mode(self, document: dict | None) -> bool:
        """识别文档是否存在“小计 + 最终优惠价”的特殊总价模式。"""
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
                if (
                    abs(
                        preferential_entry["line_index"]
                        - subtotal_entry["line_index"]
                    )
                    > self.PREFERENTIAL_TOTAL_LINE_WINDOW
                ):
                    continue
                if (
                    preferential_entry["amount"]
                    <= subtotal_entry["amount"] + self.MONEY_TOLERANCE
                ):
                    return True
        return False

    def _extract_preferential_total_entries(
        self, lines: list[str]
    ) -> list[dict]:
        """从全文中提取“最终优惠价/优惠价”等特殊总价声明。"""
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

    def _extract_document_subtotal_entries(
        self, lines: list[str]
    ) -> list[dict]:
        """从全文中提取小计行，供优惠价模式下对账使用。"""
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

    def _looks_like_preferential_total_line(self, line: str) -> bool:
        """判断某一行是否像“最终优惠价”声明。"""
        compact = re.sub(r"\s+", "", line)
        if not compact or self._is_table_header_line(line):
            return False
        if len(self._extract_money_candidates(line)) != 1:
            return False
        if self._looks_like_item_row(line):
            return False

        strong_keywords = ("最终优惠价", "优惠价")
        weak_keywords = tuple(
            keyword
            for keyword in self.PREFERENTIAL_TOTAL_KEYWORDS
            if keyword not in strong_keywords
        )
        has_strong_keyword = any(keyword in compact for keyword in strong_keywords)
        has_weak_keyword = any(keyword in compact for keyword in weak_keywords)
        if not (has_strong_keyword or has_weak_keyword):
            return False
        if has_strong_keyword:
            return True
        return any(keyword in compact for keyword in self.TOTAL_KEYWORDS)

    def _looks_like_opening_total_label(self, label: str | None) -> bool:
        """判断总价标签是否更像开标一览表/总报价口径的声明总价。"""
        compact = re.sub(r"\s+", "", str(label or ""))
        if not compact:
            return False
        return any(keyword in compact for keyword in self.OPENING_TOTAL_KEYWORDS)

    def _select_opening_total_candidate(self, totals: list[dict]) -> dict | None:
        """从总价候选值中优先挑出开标一览表或投标总价口径的声明总价。"""
        preferred_candidates = [
            item
            for item in totals
            if not item.get("is_subtotal")
            and self._looks_like_opening_total_label(item.get("label"))
        ]
        if preferred_candidates:
            return min(
                preferred_candidates,
                key=lambda item: (
                    0 if "投标总价" in str(item.get("label") or "") else 1,
                    0
                    if item.get("source") == "explicit_amount"
                    else 1
                    if item.get("source") == "table_total"
                    else 2,
                    len(str(item.get("label") or "")),
                ),
            )

        preferential_candidates = [
            item for item in totals if item.get("is_preferential_total")
        ]
        if preferential_candidates:
            return min(
                preferential_candidates,
                key=lambda item: (
                    0 if item.get("source") == "explicit_amount" else 1,
                    len(str(item.get("label") or "")),
                ),
            )
        return None

    # 汇总校验
    def _evaluate_preferential_sum_check(
        self, calculated_total: Decimal, totals: list[dict]
    ) -> dict | None:
        """对优惠价模式进行专门的汇总校验。"""
        subtotal_candidates = [
            item for item in totals if item.get("is_subtotal")
        ]
        preferential_candidates = [
            item for item in totals if item.get("is_preferential_total")
        ]
        if not subtotal_candidates:
            return None

        best_subtotal = min(
            subtotal_candidates,
            key=lambda item: abs(item["amount"] - calculated_total),
        )
        subtotal_difference = calculated_total - best_subtotal["amount"]
        subtotal_status = (
            "pass"
            if abs(subtotal_difference) <= self.MONEY_TOLERANCE
            else "fail"
        )

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
            if best_opening_total is not None
            and abs(opening_total_difference) <= self.MONEY_TOLERANCE
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

    def _evaluate_sum_check(
        self,
        items: list[dict],
        totals: list[dict],
        *,
        preferential_mode: bool = False,
    ) -> dict:
        """计算分项汇总与声明总价的关系。"""
        calculated_total = self._sum_entry_amounts(items)
        if len(items) < 2 or not totals:
            return self._build_sum_check_result(
                status="unknown" if items else "not_detected",
                calculated_total=calculated_total if items else None,
                total_mode="preferential_total" if preferential_mode else "standard",
            )

        if preferential_mode:
            preferential_result = self._evaluate_preferential_sum_check(
                calculated_total, totals
            )
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
            status=(
                "pass" if abs(difference) <= self.MONEY_TOLERANCE else "fail"
            ),
            calculated_total=calculated_total,
            declared_total=best_total["amount"],
            difference=difference,
            matched_total_label=best_total["label"],
        )

    # 状态判定与结果描述
    def _resolve_normal_status(
        self,
        table_detected: bool,
        sum_status: str,
        row_issues: list[dict],
        duplicate_items: list[dict],
        unresolved_rows: list[dict],
    ) -> str:
        """综合汇总校验、算术疑点和 OCR 完整度，给出普通模式总状态。"""
        if not table_detected:
            return "not_detected"
        if row_issues or sum_status == "fail" or duplicate_items:
            return "fail"
        if unresolved_rows:
            return "unknown"
        if sum_status == "pass":
            return "pass"
        return "unknown"

    def _status_to_passed(self, status: str) -> bool | None:
        """把字符串状态转换为布尔 passed 标记。"""
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
        """生成普通报价模式下的摘要结论。"""
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
            return (
                "已识别到报价内容，但存在未完整识别的分项行，"
                "暂无法完成可靠校验。"
            )
        return "已识别到报价内容，但当前证据不足以完成完整校验。"

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
        """生成普通模式的详细发现列表。"""
        details = []
        if structured_analysis["used_tables"]:
            details.append(
                f"优先基于 {len(structured_analysis['used_tables'])} 张结构化表格进行分项金额与总价校验。"
            )
        if structured_analysis.get("amount_only_item_count"):
            details.append(
                f"其中 {structured_analysis['amount_only_item_count']} 条分项仅声明金额，"
                "已参与汇总校验，不执行单价乘数量校验。"
            )
        if structured_analysis["group_checks"]:
            details.append(
                f"识别到 {len(structured_analysis['group_checks'])} 组因合并单元格导致的"
                "组总价重复展示，汇总时已按分项明细去重。"
            )
        if extracted_items:
            details.append(f"识别到 {len(extracted_items)} 个分项金额。")
        if extracted_totals:
            details.append(f"识别到 {len(extracted_totals)} 个合计/总价候选值。")

        if sum_check["status"] == "pass":
            if sum_check.get("total_mode") == "preferential_total":
                if sum_check.get("opening_total") is not None:
                    details.append(
                        "检测到最终优惠价模式，分项金额汇总、分项小计与开标一览表总价一致。"
                    )
                else:
                    details.append(
                        "检测到最终优惠价模式，分项金额汇总与分项小计一致。"
                    )
                if sum_check.get("preferential_total") is not None:
                    details.append(
                        f"文档同时声明最终优惠价 {sum_check['preferential_total']}"
                        f"（{sum_check.get('preferential_total_label') or '最终优惠价'}）。"
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
                        f"分项金额汇总与分项小计一致，但与开标一览表总价不一致："
                        f"分项小计 {sum_check.get('subtotal_total')}，"
                        f"开标一览表总价 {sum_check.get('opening_total')}。"
                    )
                elif sum_check.get("subtotal_status") == "fail":
                    details.append(
                        f"检测到最终优惠价模式，但分项金额汇总与分项小计不一致："
                        f"计算值 {sum_check['calculated_total']}，"
                        f"小计 {sum_check.get('subtotal_total') or sum_check['declared_total']}。"
                    )
                    if sum_check.get("opening_total") is not None:
                        details.append(
                            f"同时，开标一览表总价为 {sum_check.get('opening_total')}"
                            f"（{sum_check.get('opening_total_label')}）。"
                        )
                else:
                    details.append(
                        f"检测到最终优惠价模式，但总价口径存在不一致："
                        f"计算值 {sum_check['calculated_total']}，"
                        f"声明值 {sum_check['declared_total']}。"
                    )
            else:
                details.append(
                    f"分项金额汇总与声明总价不一致："
                    f"计算值 {sum_check['calculated_total']}，"
                    f"声明值 {sum_check['declared_total']}。"
                )
        elif sum_check["status"] == "unknown":
            details.append("已识别到报价内容，但暂时无法可靠完成汇总校验。")
        else:
            details.append("未识别到足够的分项金额或总价信息。")

        if unresolved_rows:
            details.append(
                f"发现 {len(unresolved_rows)} 条未完整识别的分项行，"
                "当前结果可能受 OCR 拆行影响。"
            )
        if row_issues:
            details.append(f"发现 {len(row_issues)} 条逐项算术疑点。")
        if duplicate_items:
            details.append(f"发现 {len(duplicate_items)} 组疑似重项。")
        if serial_gap_hints:
            details.append(
                f"提示：检测到序号可能跳号：{', '.join(serial_gap_hints)}。"
                "该提示仅供人工复核，不影响当前金额校验结论。"
            )
        return details

    # 人工复核提示
    def _build_manual_review_payload(
        self,
        *,
        status: str,
        sum_check: dict,
        total_candidates: list[dict],
        unresolved_rows: list[dict],
        row_issues: list[dict],
    ) -> dict:
        """生成人工复核所需的提示信息。"""
        recognized_total = None
        if (
            sum_check.get("declared_total") is not None
            or sum_check.get("matched_total_label")
        ):
            recognized_total = {
                "amount": sum_check.get("declared_total"),
                "label": sum_check.get("matched_total_label"),
                "difference": sum_check.get("difference"),
                "total_mode": sum_check.get("total_mode"),
            }

        return {
            "required": bool(
                status in {"fail", "unknown"} or unresolved_rows or row_issues
            ),
            "recognized_total": recognized_total,
            "calculated_total": sum_check.get("calculated_total"),
            "difference": sum_check.get("difference"),
            "total_candidates": total_candidates,
            "unclear_content_count": len(unresolved_rows),
            "unclear_contents": self._build_manual_review_unclear_contents(
                unresolved_rows
            ),
            "row_issue_count": len(row_issues),
            "row_issues": self._serialize_entries(row_issues),
        }

    def _build_manual_review_unclear_contents(
        self, unresolved_rows: list[dict]
    ) -> list[dict]:
        """收集未完整识别行的文本内容供展示。"""
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

    # 疑似重复报价检测
    def _extract_duplicate_items(self, entries: list[dict]) -> list[dict]:
        """识别分项金额中标签、金额和上下文完全重复的疑似重项。"""
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