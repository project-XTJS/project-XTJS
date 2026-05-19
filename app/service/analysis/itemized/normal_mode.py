# itemized/normal_mode.py
"""
分项报价 - 普通模式 Mixin

包含普通报价模式（非下浮率）下的汇总校验、
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
    TOTAL_KEYWORDS: tuple
    OPENING_TOTAL_KEYWORDS: tuple
    ZERO_AMOUNT_KEYWORDS: tuple
    LOW_CONFIDENCE_UNRESOLVED_THRESHOLD: int

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
            total_sections=total_sections,
        )
        extracted_items = list(structured_analysis["items"])
        extracted_totals = list(structured_analysis["totals"])
        row_issues = list(structured_analysis["row_issues"])
        unresolved_rows = list(structured_analysis["unresolved_rows"])

        if structured_analysis["used_tables"]:
            if not total_sections:
                extracted_totals.extend(
                    self._collect_section_totals(candidate_sections)
                )
        else:
            section_analysis = self._collect_section_analysis(item_source_sections)
            extracted_items.extend(section_analysis["items"])
            extracted_totals.extend(section_analysis["totals"])
            row_issues.extend(section_analysis["row_issues"])
            unresolved_rows.extend(section_analysis["unresolved_rows"])

        if total_sections:
            extracted_totals.extend(self._collect_section_totals(total_sections))

        # 已识别到结构化分项表时，总价页只补总价候选，不再回退生成分项项。
        if not extracted_items and total_sections and not structured_analysis["used_tables"]:
            fallback_analysis = self._collect_section_analysis(total_sections)
            extracted_items.extend(fallback_analysis["items"])
            extracted_totals.extend(fallback_analysis["totals"])
            row_issues.extend(fallback_analysis["row_issues"])
            unresolved_rows.extend(fallback_analysis["unresolved_rows"])

        if not extracted_totals or all(
            entry.get("is_subtotal") for entry in extracted_totals
        ):
            extracted_totals.extend(
                self._collect_section_totals(total_sections or candidate_sections)
            )

        # 分项报价表只有 1 条分项且没有单独合计行时，将该行总价视作合计价。
        inferred_total = self._build_single_item_total_candidate(
            extracted_items, extracted_totals
        )
        if inferred_total is not None:
            extracted_totals.append(inferred_total)

        extracted_items = self._dedupe_entries(extracted_items)
        extracted_totals = self._dedupe_entries(extracted_totals)
        row_issues = self._dedupe_row_issues(row_issues)
        unresolved_rows = self._dedupe_unresolved_rows(unresolved_rows)
        extracted_totals, excluded_totals = self._filter_total_candidates_to_primary_scope(
            extracted_totals,
            item_sections=item_sections,
            total_sections=total_sections,
        )
        duplicate_items = self._extract_duplicate_items(extracted_items)
        serial_gap_hints = (
            self._extract_serial_gap_hints(item_sections) if item_sections else []
        )

        table_detected = bool(
            item_sections or structured_analysis["used_tables"] or extracted_items
        )
        sum_check = self._evaluate_sum_check(extracted_items, extracted_totals)
        confidence = self._assess_itemized_confidence(
            item_sections=item_sections,
            total_sections=total_sections,
            structured_analysis=structured_analysis,
            total_candidates=extracted_totals,
            excluded_totals=excluded_totals,
            unresolved_rows=unresolved_rows,
            row_issues=row_issues,
        )
        row_status = self._resolve_row_arithmetic_status(
            table_detected=table_detected,
            structured_analysis=structured_analysis,
            row_issues=row_issues,
            unresolved_rows=unresolved_rows,
            confidence=confidence,
        )
        effective_sum_status = self._resolve_sum_consistency_status(
            sum_check["status"],
            confidence=confidence,
        )
        status = self._resolve_normal_status(
            table_detected,
            effective_sum_status,
            row_status,
            duplicate_items,
            confidence["blocking_unresolved_rows"],
        )
        passed = self._status_to_passed(status)
        serialized_total_candidates = self._serialize_entries(extracted_totals)
        serialized_unresolved_rows = self._serialize_entries(unresolved_rows)
        serialized_excluded_totals = self._serialize_entries(excluded_totals)
        sum_check = {
            **sum_check,
            "raw_status": sum_check["status"],
            "status": effective_sum_status,
            "confidence_level": confidence["level"],
            "confidence_reasons": list(confidence["reasons"]),
        }
        details = self._build_normal_details(
            structured_analysis=structured_analysis,
            extracted_items=extracted_items,
            extracted_totals=extracted_totals,
            sum_check=sum_check,
            row_issues=row_issues,
            duplicate_items=duplicate_items,
            unresolved_rows=unresolved_rows,
            serial_gap_hints=serial_gap_hints,
            confidence=confidence,
            excluded_totals=excluded_totals,
        )
        manual_review = self._build_manual_review_payload(
            status=status,
            sum_check=sum_check,
            total_candidates=serialized_total_candidates,
            unresolved_rows=serialized_unresolved_rows,
            row_issues=row_issues,
            confidence=confidence,
        )

        return {
            "itemized_table_detected": table_detected,
            "mode": "normal",
            "status": status,
            "passed": passed,
            "summary": self._build_normal_summary(
                status,
                sum_check["status"],
                row_issues,
                duplicate_items,
                unresolved_rows,
                has_total_sections=bool(total_sections),
            ),
            "checks": {
                "row_arithmetic": {
                    "status": row_status,
                    "raw_status": (
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
                    "blocking_unresolved_count": confidence["blocking_unresolved_count"],
                    "benign_unresolved_count": confidence["benign_unresolved_count"],
                    "confidence_level": confidence["level"],
                    "confidence_reasons": list(confidence["reasons"]),
                    "skipped_count": int(
                        structured_analysis.get("amount_only_item_count") or 0
                    ),
                    "group_check_count": len(structured_analysis["group_checks"]),
                    "group_checks": self._serialize_entries(
                        structured_analysis["group_checks"]
                    ),
                },
                "sum_consistency": {
                    "status": sum_check["status"],
                    "calculated_total": sum_check["calculated_total"],
                    "declared_total": sum_check["declared_total"],
                    "difference": sum_check["difference"],
                    "matched_total_label": sum_check["matched_total_label"],
                    "raw_status": sum_check.get("raw_status"),
                    "total_mode": sum_check.get("total_mode"),
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
                    "confidence_level": confidence["level"],
                    "confidence_reasons": list(confidence["reasons"]),
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
                "excluded_total_candidates": serialized_excluded_totals,
                "unresolved_rows": serialized_unresolved_rows,
                "confidence_assessment": confidence,
            },
            "manual_review": manual_review,
            "details": details,
        }

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
        return None

    # 汇总校验
    def _evaluate_sum_check(
        self,
        items: list[dict],
        totals: list[dict],
    ) -> dict:
        """计算分项汇总与声明总价的关系。"""
        calculated_total = self._sum_entry_amounts(items)
        # 单条分项只要能拿到明确的合计/总价候选，也允许继续做总价一致性校验。
        if not items or not totals:
            return self._build_sum_check_result(
                status="unknown" if items else "not_detected",
                calculated_total=calculated_total if items else None,
            )

        opening_total = self._select_opening_total_candidate(totals)
        subtotal_candidates = [item for item in totals if item.get("is_subtotal")]
        best_subtotal = (
            min(
                subtotal_candidates,
                key=lambda item: abs(item["amount"] - calculated_total),
            )
            if subtotal_candidates
            else None
        )
        subtotal_difference = (
            calculated_total - best_subtotal["amount"]
            if best_subtotal is not None
            else None
        )
        subtotal_status = (
            "pass"
            if best_subtotal is not None
            and abs(subtotal_difference) <= self.MONEY_TOLERANCE
            else ("fail" if best_subtotal is not None else None)
        )

        if opening_total is not None:
            opening_total_difference = calculated_total - opening_total["amount"]
            opening_total_status = (
                "pass"
                if abs(opening_total_difference) <= self.MONEY_TOLERANCE
                else "fail"
            )
            return self._build_sum_check_result(
                status=opening_total_status,
                calculated_total=calculated_total,
                declared_total=opening_total["amount"],
                difference=opening_total_difference,
                matched_total_label=opening_total["label"],
                subtotal_total=(best_subtotal or {}).get("amount"),
                subtotal_label=(best_subtotal or {}).get("label"),
                subtotal_difference=subtotal_difference,
                subtotal_status=subtotal_status,
                opening_total=opening_total["amount"],
                opening_total_label=opening_total["label"],
                opening_total_difference=opening_total_difference,
                opening_total_status=opening_total_status,
            )

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
            subtotal_total=(best_subtotal or {}).get("amount"),
            subtotal_label=(best_subtotal or {}).get("label"),
            subtotal_difference=subtotal_difference,
            subtotal_status=subtotal_status,
        )

    def _build_single_item_total_candidate(
        self,
        items: list[dict],
        totals: list[dict],
    ) -> dict | None:
        """单条分项缺少合计行时，将该行总价补成隐式合计候选。"""
        if totals or len(items) != 1:
            return None

        item = items[0]
        amount = item.get("amount")
        if amount is None:
            return None

        item_label = str(item.get("label") or "").strip()
        label = "单条分项行总价（视作合计）"
        if item_label:
            label = f"{item_label} 行总价（视作合计）"

        inferred_total = {
            "label": label,
            "amount": amount,
            "source": "single_item_total_inferred",
            "is_subtotal": False,
        }
        for key in (
            "serial",
            "line_index",
            "section_id",
            "section_anchor",
            "section_pages",
        ):
            if item.get(key) is not None:
                inferred_total[key] = item.get(key)
        return inferred_total

    # 状态判定与结果描述
    def _filter_total_candidates_to_primary_scope(
        self,
        totals: list[dict],
        *,
        item_sections: list[dict],
        total_sections: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """只保留主报价表附近及开标总价口径的总价候选，隔离无关附件。"""
        if not totals:
            return [], []

        primary_pages = {
            page
            for section in item_sections or []
            for page in (section.get("pages") or [])
            if isinstance(page, int)
        }
        total_pages = {
            page
            for section in total_sections or []
            for page in (section.get("pages") or [])
            if isinstance(page, int)
        }

        kept = []
        excluded = []
        for total in totals:
            pages = {
                page for page in (total.get("section_pages") or []) if isinstance(page, int)
            }
            keep = False
            if self._looks_like_opening_total_label(total.get("label")):
                keep = True
            elif pages and self._pages_intersect_or_near(pages, primary_pages):
                keep = True
            elif pages and self._pages_intersect_or_near(pages, total_pages):
                keep = True
            elif not pages and not primary_pages:
                keep = True

            if keep:
                kept.append(total)
            else:
                excluded.append(total)

        return (kept or totals), excluded

    def _pages_intersect_or_near(
        self,
        pages: set[int],
        reference_pages: set[int],
        *,
        gap: int = 1,
    ) -> bool:
        """判断两组页码是否重叠或足够接近。"""
        if not pages or not reference_pages:
            return False
        if pages & reference_pages:
            return True
        return min(abs(page - ref) for page in pages for ref in reference_pages) <= gap

    def _assess_itemized_confidence(
        self,
        *,
        item_sections: list[dict],
        total_sections: list[dict],
        structured_analysis: dict,
        total_candidates: list[dict],
        excluded_totals: list[dict],
        unresolved_rows: list[dict],
        row_issues: list[dict],
    ) -> dict:
        """评估当前分项报价抽取是否足够稳定，可用于硬性失败判断。"""
        benign_unresolved_rows = [
            row
            for row in unresolved_rows
            if self._is_benign_unresolved_row(row)
            or self._is_parent_summary_unresolved_row(row, structured_analysis)
        ]
        blocking_unresolved_rows = [
            row for row in unresolved_rows if row not in benign_unresolved_rows
        ]
        distinct_totals = {
            self._format_decimal(item.get("amount"))
            for item in total_candidates
            if item.get("amount") is not None
        }
        excluded_distinct_totals = {
            self._format_decimal(item.get("amount"))
            for item in excluded_totals
            if item.get("amount") is not None
        }
        column_shift_suspected = any(
            self._looks_like_column_shift_unresolved_row(row)
            for row in blocking_unresolved_rows
        )

        reasons = []
        if excluded_distinct_totals:
            reasons.append("检测到主报价表范围外的总价候选，已按主报价表隔离。")
        if len(item_sections or []) > 1:
            reasons.append("同一文件内存在多处分项报价表锚点。")
        if len(structured_analysis.get("used_tables") or []) > 1:
            reasons.append("当前主报价表涉及多张结构化表，需谨慎判定。")
        threshold = int(getattr(self, "LOW_CONFIDENCE_UNRESOLVED_THRESHOLD", 3) or 3)
        if len(blocking_unresolved_rows) >= threshold:
            reasons.append("未解析分项行较多，当前结构化结果可信度不足。")
        if column_shift_suspected:
            reasons.append("疑似存在金额列错位或厂家列误绑金额列。")
        if row_issues and blocking_unresolved_rows:
            reasons.append("在存在较多未解析行时又出现算术异常，优先按低置信度处理。")
        if len(distinct_totals) >= 2 and excluded_distinct_totals:
            reasons.append("总价候选存在多个口径，已隔离无关候选。")

        return {
            "level": "low" if reasons else "high",
            "reasons": reasons,
            "benign_unresolved_count": len(benign_unresolved_rows),
            "blocking_unresolved_count": len(blocking_unresolved_rows),
            "blocking_unresolved_rows": blocking_unresolved_rows,
            "column_shift_suspected": column_shift_suspected,
            "distinct_total_count": len(distinct_totals),
            "excluded_total_count": len(excluded_totals),
            "total_section_count": len(total_sections or []),
        }

    def _resolve_row_arithmetic_status(
        self,
        *,
        table_detected: bool,
        structured_analysis: dict,
        row_issues: list[dict],
        unresolved_rows: list[dict],
        confidence: dict,
    ) -> str:
        """结合低置信度规则，计算逐项算术校验状态。"""
        if row_issues:
            return "unknown" if confidence["level"] == "low" else "fail"
        if confidence["blocking_unresolved_count"]:
            return "unknown"
        if (
            structured_analysis.get("amount_only_item_count")
            and not structured_analysis["group_checks"]
            and not any(
                entry.get("relation_type") == "row_total"
                for entry in (structured_analysis.get("relation_rows") or [])
            )
        ):
            return "not_applicable"
        if not table_detected:
            return "not_detected"
        if unresolved_rows and not confidence["blocking_unresolved_count"]:
            return "pass"
        return "pass"

    def _resolve_sum_consistency_status(
        self,
        raw_status: str,
        *,
        confidence: dict,
    ) -> str:
        """结合低置信度规则，计算汇总一致性状态。"""
        if raw_status == "fail" and confidence["level"] == "low":
            return "unknown"
        if raw_status == "pass" and confidence["blocking_unresolved_count"]:
            return "unknown"
        return raw_status

    def _is_benign_unresolved_row(self, row: dict) -> bool:
        """识别免费/包含等不会影响总价的未解析分项行。"""
        values = [
            row.get("amount_cell"),
            row.get("unit_price_cell"),
            row.get("line_total_cell"),
            row.get("quantity_cell"),
            row.get("text"),
            row.get("label"),
        ]
        if not self._contains_zero_amount_hint(*values):
            return False
        return any(
            self._is_placeholder_amount_text(value)
            for value in (
                row.get("amount_cell"),
                row.get("unit_price_cell"),
                row.get("line_total_cell"),
            )
        )

    def _looks_like_column_shift_unresolved_row(self, row: dict) -> bool:
        """识别金额列被厂商名等文本误占位的低置信度场景。"""
        for value in (
            row.get("amount_cell"),
            row.get("line_total_cell"),
            row.get("unit_price_cell"),
            row.get("quantity_cell"),
        ):
            text = str(value or "").strip()
            if not text:
                continue
            if self._is_placeholder_amount_text(text):
                continue
            if self._extract_money_candidates(text):
                continue
            if self._contains_zero_amount_hint(text):
                continue
            if re.search(r"[\u4e00-\u9fffA-Za-z]", text):
                return True
        return False

    def _is_parent_summary_unresolved_row(
        self, row: dict, structured_analysis: dict
    ) -> bool:
        """识别已被子项完整展开的父项汇总行，避免其作为阻断性未解析项。"""
        serial = str(row.get("serial") or "").strip()
        if not serial or "." in serial:
            return False
        if any(
            self._extract_money_candidates(str(value or ""))
            for value in (
                row.get("amount_cell"),
                row.get("unit_price_cell"),
                row.get("line_total_cell"),
            )
        ):
            return False

        relation_rows = structured_analysis.get("relation_rows") or []
        section_id = row.get("section_id")
        child_rows = [
            entry
            for entry in relation_rows
            if str(entry.get("serial") or "").startswith(f"{serial}.")
            and (not section_id or entry.get("section_id") == section_id)
        ]
        return len(child_rows) >= 2

    def _resolve_normal_status(
        self,
        table_detected: bool,
        sum_status: str,
        row_status: str,
        duplicate_items: list[dict],
        unresolved_rows: list[dict],
    ) -> str:
        """综合汇总校验、算术疑点和 OCR 完整度，给出普通模式总状态。"""
        if not table_detected:
            return "not_detected"
        if duplicate_items or row_status == "fail" or sum_status == "fail":
            return "fail"
        if row_status == "unknown" or sum_status == "unknown" or unresolved_rows:
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
        *,
        has_total_sections: bool = False,
    ) -> str:
        """生成普通报价模式下的摘要结论。"""
        if status == "not_detected":
            if has_total_sections:
                return "未识别到分项报价表，当前仅检测到报价一览表，无法执行分项报价表一致性校验。"
            return "未识别到分项报价表，无法执行分项报价表一致性校验。"
        if status == "pass":
            return "分项报价检查通过。"
        if status == "unknown":
            return "已识别到报价内容，但当前结构化可信度不足，暂不宜直接判定分项报价存在硬性异常。"
        if row_issues and sum_status == "fail":
            return "发现逐项算术错误，且分项汇总与声明总价不一致。"
        if row_issues:
            return "发现逐项算术错误。"
        if duplicate_items:
            return "发现疑似重项。"
        if sum_status == "fail":
            return "分项汇总与声明总价不一致。"
        if False:
            details.append(
                "当前分项报价结构化可信度较低，已将原本可能的硬性异常降级为待人工复核。"
            )
            for reason in confidence.get("reasons") or []:
                details.append(f"低置信度原因：{reason}")
        if False:
            details.append(
                "当前分项报价结构化可信度较低，已将原本可能的硬性异常降级为待人工复核。"
            )
            for reason in confidence.get("reasons") or []:
                details.append(f"低置信度原因：{reason}")
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
        confidence: dict,
        excluded_totals: list[dict],
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

        if excluded_totals:
            details.append(
                f"已按主报价表范围隔离 {len(excluded_totals)} 个非主报价表总价候选。"
            )

        if sum_check["status"] == "pass":
            if sum_check.get("opening_total") is not None:
                details.append("分项金额汇总与开标一览表总价一致。")
            else:
                details.append("分项金额汇总与声明总价一致。")
        elif sum_check["status"] == "fail":
            if (
                sum_check.get("subtotal_status") == "pass"
                and sum_check.get("opening_total_status") == "fail"
                and sum_check.get("opening_total") is not None
            ):
                details.append(
                    f"分项金额汇总与分项表合计一致，但与开标一览表总价不一致："
                    f"分项汇总 {sum_check['calculated_total']}，"
                    f"开标一览表总价 {sum_check.get('opening_total')}。"
                )
            elif sum_check.get("opening_total") is not None:
                details.append(
                    f"分项金额汇总与开标一览表总价不一致："
                    f"计算值 {sum_check['calculated_total']}，"
                    f"开标一览表总价 {sum_check.get('opening_total')}。"
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
        confidence: dict,
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
                status in {"fail", "unknown"}
                or unresolved_rows
                or row_issues
                or confidence.get("level") == "low"
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
            "confidence_level": confidence.get("level"),
            "confidence_reasons": list(confidence.get("reasons") or []),
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
