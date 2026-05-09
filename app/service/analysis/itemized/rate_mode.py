# itemized/rate_mode.py
"""
分项报价 - 下浮率模式 Mixin

处理下浮率/优惠率报价模式的特殊逻辑：
模式检测、列项抽取、删减项比对、跳号提示等。
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any


class RateModeMixin:

    # 依赖常量与其它 Mixin 提供的方法
    RATE_KEYWORDS: tuple
    ITEM_SECTION_ANCHORS: tuple
    TOTAL_KEYWORDS: tuple
    UNIT_KEYWORDS: tuple

    # 下浮率模式入口
    def _check_downward_rate_mode(
        self,
        candidate_sections: list[dict],
        tender_document: dict | None = None,
    ) -> dict:
        """执行下浮率报价模式下的列项抽取和删减项比对。"""
        relevant_sections = [
            section
            for section in candidate_sections
            if any(
                keyword in "\n".join(section["lines"])
                for keyword in self.RATE_KEYWORDS
            )
        ]
        if not relevant_sections:
            relevant_sections = candidate_sections

        serials = []
        extracted_items = []
        for section in relevant_sections:
            serials.extend(self._extract_serials(section["lines"]))
            extracted_items.extend(
                self._extract_rate_items(section["lines"], section_context=section)
            )

        extracted_items = self._dedupe_entries(extracted_items)
        serial_gap_hints = self._extract_serial_gap_hints(relevant_sections)
        comparison_items = self._extract_comparison_items_from_sections(
            relevant_sections, rate_mode=True
        )
        reference_items = (
            self._extract_reference_items(tender_document)
            if tender_document
            else []
        )
        comparison_result = (
            self._compare_reference_items(reference_items, comparison_items)
            if reference_items
            else None
        )

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
            details.append(
                "当前未提供招标文件，无法完成招标列项与投标列项的删减项比对。"
            )
        elif missing_items:
            details.append(
                f"对比招标列项后发现疑似删减项：{', '.join(missing_items)}。"
            )
        else:
            details.append(
                "已对比招标文件与投标文件列项，暂未发现明显删减项。"
            )
        if serial_gap_hints:
            details.append(
                f"提示：投标文件内部检测到序号可能跳号：{', '.join(serial_gap_hints)}。"
                "该提示仅供人工复核，不直接作为删减项判定依据。"
            )

        return {
            "itemized_table_detected": bool(
                relevant_sections or extracted_items or comparison_items
            ),
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

    def _detect_downward_rate_mode(self, sections: list[dict]) -> bool:
        """判断当前文档是否属于下浮率/优惠率报价模式。"""
        for section in sections:
            section_text = "\n".join(section["lines"])
            if not any(keyword in section_text for keyword in self.RATE_KEYWORDS):
                continue
            if "%" in section_text or "％" in section_text:
                return True
            if any(
                keyword in line
                for line in section["lines"]
                for keyword in self.RATE_KEYWORDS
            ):
                return True
        return False

    # 序号提取与缺失检测
    def _extract_serials(self, lines: list[str]) -> list[str]:
        """从候选区段中抽取所有可识别的序号。"""
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
        """汇总多个区段中的序号，并给出可能的跳号提示。"""
        serials = []
        for section in sections:
            serials.extend(self._extract_serials(section["lines"]))
        return self._find_missing_serials(serials)

    def _find_missing_serials(self, serials: list[str]) -> list[str]:
        """根据整数序号和子序号推断可能缺失的编号。"""
        if not serials:
            return []

        missing = []
        int_serials = sorted(
            {int(serial) for serial in serials if serial.isdigit()}
        )
        # 如果序号跨度异常大，可能不是连续编号，放弃整数推断
        if (
            len(int_serials) >= 3
            and int_serials[-1] - int_serials[0] > len(int_serials) + 5
        ):
            int_serials = []
        for left, right in zip(int_serials, int_serials[1:]):
            if right - left <= 1:
                continue
            missing.extend(
                [str(number) for number in range(left + 1, right)]
            )

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
                missing.extend(
                    [f"{prefix}.{number}" for number in range(left + 1, right)]
                )
        return missing

    # 下浮率行项抽取
    def _extract_rate_items(
        self, lines: list[str], *, section_context: dict | None = None
    ) -> list[dict]:
        """在下浮率模式下提取可用于比对的列项标签。"""
        items = []
        for idx, line in enumerate(lines):
            if (
                not any(keyword in line for keyword in self.RATE_KEYWORDS)
                and "%" not in line
                and "％" not in line
            ):
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

    # 招标参考列项提取与比对
    def _extract_reference_items(self, document: dict | None) -> list[dict]:
        """从招标参考文档中提取标准列项集合。"""
        if not document:
            return []
        item_sections = document.get("item_sections") or []
        if not item_sections:
            lines = document.get("lines") or []
            item_sections = self._find_sections(
                lines, self.ITEM_SECTION_ANCHORS, require_score=False
            )
        return self._extract_comparison_items_from_sections(
            item_sections, rate_mode=False
        )

    def _extract_comparison_items_from_sections(
        self, sections: list[dict], *, rate_mode: bool
    ) -> list[dict]:
        """从多个区段中提取列项，并按序号与标签去重。"""
        items = []
        for section in sections:
            items.extend(
                self._extract_comparison_items(section["lines"], rate_mode=rate_mode)
            )

        deduped = []
        seen = set()
        for item in items:
            key = (item.get("serial"), item.get("label_key"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _extract_comparison_items(
        self, lines: list[str], *, rate_mode: bool
    ) -> list[dict]:
        """从单个区段中提取用于招投标比对的列项。"""
        items = []
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if not compact:
                continue
            if compact.startswith("随机备品备件") or (
                "备件名称" in compact and "规格型号" in compact
            ):
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
            has_rate = (
                any(keyword in line for keyword in self.RATE_KEYWORDS)
                or "%" in line
                or "％" in line
            )
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

    def _extract_comparison_label(
        self, line: str, index: int, *, rate_mode: bool
    ) -> str:
        """清洗比较用标签，去掉金额、单位和下浮率尾巴。"""
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s*", "", line)
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s+", "", label)
        if rate_mode:
            label = re.split(
                r"(?:下浮率|优惠率|折扣率|折让率|下浮|%|％)",
                label,
                maxsplit=1,
            )[0]
        label = re.sub(
            r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label
        )
        label = re.sub(r"\b(?:免费)\b.*$", "", label)
        label = re.sub(
            r"\s*(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$",
            "",
            label,
        )
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label[:80] if label else f"第{index + 1}行"

    def _compare_reference_items(
        self, reference_items: list[dict], bid_items: list[dict]
    ) -> dict:
        """比较招标与投标列项，输出疑似缺失项。"""
        reference_with_serial = [
            item for item in reference_items if item.get("serial")
        ]
        bid_serials = {
            item["serial"] for item in bid_items if item.get("serial")
        }
        missing_items = []
        comparison_basis = "tender_vs_bid_label"

        if reference_with_serial and bid_serials:
            comparison_basis = "tender_vs_bid_serial"
            for item in reference_with_serial:
                if item["serial"] in bid_serials:
                    continue
                missing_items.append(self._format_comparison_item(item))
        else:
            bid_label_keys = {
                item["label_key"] for item in bid_items if item.get("label_key")
            }
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
        """把列项格式化成便于展示和人工复核的字符串。"""
        serial = item.get("serial")
        label = item.get("label")
        if serial and label:
            return f"{serial}:{label}"
        return label or str(serial or "")

    # 下浮率模式摘要
    def _build_downward_rate_summary(self, missing_item_status: str) -> str:
        """生成下浮率模式下的摘要结论。"""
        if missing_item_status == "fail":
            return "检测到下浮率模式，并发现疑似删减项。"
        if missing_item_status == "pass":
            return "检测到下浮率模式，已完成招标列项与投标列项比对，暂未发现删减项。"
        return (
            "检测到下浮率模式，但当前缺少足够参考信息，无法完成删减项比对。"
        )