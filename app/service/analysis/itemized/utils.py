"""
分项报价 - 通用工具 Mixin

提供 Decimal 转换、格式化、去重、标签归一化、上下文构建等基础方法。
"""

from __future__ import annotations

import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any


class UtilsMixin:
    """通用工具 Mixin，作为 ItemizedPricingChecker 的基类之一。"""

    ZERO_AMOUNT_KEYWORDS: tuple

    # Decimal 转换与格式化
    def _to_decimal(self, value: str | Decimal | None) -> Decimal | None:
        """安全地将字符串或 Decimal 转为 Decimal。"""
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value).replace(",", "").replace("￥", "").replace("¥", "").strip())
        except (InvalidOperation, ValueError):
            return None

    def _to_quantity_decimal(self, value: str | Decimal | None) -> Decimal | None:
        """将可能包含单位的数量字符串转为 Decimal。"""
        quantity = self._to_decimal(value)
        if quantity is not None:
            return quantity
        if value is None:
            return None

        normalized = str(value).replace(",", "").strip()
        if not normalized:
            return None

        # 兼容“2面 / 3处 / 9台”这类前导数字数量写法。
        leading_match = re.match(r"^\s*([+-]?\d+(?:\.\d+)?)", normalized)
        if leading_match:
            return self._to_decimal(leading_match.group(1))

        unit_pattern = "|".join(
            sorted(
                (re.escape(unit) for unit in self.UNIT_KEYWORDS if unit),
                key=len, reverse=True,
            )
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

    def _format_decimal(self, value: Decimal | None) -> str | None:
        """把 Decimal 规范化为保留两位小数的字符串。"""
        if value is None:
            return None
        normalized = value.quantize(Decimal("0.01"))
        return format(normalized, "f")

    def _sum_entry_amounts(self, entries: list[dict]) -> Decimal:
        """汇总 entry 列表中的 amount 字段。"""
        return sum(
            (entry["amount"] for entry in entries if entry.get("amount") is not None),
            Decimal("0"),
        )

    # 文本清理与标签归一化
    def _normalize_label_key(self, label: str | None) -> str:
        """将标签归一化为适合比较和去重的键。"""
        normalized = re.sub(r"\s+", "", str(label or ""))
        return normalized.strip("：: /")

    def _clean_label(self, line: str) -> str:
        """从原始文本中移除金额和固定前缀，保留可读标签。"""
        label = re.sub(r"(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*元?", "", line)
        label = label.replace("小写：", "").replace("小写:", "")
        label = label.replace("金额：", "").replace("金额:", "")
        label = label.replace("报价：", "").replace("报价:", "")
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label.strip()

    def _contains_zero_amount_hint(self, *values: object) -> bool:
        """判断文本中是否包含“免费/包含”等零金额或包干提示。"""
        combined = "".join(
            re.sub(r"\s+", "", str(value or ""))
            for value in values
            if str(value or "").strip()
        )
        return bool(combined) and any(
            keyword in combined for keyword in self.ZERO_AMOUNT_KEYWORDS
        )

    def _is_placeholder_amount_text(self, value: object) -> bool:
        """判断单元格是否只是金额占位符，而非真实金额。"""
        normalized = re.sub(r"\s+", "", str(value or ""))
        if not normalized:
            return True
        return normalized in {
            "/",
            "\\",
            "-",
            "--",
            "—",
            "／",
            "N/A",
            "n/a",
            "NA",
            "na",
            "免费",
            "包含",
            "赠送",
            "无偿",
            "不收费",
            "0",
            "0.0",
            "0.00",
            "¥0",
            "￥0",
            "¥0.0",
            "￥0.0",
            "¥0.00",
            "￥0.00",
        }

    # 去重工具
    def _dedupe_entries(self, entries: list[dict]) -> list[dict]:
        """按标签、金额、来源和上下文对抽取结果去重。"""
        deduped = []
        seen = set()
        for entry in entries:
            key = self._entry_dedupe_key(entry)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(dict(entry))
        return deduped

    def _dedupe_row_issues(self, issues: list[dict]) -> list[dict]:
        """按标签和数值组合去重逐项算术疑点。"""
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
        """按序号和标签去重未完整识别的分项行。"""
        deduped = []
        seen = set()
        for row in rows:
            key = (row.get("serial"), self._normalize_label_key(row.get("label")))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    # 条目上下文与重复键构建
    def _build_entry_context(
        self,
        section_context: dict | None,
        *,
        serial: str | None = None,
        line_index: int | None = None,
    ) -> dict:
        """为抽取结果附带区段、页码、序号等上下文信息。"""
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

    def _entry_context_key(self, entry: dict) -> tuple | None:
        """构造仅由上下文决定的唯一键。"""
        serial = str(entry.get("serial") or "").strip()
        section_id = str(entry.get("section_id") or "").strip()
        section_anchor = self._normalize_label_key(entry.get("section_anchor"))
        section_pages = tuple(
            page for page in (entry.get("section_pages") or []) if isinstance(page, int)
        )
        if serial:
            return ("serial", serial, section_id, section_anchor, section_pages)

        line_index = entry.get("line_index")
        if line_index is not None:
            return ("line", section_id, section_anchor, section_pages, int(line_index))

        if section_id or section_anchor or section_pages:
            return ("section", section_id, section_anchor, section_pages)
        return None

    def _entry_dedupe_key(self, entry: dict) -> tuple:
        """构造用于抽取结果去重的完整键。"""
        amount = entry.get("amount")
        return (
            self._normalize_label_key(entry.get("label")),
            self._format_decimal(amount) if isinstance(amount, Decimal) else amount,
            entry.get("source"),
            bool(entry.get("is_total")),
            bool(entry.get("is_subtotal")),
            self._entry_context_key(entry),
        )

    def _entry_duplicate_key(self, entry: dict) -> tuple:
        """构造用于识别疑似重项的比对键。"""
        amount = entry.get("amount")
        return (
            self._normalize_label_key(entry.get("label")),
            self._format_decimal(amount) if isinstance(amount, Decimal) else amount,
            self._entry_context_key(entry),
        )

    # 序列化
    def _serialize_entries(self, entries: list[dict]) -> list[dict]:
        """将 Decimal 金额转为字符串，便于接口输出。"""
        serialized = []
        for entry in entries:
            normalized_entry = dict(entry)
            for key, value in list(normalized_entry.items()):
                if isinstance(value, Decimal):
                    normalized_entry[key] = self._format_decimal(value)
            serialized.append(normalized_entry)
        return serialized
