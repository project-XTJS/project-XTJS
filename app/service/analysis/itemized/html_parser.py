"""
分项报价 - HTML 表格解析辅助

将 HTML 表格片段解析成保留行列合并信息的二维单元格结构。
"""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser


class _TableHTMLParser(HTMLParser):
    """解析 HTML 表格片段，提取带 rowspan/colspan 的单元格文本。"""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[dict]] = []
        self._current_row: list[dict] | None = None
        self._current_cell: dict | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """遇到行或单元格起始标签时创建对应的缓存结构。"""
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

    def handle_endtag(self, tag: str) -> None:
        """遇到结束标签时落盘当前单元格或整行数据。"""
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

    def handle_data(self, data: str) -> None:
        """累积当前单元格内的文本内容。"""
        if self._current_cell is not None:
            self._current_cell["text_parts"].append(data)

    @staticmethod
    def _safe_span(value: str | None) -> int:
        """将 rowspan/colspan 安全转换为正整数，异常时回退为 1。"""
        try:
            return max(1, int(str(value or "1")))
        except ValueError:
            return 1