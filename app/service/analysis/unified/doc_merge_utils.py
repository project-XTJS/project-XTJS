# unified/doc_merge_utils.py
"""
统一商务标审查 - 文档合并与页码偏移 Mixin

提供商务标和技术标的合并（技术标页码整体偏移），以及页码计算工具。
"""

from __future__ import annotations

import copy
from typing import Any


class DocMergeUtilsMixin:
    """
    文档合并与页码工具。

    依赖：
    - 常量：PAGE_KEYS, PAGE_LIST_KEYS
    """

    PAGE_KEYS: set
    PAGE_LIST_KEYS: set

    def _merge_bid_documents(
        self,
        business_payload: dict[str, Any],
        technical_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """合并商务标和技术标的 data 节点，技术标页码做整体偏移。"""
        business_data = self._data_node(business_payload)
        technical_data = self._data_node(technical_payload)

        page_offset = self._page_count(business_data)
        merged_data: dict[str, Any] = {}
        keys = set(business_data.keys()) | set(technical_data.keys())

        for key in keys:
            left = business_data.get(key)
            right = technical_data.get(key)
            if isinstance(left, list) or isinstance(right, list):
                left_list = left if isinstance(left, list) else []
                right_list = right if isinstance(right, list) else []
                merged_data[key] = copy.deepcopy(left_list) + self._offset_page_refs(
                    right_list, page_offset, parent_key=key
                )
            elif left is not None:
                merged_data[key] = copy.deepcopy(left)
            else:
                merged_data[key] = self._offset_page_refs(right, page_offset, parent_key=key)

        return {"data": merged_data}

    def _offset_page_refs(self, value: Any, offset: int, *, parent_key: str | None = None) -> Any:
        """对字典或列表中的页码字段按 offset 增加。"""
        if offset <= 0:
            return copy.deepcopy(value)

        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key in self.PAGE_KEYS and isinstance(item, int):
                    result[key] = item + offset
                elif key in self.PAGE_LIST_KEYS and isinstance(item, list):
                    result[key] = [
                        member + offset if isinstance(member, int) else copy.deepcopy(member)
                        for member in item
                    ]
                else:
                    result[key] = self._offset_page_refs(item, offset, parent_key=key)
            return result

        if isinstance(value, list):
            if parent_key in self.PAGE_LIST_KEYS:
                return [
                    member + offset if isinstance(member, int) else copy.deepcopy(member)
                    for member in value
                ]
            return [self._offset_page_refs(member, offset, parent_key=parent_key) for member in value]

        return copy.deepcopy(value)

    def _page_count(self, data_node: dict[str, Any]) -> int:
        """统计数据节点中的总页数。"""
        pages = data_node.get("pages") or []
        if isinstance(pages, list) and pages:
            return len(pages)

        max_page = 0
        for collection_key in ("layout_sections", "logical_tables", "native_tables"):
            for item in data_node.get(collection_key, []) or []:
                max_page = max(max_page, self._max_page_in_payload(item))
        return max_page

    def _max_page_in_payload(self, payload: Any) -> int:
        """递归获取一个字典/列表结构中的最大页码。"""
        if isinstance(payload, dict):
            values = []
            for key, value in payload.items():
                if key in self.PAGE_KEYS and isinstance(value, int):
                    values.append(value)
                elif key in self.PAGE_LIST_KEYS and isinstance(value, list):
                    values.extend(member for member in value if isinstance(member, int))
                else:
                    values.append(self._max_page_in_payload(value))
            return max(values or [0])
        if isinstance(payload, list):
            return max((self._max_page_in_payload(item) for item in payload), default=0)
        return 0