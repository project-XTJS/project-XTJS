# -*- coding: utf-8 -*-
"""
文档类型常量与类型定义模块

定义项目中所有文档类型、兼容关系以及中文标签映射，
并暴露用于获取标签的工具函数。
"""

from typing import Literal

# 基础文档类型常量
DOCUMENT_TYPE_TENDER = "tender"                # 招标文件
DOCUMENT_TYPE_BUSINESS_BID = "business_bid"    # 商务标文件
DOCUMENT_TYPE_TECHNICAL_BID = "technical_bid"  # 技术标文件
LEGACY_DOCUMENT_TYPE_BID = "bid"               # 历史遗留的投标文件类型，仅用于兼容旧数据

# 类型别名（仅包含当前活跃的类型，用于类型标注）
DocumentType = Literal["tender", "business_bid", "technical_bid"]

# 活跃与支持的文档类型
ACTIVE_DOCUMENT_TYPES = (
    DOCUMENT_TYPE_TENDER,
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
SUPPORTED_DOCUMENT_TYPES = ACTIVE_DOCUMENT_TYPES + (LEGACY_DOCUMENT_TYPE_BID,)

# 各业务线兼容的文档类型集合
BUSINESS_BID_COMPATIBLE_TYPES = {
    DOCUMENT_TYPE_BUSINESS_BID,
    LEGACY_DOCUMENT_TYPE_BID,
}
TECHNICAL_BID_COMPATIBLE_TYPES = {
    DOCUMENT_TYPE_TECHNICAL_BID,
}
BID_DOCUMENT_TYPES = {
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    LEGACY_DOCUMENT_TYPE_BID,
}

# 文档类型中文标签映射
DOCUMENT_TYPE_LABELS = {
    DOCUMENT_TYPE_TENDER: "招标文件",
    DOCUMENT_TYPE_BUSINESS_BID: "商务标文件",
    DOCUMENT_TYPE_TECHNICAL_BID: "技术标文件",
    LEGACY_DOCUMENT_TYPE_BID: "历史投标文件",
}

# 工具函数
def get_document_type_label(document_type: str) -> str:
    """
    根据文档类型字符串返回对应的中文标签。

    参数：
        document_type: 文档类型字符串（大小写不敏感，自动去除首尾空白）

    返回：
        若找到匹配类型，返回对应中文标签；否则返回描述性提示字符串。
    """
    normalized = (document_type or "").strip().lower()
    return DOCUMENT_TYPE_LABELS.get(normalized, f"文档类型 '{normalized}'")