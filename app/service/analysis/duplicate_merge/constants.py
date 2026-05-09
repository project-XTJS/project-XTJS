# -*- coding: utf-8 -*-
"""
查重合并模块常量
"""
from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)

# 按文档类型映射到聚类结果键名
MERGED_RESULT_KEY_BY_DOC_TYPE = {
    DOCUMENT_TYPE_BUSINESS_BID: "business_bid_duplicate_clusters",
    DOCUMENT_TYPE_TECHNICAL_BID: "technical_bid_duplicate_clusters",
}

# 按文档类型映射到原始查重结果键名
RAW_RESULT_KEY_BY_DOC_TYPE = {
    DOCUMENT_TYPE_BUSINESS_BID: "business_bid_duplicate_check",
    DOCUMENT_TYPE_TECHNICAL_BID: "technical_bid_duplicate_check",
}

# 逆映射：由聚类结果键名反向获取文档类型
DOC_TYPE_BY_MERGED_RESULT_KEY = {
    value: key for key, value in MERGED_RESULT_KEY_BY_DOC_TYPE.items()
}

# 按源结果键名获取对应的文档类型列表（用于处理合并后的查重结果）
DOC_TYPES_BY_SOURCE_RESULT_KEY = {
    "duplicate_check": [DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID],
    "business_bid_duplicate_check": [DOCUMENT_TYPE_BUSINESS_BID],
    "technical_bid_duplicate_check": [DOCUMENT_TYPE_TECHNICAL_BID],
}