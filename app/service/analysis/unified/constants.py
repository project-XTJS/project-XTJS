# unified/constants.py
"""
统一商务标审查 - 类常量

将所有原类体中的类常量集中定义，方便其他 Mixin 共享引用。
"""

import re

# 结果与抽数表的 schema 版本号
RESULT_SCHEMA_VERSION = "1.1"
EXTRACTION_TABLE_SCHEMA_VERSION = "1.0"
# 默认结果键名
DEFAULT_RESULT_KEY = "unified_business_review"
BUSINESS_RESULT_KEY = "business_bid_format_review"

# 页面字段相关常量
PAGE_KEYS = {"page", "page_no", "page_num", "page_index"}
PAGE_LIST_KEYS = {"pages", "page_numbers", "page_nos"}

# 附件引用与页码引用正则
ATTACHMENT_REF_RE = re.compile(r"附件\s*\d+(?:\s*[-－]\s*\d+)?")
PAGE_REF_RE = re.compile(r"第\s*\d+\s*页")

# 文件名后缀识别
BUSINESS_FILE_RE = re.compile(r"[\s_-]*商务标\s*$")
TECHNICAL_FILE_RE = re.compile(r"[\s_-]*技术标\s*$")

# 审查项的展示顺序
CHECK_DISPLAY_ORDER = (
    "integrity_check",
    "consistency_check",
    "pricing_check",
    "itemized_pricing_check",
    "deviation_check",
    "verification_check",
)