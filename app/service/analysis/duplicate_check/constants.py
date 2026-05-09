# -*- coding: utf-8 -*-
"""
文档查重常量与预编译模式
"""
import re

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID

SUPPORTED_DOCUMENT_TYPES = (DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID)

PAGE_NUMBER_PATTERN = re.compile(r"^\d+$")
SPLIT_LINE_PATTERN = re.compile(r"[\r\n]+")
SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[。！？!?；;])|(?<=\.)(?=\s|$)")

BUSINESS_SCOPE_SKIP_REASON = "missing_business_duplicate_scope_content"
TEMPLATE_EXCLUDED_SKIP_REASON = "content_fully_covered_by_tender_template"

MIN_SENTENCE_COMPACT_LENGTH = 10
BUSINESS_SIMILARITY_MIN_KEY_LENGTH = 8

BUSINESS_BLOCK_SIMILARITY_THRESHOLD = 0.78
BUSINESS_SECTION_SIMILARITY_THRESHOLD = 0.72
BUSINESS_TABLE_SIMILARITY_THRESHOLD = 0.72

COMMON_DUPLICATE_HEADER_TOKENS = (
    "序号", "项目名称", "招标编号", "项目编号", "招标文件", "采购文件",
    "投标文件", "响应文件", "采购规格", "响应规格", "偏离说明",
    "商务条款", "技术条款", "分项名称", "分项说明", "单价", "合计",
    "备注", "对应投标文件所在页",
)

COMMON_DUPLICATE_REQUIREMENT_TOKENS = (
    "提供复印件", "项目管理经验", "相关领域", "工程师认证证书",
    "认证证书", "毕业时间为准", "投标人送交", "第三方进行计量",
    "提供证书", "招标人提供", "培训相关费用", "合同总价",
    "正式验收", "现场初验收", "试运行及终验",
)

DEVIATION_RESPONSE_TOKENS = (
    "我方", "我公司", "响应", "偏离", "详见", "技术文件",
    "商务文件", "技术分册", "商务分册", "技术册", "商务册",
)

COMMON_DUPLICATE_TEMPLATE_PATTERNS = (
    re.compile(r"^(?:项目名称|项目编号|招标编号|采购编号|招标人|采购人|投标人|供应商)\s*[:：_]"),
    re.compile(r"^(?:GB|GJB|ISO|IEC|YD/T|SJ/T)[A-Z0-9./ -]*[;；。]?$", re.IGNORECASE),
)