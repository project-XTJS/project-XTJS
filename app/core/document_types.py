# -*- coding: utf-8 -*-
"""Document type constants used across project ingestion and analysis."""

from typing import Literal


DOCUMENT_TYPE_TENDER = "tender"
DOCUMENT_TYPE_BUSINESS_BID = "business_bid"
DOCUMENT_TYPE_TECHNICAL_BID = "technical_bid"

DocumentType = Literal["tender", "business_bid", "technical_bid"]

ACTIVE_DOCUMENT_TYPES = (
    DOCUMENT_TYPE_TENDER,
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
SUPPORTED_DOCUMENT_TYPES = ACTIVE_DOCUMENT_TYPES

BUSINESS_BID_COMPATIBLE_TYPES = {
    DOCUMENT_TYPE_BUSINESS_BID,
}
TECHNICAL_BID_COMPATIBLE_TYPES = {
    DOCUMENT_TYPE_TECHNICAL_BID,
}
BID_DOCUMENT_TYPES = {
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
}

DOCUMENT_TYPE_LABELS = {
    DOCUMENT_TYPE_TENDER: "招标文件",
    DOCUMENT_TYPE_BUSINESS_BID: "商务标文件",
    DOCUMENT_TYPE_TECHNICAL_BID: "技术标文件",
}


def get_document_type_label(document_type: str) -> str:
    normalized = (document_type or "").strip().lower()
    return DOCUMENT_TYPE_LABELS.get(normalized, f"文档类型 '{normalized}'")
