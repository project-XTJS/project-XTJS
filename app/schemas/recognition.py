# -*- coding: utf-8 -*-
"""
文档识别相关的响应模型与元数据构建工具。

包含 OCR 分析文件元数据的组装函数，以及 PDF 处理结果的 Pydantic 响应模型。
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DOCUMENT_TYPE_TENDER,
)


def build_analyze_file_metadata(
    *,
    filename: str,
    file_type: str,
    file_size: int,
    page_count: int = 0,
    mime_type: str = "",
    text_length: int = 0,
    parser_engine: str = "",
    source_mode: str = "",
    ocr_engine: str = "",
    ocr_used: bool = False,
    layout_used: bool = False,
    layout_section_count: int = 0,
    recognition_route: str = "",
    recognition_reason: str = "",
    pdf_mode: str = "",
    active_device: str = "",
    seal_detected: bool = False,
    seal_count: int = 0,
    ppstructure_v3_requested: bool | None = None,
    ppstructure_v3_enabled: bool = False,
    seal_recognition_enabled: bool = False,
) -> Dict[str, Any]:
    """
    根据 OCR 提取的各项参数组装标准化的文件元数据字典。

    返回的结构包含文档基础信息与识别细节，用于 /analyze-file 接口的响应体。
    注意：当前实现仅返回精简字段，完整字段可在后续版本中补全。
    """
    return {
        "schema_version": "analyze_file_v3",
        "document": {
            "filename": filename,
            "file_type": file_type,
            "file_size": file_size,
            "page_count": page_count,
            "mime_type": mime_type,
            "text_length": text_length,
        },
        "recognition": {
            "route": recognition_route,
            "ocr_used": ocr_used,
            "layout_used": layout_used,
            "parser_engine": parser_engine,
        },
    }


# PDF 处理阶段一的通用响应基类
class PdfRound1Response(BaseModel):
    """PDF 第一轮轻量解析响应模型，包含文档元信息、处理状态和质量标记。"""

    schema_version: str = "pdf_round1_lite_v1"
    document_meta: "DocumentMeta"
    processing_meta: "ProcessingMeta"
    quality_flags: "QualityFlags" = Field(default_factory=lambda: QualityFlags())
    blocks: List[Dict[str, Any]] = Field(default_factory=list)
    headings: List[Dict[str, Any]] = Field(default_factory=list)
    anchors: List[Dict[str, Any]] = Field(default_factory=list)


class DocumentMeta(BaseModel):
    """文档基础元信息。"""
    document_id: str = ""
    file_name: str = ""
    file_hash: str = ""
    mime_type: str = "application/pdf"
    page_count: int = 0
    document_type: str = ""


class ProcessingMeta(BaseModel):
    """OCR / 解析过程的处理元信息。"""
    parser_engine: str = ""
    ocr_engine: str = ""
    ocr_used: bool = False
    parse_time: str = ""
    avg_confidence: Optional[float] = None
    errors: List[str] = Field(default_factory=list)


class QualityFlags(BaseModel):
    """文档质量标记，用于标识扫描件、低置信度页等潜在问题。"""
    is_scanned_pdf: Optional[bool] = None
    low_confidence_pages: List[int] = Field(default_factory=list)
    suspect_garbled_pages: List[int] = Field(default_factory=list)


# 按文档类型特化的响应模型（通过构造函数自动设置文档类型）
class TenderPdfResponse(PdfRound1Response):
    """招标文件 PDF 响应，自动标记文档类型。"""
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_TENDER


class BusinessBidPdfResponse(PdfRound1Response):
    """商务标 PDF 响应，自动标记文档类型。"""
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_BUSINESS_BID


class TechnicalBidPdfResponse(PdfRound1Response):
    """技术标 PDF 响应，自动标记文档类型。"""
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_TECHNICAL_BID