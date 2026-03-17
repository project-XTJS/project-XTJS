from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field

# 1. 供 analyze-file 接口使用的元数据构建函数
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
    ocr_available: bool = False,
    active_device: str = "",
    seal_enabled: bool = False,
    seal_removed: bool = False,
    seal_detected: bool = False,
    seal_count: int = 0,
    seal_texts: list[str] | None = None,
) -> Dict[str, Any]:
    """构建 analyze-file 的结构化元数据。"""
    return {
        "schema_version": "analyze_file_v1",
        "document": {
            "filename": filename,
            "file_type": file_type,
            "file_size": file_size,
            "page_count": page_count,
            "mime_type": mime_type,
            "text_length": text_length,
        },
        "processing": {
            "parser_engine": parser_engine,
            "source_mode": source_mode,
        },
        "ocr": {
            "available": ocr_available,
            "used": ocr_used,
            "engine": ocr_engine,
            "active_device": active_device,
        },
        "seal": {
            "enabled": seal_enabled,
            "removed": seal_removed,
            "detected": seal_detected,
            "count": seal_count,
            "texts": seal_texts or [],
        },
    }

# 2. PDF 一轮识别
class DocumentMeta(BaseModel):
    document_id: str = ""
    file_name: str = ""
    file_hash: str = ""
    mime_type: str = "application/pdf"
    page_count: int = 0
    document_type: str = ""

class ProcessingMeta(BaseModel):
    parser_engine: str = ""
    ocr_engine: str = ""
    ocr_used: bool = False
    parse_time: str = ""
    avg_confidence: Optional[float] = None
    errors: List[str] = Field(default_factory=list)

class QualityFlags(BaseModel):
    is_scanned_pdf: Optional[bool] = None
    low_confidence_pages: List[int] = Field(default_factory=list)
    suspect_garbled_pages: List[int] = Field(default_factory=list)

class PdfRound1Response(BaseModel):
    """PDF 一轮识别的标准返回结构"""
    schema_version: str = "pdf_round1_lite_v1"
    document_meta: DocumentMeta
    processing_meta: ProcessingMeta
    quality_flags: QualityFlags = Field(default_factory=QualityFlags)
    blocks: List[Dict[str, Any]] = Field(default_factory=list)
    headings: List[Dict[str, Any]] = Field(default_factory=list)
    anchors: List[Dict[str, Any]] = Field(default_factory=list)

class TenderPdfResponse(PdfRound1Response):
    """招标文件的专属响应模型（继承自通用模型）"""
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = "tender"

class BidPdfResponse(PdfRound1Response):
    """投标文件的专属响应模型"""
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = "bid"