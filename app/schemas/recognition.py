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
    seal_covered_text_count: int = 0,
    signature_detected: bool = False,
    signature_count: int = 0,
    ppstructure_v3_requested: bool | None = None,
    ppstructure_v3_enabled: bool = False,
    seal_recognition_enabled: bool = False,
    signature_recognition_enabled: bool = False,
) -> Dict[str, Any]:
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
    schema_version: str = "pdf_round1_lite_v1"
    document_meta: DocumentMeta
    processing_meta: ProcessingMeta
    quality_flags: QualityFlags = Field(default_factory=QualityFlags)
    blocks: List[Dict[str, Any]] = Field(default_factory=list)
    headings: List[Dict[str, Any]] = Field(default_factory=list)
    anchors: List[Dict[str, Any]] = Field(default_factory=list)


class TenderPdfResponse(PdfRound1Response):
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_TENDER


class BusinessBidPdfResponse(PdfRound1Response):
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_BUSINESS_BID


class TechnicalBidPdfResponse(PdfRound1Response):
    def __init__(self, **data):
        super().__init__(**data)
        self.document_meta.document_type = DOCUMENT_TYPE_TECHNICAL_BID
