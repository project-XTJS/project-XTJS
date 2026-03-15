from typing import Any, Dict


class PdfRound1SchemaConfig:
    """PDF 一轮识别Schema 配置。"""

    # 一轮识别 JSON 的版本号，后续字段变更时用于兼容
    SCHEMA_VERSION = "pdf_round1_lite_v1"
    # 文档类型常量：招标文件 / 投标文件
    DOCUMENT_TYPE_TENDER = "tender"
    DOCUMENT_TYPE_BID = "bid"

    # 一轮识别最小字段目录
    FIELD_CATALOG = {
        # 文档元信息
        "document_meta": [
            "document_id",
            "file_name",
            "file_hash",
            "mime_type",
            "page_count",
            "document_type",
        ],
        # 识别过程信息（引擎、耗时、置信度等）
        "processing_meta": [
            "parser_engine",
            "ocr_engine",
            "ocr_used",
            "parse_time",
            "avg_confidence",
            "errors",
        ],
        # 核心文本块（正文主体）
        "blocks": [
            "block_id",
            "page_no",
            "block_type",
            "order_in_page",
            "text_raw",
            "text_norm",
            "bbox",
            "confidence",
        ],
        # 标题层级结构（章节边界）
        "headings": [
            "heading_id",
            "level",
            "title",
            "start_block_id",
            "end_block_id",
            "start_page",
            "end_page",
        ],
        # 关键锚点（截止时间、附件、★条款等）
        "anchors": [
            "type",
            "value",
            "page_no",
            "block_id",
        ],
        # 质量标记（用于触发人工复核）
        "quality_flags": [
            "is_scanned_pdf",
            "low_confidence_pages",
            "suspect_garbled_pages",
        ],
    }


def build_pdf_round1_template(
    *,
    document_id: str = "",
    file_name: str = "",
    file_hash: str = "",
    mime_type: str = "application/pdf",
    page_count: int = 0,
    document_type: str = "",
    parser_engine: str = "",
    ocr_engine: str = "",
    ocr_used: bool = False,
) -> Dict[str, Any]:
    """构建通用 PDF 一轮识别模板。"""
    return {
        "schema_version": PdfRound1SchemaConfig.SCHEMA_VERSION,
        "document_meta": {
            "document_id": document_id,
            "file_name": file_name,
            "file_hash": file_hash,
            "mime_type": mime_type,
            "page_count": page_count,
            "document_type": document_type,
        },
        "processing_meta": {
            "parser_engine": parser_engine,
            "ocr_engine": ocr_engine,
            "ocr_used": ocr_used,
            "parse_time": "",
            "avg_confidence": None,
            "errors": [],
        },
        "blocks": [],
        "headings": [],
        "anchors": [],
        "quality_flags": {
            "is_scanned_pdf": None,
            "low_confidence_pages": [],
            "suspect_garbled_pages": [],
        },
    }


def build_tender_pdf_round1_template(
    *,
    document_id: str = "",
    file_name: str = "",
    file_hash: str = "",
    mime_type: str = "application/pdf",
    page_count: int = 0,
    parser_engine: str = "",
    ocr_engine: str = "",
    ocr_used: bool = False,
) -> Dict[str, Any]:
    """构建招标文件（tender）的一轮识别模板。"""
    return build_pdf_round1_template(
        document_id=document_id,
        file_name=file_name,
        file_hash=file_hash,
        mime_type=mime_type,
        page_count=page_count,
        document_type=PdfRound1SchemaConfig.DOCUMENT_TYPE_TENDER,
        parser_engine=parser_engine,
        ocr_engine=ocr_engine,
        ocr_used=ocr_used,
    )


def build_bid_pdf_round1_template(
    *,
    document_id: str = "",
    file_name: str = "",
    file_hash: str = "",
    mime_type: str = "application/pdf",
    page_count: int = 0,
    parser_engine: str = "",
    ocr_engine: str = "",
    ocr_used: bool = False,
) -> Dict[str, Any]:
    """构建投标文件（bid）的一轮识别模板。"""
    return build_pdf_round1_template(
        document_id=document_id,
        file_name=file_name,
        file_hash=file_hash,
        mime_type=mime_type,
        page_count=page_count,
        document_type=PdfRound1SchemaConfig.DOCUMENT_TYPE_BID,
        parser_engine=parser_engine,
        ocr_engine=ocr_engine,
        ocr_used=ocr_used,
    )


def get_pdf_round1_lite_field_catalog() -> Dict[str, Any]:
    """返回一轮识别字段目录。"""
    return dict(PdfRound1SchemaConfig.FIELD_CATALOG)


# 兼容旧命名：避免历史调用立即失效
RecognitionSchemaConfig = PdfRound1SchemaConfig
build_pdf_round1_recognition_template = build_pdf_round1_template
build_tender_pdf_round1_recognition_template = build_tender_pdf_round1_template
build_bid_pdf_round1_recognition_template = build_bid_pdf_round1_template
get_pdf_round1_field_catalog = get_pdf_round1_lite_field_catalog
