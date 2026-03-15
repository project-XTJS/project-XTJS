from typing import Any, Dict


class RecognitionSchemaConfig:
    """PDF 招投标文档一轮识别定义。"""

    # 轻量模板版本号：用于后续兼容升级
    PDF_ROUND1_SCHEMA_VERSION = "pdf_round1_lite_v1"

    # 字段目录：仅保留当前 MVP 必要字段
    PDF_ROUND1_FIELD_CATALOG = {
        # 文档基础信息
        "document_meta": [
            "document_id",
            "file_name",
            "file_hash",
            "mime_type",
            "page_count",
            "document_type",
        ],
        # 识别处理过程信息
        "processing_meta": [
            "parser_engine",
            "ocr_engine",
            "ocr_used",
            "parse_time",
            "avg_confidence",
            "errors",
        ],
        # 核心文本块（通过 page_no + bbox 即可完成定位）
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
        # 章节结构
        "headings": [
            "heading_id",
            "level",
            "title",
            "start_block_id",
            "end_block_id",
            "start_page",
            "end_page",
        ],
        # 关键锚点（如截止时间、商务标清单、★条款）
        "anchors": [
            "type",
            "value",
            "page_no",
            "block_id",
        ],
        # 质量标记
        "quality_flags": [
            "is_scanned_pdf",
            "low_confidence_pages",
            "suspect_garbled_pages",
        ],
    }


def build_pdf_round1_recognition_template(
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
    """
    构建 PDF 一轮识别 JSON 模板。

    调用方在识别后填充 blocks/headings/anchors，并据此做二轮业务抽取。
    """
    return {
        "schema_version": RecognitionSchemaConfig.PDF_ROUND1_SCHEMA_VERSION,
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


def get_pdf_round1_field_catalog() -> Dict[str, Any]:
    """返回轻量字段目录，供其他模块做字段校验或输出 schema 文档。"""
    return dict(RecognitionSchemaConfig.PDF_ROUND1_FIELD_CATALOG)

