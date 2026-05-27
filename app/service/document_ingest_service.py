# -*- coding: utf-8 -*-
"""
文档上传、识别与入库的通用服务函数。

提供从文件上传到 OCR 提取、数据库记录的完整流程，
以及仅上传（不 OCR）、对已有文档追加 OCR 等能力。
"""

import logging
import os
from typing import Any, Callable, Optional

from fastapi import HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.core.document_types import DOCUMENT_TYPE_TENDER, DocumentType
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService
from app.service.project_runtime import ProjectTaskCancelledError
from app.utils.text_utils import cleanup_temp_file, save_temp_file

logger = logging.getLogger(__name__)


PDF_POINT_COORDINATE_SYSTEM = "pdf_point"


def _is_tender_document_type(document_type: Any) -> bool:
    return str(document_type or "").strip().lower() == DOCUMENT_TYPE_TENDER


def _clean_pdf_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\x00", "").split()).strip()


def _clean_pdf_bbox(value: Any) -> list[float] | None:
    if value is None:
        return None
    if hasattr(value, "x0") and hasattr(value, "y0") and hasattr(value, "x1") and hasattr(value, "y1"):
        value = [value.x0, value.y0, value.x1, value.y1]
    if not isinstance(value, (list, tuple)) or len(value) < 4:
        return None
    try:
        x0, y0, x1, y1 = [float(value[index]) for index in range(4)]
    except (TypeError, ValueError):
        return None
    if x1 <= x0 or y1 <= y0:
        return None
    return [round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2)]


def _table_records_from_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    if len(rows) <= 1:
        return []
    headers = [cell or f"col_{index + 1}" for index, cell in enumerate(rows[0])]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        record: dict[str, str] = {}
        for index, cell in enumerate(row):
            key = headers[index] if index < len(headers) else f"col_{index + 1}"
            if key in record:
                key = f"{key}_{index + 1}"
            record[key] = cell
        if any(value for value in record.values()):
            records.append(record)
    return records


def _extract_pdf_page_tables(page: Any, page_no: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    find_tables = getattr(page, "find_tables", None)
    if not callable(find_tables):
        return [], []

    try:
        table_result = find_tables()
    except Exception:
        logger.debug("PyMuPDF table extraction failed page=%s", page_no, exc_info=True)
        return [], []

    table_sections: list[dict[str, Any]] = []
    logical_tables: list[dict[str, Any]] = []
    for table_index, table in enumerate(getattr(table_result, "tables", []) or []):
        try:
            raw_rows = table.extract()
        except Exception:
            logger.debug("PyMuPDF table rows extraction failed page=%s table=%s", page_no, table_index, exc_info=True)
            continue

        rows: list[list[str]] = []
        for raw_row in raw_rows or []:
            if not isinstance(raw_row, (list, tuple)):
                continue
            row = [_clean_pdf_text(cell) for cell in raw_row]
            if any(row):
                rows.append(row)
        if not rows:
            continue

        table_text = "\n".join(" | ".join(cell for cell in row if cell) for row in rows).strip()
        bbox = _clean_pdf_bbox(getattr(table, "bbox", None))
        common = {
            "page": page_no,
            "pages": [page_no],
            "type": "table",
            "text": table_text,
            "bbox": bbox,
            "coordinate_system": PDF_POINT_COORDINATE_SYSTEM,
            "source": "pymupdf_text_layer",
            "table_index": table_index,
        }
        table_sections.append(common.copy())
        logical_tables.append(
            {
                **common,
                "headers": rows[0],
                "rows": rows,
                "records": _table_records_from_rows(rows),
            }
        )

    return table_sections, logical_tables


def _extract_pdf_text_layer_result(
    file_path: str,
    file_name: str,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    try:
        import fitz
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("PyMuPDF is required for tender PDF text-layer extraction.") from exc

    try:
        pdf = fitz.open(file_path)
    except Exception as exc:
        raise ValueError(f"Invalid tender PDF: {file_name}") from exc

    try:
        if getattr(pdf, "needs_pass", False):
            raise ValueError(f"Tender PDF is encrypted and cannot be opened: {file_name}")

        pages: list[dict[str, Any]] = []
        layout_sections: list[dict[str, Any]] = []
        table_sections: list[dict[str, Any]] = []
        logical_tables: list[dict[str, Any]] = []
        text_page_count = 0

        for page_index in range(len(pdf)):
            if cancel_check is not None:
                cancel_check()

            page_no = page_index + 1
            page = pdf.load_page(page_index)
            page_rect = _clean_pdf_bbox(page.rect)
            page_line_texts: list[str] = []

            try:
                text_dict = page.get_text("dict", sort=True) or {}
            except TypeError:
                text_dict = page.get_text("dict") or {}

            for block_index, block in enumerate(text_dict.get("blocks") or []):
                if not isinstance(block, dict) or int(block.get("type") or 0) != 0:
                    continue
                block_lines: list[str] = []
                line_payloads: list[dict[str, Any]] = []
                for line_index, line in enumerate(block.get("lines") or []):
                    if not isinstance(line, dict):
                        continue
                    spans = line.get("spans") or []
                    line_text = _clean_pdf_text("".join(str(span.get("text") or "") for span in spans if isinstance(span, dict)))
                    if not line_text:
                        continue
                    block_lines.append(line_text)
                    page_line_texts.append(line_text)
                    line_payloads.append(
                        {
                            "text": line_text,
                            "bbox": _clean_pdf_bbox(line.get("bbox")),
                            "line_index": line_index,
                            "coordinate_system": PDF_POINT_COORDINATE_SYSTEM,
                        }
                    )

                block_text = "\n".join(block_lines).strip()
                if not block_text:
                    continue
                section = {
                    "page": page_no,
                    "type": "text",
                    "text": block_text,
                    "bbox": _clean_pdf_bbox(block.get("bbox")),
                    "coordinate_system": PDF_POINT_COORDINATE_SYSTEM,
                    "source": "pymupdf_text_layer",
                    "block_index": block_index,
                }
                if line_payloads:
                    section["lines"] = line_payloads
                layout_sections.append(section)

            page_text = "\n".join(page_line_texts).strip()
            if not page_text:
                try:
                    page_text = str(page.get_text("text", sort=True) or "").strip()
                except TypeError:
                    page_text = str(page.get_text("text") or "").strip()
                if page_text:
                    layout_sections.append(
                        {
                            "page": page_no,
                            "type": "text",
                            "text": page_text,
                            "bbox": page_rect,
                            "coordinate_system": PDF_POINT_COORDINATE_SYSTEM,
                            "source": "pymupdf_text_layer",
                            "block_index": None,
                        }
                    )

            if page_text:
                text_page_count += 1
            pages.append({"page": page_no, "text": page_text, "bbox": page_rect})

            page_table_sections, page_logical_tables = _extract_pdf_page_tables(page, page_no)
            table_sections.extend(page_table_sections)
            logical_tables.extend(page_logical_tables)

        content = "\n".join(str(page.get("text") or "").strip() for page in pages if str(page.get("text") or "").strip())

        return {
            "content": content,
            "text_length": len(content),
            "pages": pages,
            "page_count": len(pages),
            "parser_engine": "PyMuPDF",
            "source_mode": "pdf_text_layer",
            "active_device": "cpu",
            "ocr_engine": None,
            "ocr_used": False,
            "layout_used": bool(layout_sections),
            "layout_sections": layout_sections,
            "layout_section_count": len(layout_sections),
            "table_sections": table_sections,
            "table_section_count": len(table_sections),
            "native_tables": logical_tables,
            "native_table_count": len(logical_tables),
            "logical_tables": logical_tables,
            "logical_table_count": len(logical_tables),
            "seal_detected": False,
            "seal_count": 0,
            "seal_texts": [],
            "seal_locations": [],
            "signature_detected": False,
            "signature_count": 0,
            "signature_texts": [],
            "signature_locations": [],
            "bbox_coordinate_space": PDF_POINT_COORDINATE_SYSTEM,
            "bbox_source_coordinate_space": PDF_POINT_COORDINATE_SYSTEM,
            "recognition_route": "pymupdf_text_layer",
            "recognition_reason": "tender_pdf_text_layer_only",
            "pdf_mode": "text_layer_only",
            "pdf_text_stats": {
                "page_count": len(pages),
                "text_page_count": text_page_count,
                "layout_section_count": len(layout_sections),
                "logical_table_count": len(logical_tables),
            },
            "ppstructure_v3_requested": False,
            "ppstructure_v3_enabled": False,
            "seal_recognition_enabled": False,
        }
    finally:
        pdf.close()


# 工具函数：URL 归一化与结果精简

def normalize_file_url(file_url: str) -> str:
    """
    将外部传入的预签名 URL 转换为系统内部的 MinIO file URL 格式。
    若已是内部格式则原样返回。
    """
    normalized_file_url = file_url.strip()
    if not normalized_file_url:
        raise ValueError("file_url cannot be empty")

    if MinioService.is_presigned_url(normalized_file_url):
        bucket_name = MinioService.bucket_name_from_presigned_url(normalized_file_url)
        object_name = MinioService.object_name_from_presigned_url(normalized_file_url)
        normalized_file_url = MinioService.build_file_url(object_name, bucket_name)
    return normalized_file_url


def compact_document_payload(document: dict[str, Any]) -> dict[str, Any]:
    """
    提取文档记录的核心字段，减少接口返回体体积。
    避免向前端暴露内部大字段（如 content）。
    """
    fields = (
        "id",
        "identifier_id",
        "document_type",
        "file_name",
        "file_url",
        "extracted",
        "deleted",
        "create_time",
        "update_time",
    )
    return {key: document.get(key) for key in fields if key in document}


# 异常处理与资源回滚

def _rollback_uploaded_object(upload_result: Optional[dict], oss_service: MinioService) -> Optional[str]:
    """
    当后续入库步骤失败时，尝试删除已上传至 MinIO 的对象。
    返回回滚失败的错误信息，成功时返回 None。
    """
    if not upload_result:
        return None
    object_name = upload_result.get("object_name")
    if not object_name:
        return None
    try:
        oss_service.delete_file(object_name)
        return None
    except Exception as cleanup_exc:  # pragma: no cover
        logger.exception("MinIO upload rollback failed: object_name=%s", object_name)
        return str(cleanup_exc)


def _log_document_pipeline_exception(
    *,
    operation: str,
    file_name: str,
    document_type: Optional[DocumentType] = None,
    identifier_id: Optional[str] = None,
    upload_result: Optional[dict] = None,
) -> None:
    """Log file-level failures before they are converted into response payloads."""
    context = {
        "operation": operation,
        "file_name": str(file_name or "").strip() or "<unknown>",
        "document_type": str(document_type or "").strip() or "<unknown>",
        "identifier_id": str(identifier_id or "").strip() or "<none>",
        "bucket_name": str((upload_result or {}).get("bucket_name") or "").strip() or "<none>",
        "object_name": str((upload_result or {}).get("object_name") or "").strip() or "<none>",
    }
    logger.exception(
        "document pipeline failed "
        "operation=%(operation)s file_name=%(file_name)s "
        "document_type=%(document_type)s identifier_id=%(identifier_id)s "
        "bucket_name=%(bucket_name)s object_name=%(object_name)s",
        context,
    )


def _extract_recognition_content(
    file_bytes: bytes,
    file_name: str,
    analysis_service,
    cancel_check: Callable[[], None] | None = None,
    *,
    document_type: Any = None,
) -> dict:
    """对单文件执行识别，并输出用于存储的精简识别结果。"""
    file_extension = os.path.splitext(file_name)[1].lower().lstrip(".")
    if _is_tender_document_type(document_type):
        if file_extension != "pdf":
            raise ValueError("Tender documents must be PDF files when OCR fallback is disabled.")
        temp_file_path = save_temp_file(file_bytes, ".pdf")
        try:
            if cancel_check is not None:
                cancel_check()
            recognition_result = _extract_pdf_text_layer_result(
                temp_file_path,
                file_name,
                cancel_check=cancel_check,
            )
            if cancel_check is not None:
                cancel_check()
            recognition_result.pop("content", None)
            recognition_result.pop("pages", None)
            recognition_result["filename"] = file_name
            return recognition_result
        finally:
            cleanup_temp_file(temp_file_path)

    allowed_extensions = set(analysis_service.get_supported_extensions())
    if file_extension not in allowed_extensions:
        raise ValueError(
            f"Unsupported file type: {file_extension}. "
            f"Supported types: {', '.join(sorted(allowed_extensions))}."
        )

    temp_file_path = save_temp_file(file_bytes, f".{file_extension}")
    try:
        if cancel_check is not None:
            cancel_check()
        recognition_result = analysis_service.extract_text_result(
            temp_file_path,
            file_extension,
            cancel_check=cancel_check,
        )
        if cancel_check is not None:
            cancel_check()
        # 去掉大字段，避免把完整文本和分页内容直接写入数据库。
        recognition_result.pop("content", None)
        recognition_result.pop("pages", None)
        recognition_result["filename"] = file_name
        return recognition_result
    finally:
        cleanup_temp_file(temp_file_path)


def _format_upload_create_error(exc: Exception, rollback_error: Optional[str]) -> tuple[int, str]:
    """
    将上传/识别/入库过程中抛出的异常统一映射为 (HTTP 状态码, 错误描述)，
    并在回滚失败时附加回滚错误提示。
    """
    if isinstance(exc, ValueError):
        status_code = 400
        detail = str(exc)
    elif isinstance(exc, PsycopgError):
        status_code = 500
        detail = f"database error: {exc}"
    elif isinstance(exc, RuntimeError):
        status_code = 500
        detail = str(exc)
    else:
        status_code = 500
        detail = "upload and create document failed, please retry later"
        logger.exception("upload-and-create document failed")

    if rollback_error:
        detail = f"{detail}; rollback uploaded object failed: {rollback_error}"
    return status_code, detail


# OCR 识别辅助

def _extract_recognition_content(
    file_bytes: bytes,
    file_name: str,
    analysis_service,
    cancel_check: Callable[[], None] | None = None,
    *,
    document_type: Any = None,
) -> dict:
    """
    对单文件执行 OCR 提取，返回精简的识别结果字典。
    已移除 content、pages 等大字段，避免直接存入数据库主表。
    """
    file_extension = os.path.splitext(file_name)[1].lower().lstrip(".")
    if _is_tender_document_type(document_type):
        if file_extension != "pdf":
            raise ValueError("Tender documents must be PDF files when OCR fallback is disabled.")
        temp_file_path = save_temp_file(file_bytes, ".pdf")
        try:
            if cancel_check is not None:
                cancel_check()
            recognition_result = _extract_pdf_text_layer_result(
                temp_file_path,
                file_name,
                cancel_check=cancel_check,
            )
            if cancel_check is not None:
                cancel_check()
            recognition_result.pop("content", None)
            recognition_result.pop("pages", None)
            recognition_result["filename"] = file_name
            return recognition_result
        finally:
            cleanup_temp_file(temp_file_path)

    allowed_extensions = set(analysis_service.get_supported_extensions())
    if file_extension not in allowed_extensions:
        raise ValueError(
            f"Unsupported file type: {file_extension}. "
            f"Supported types: {', '.join(sorted(allowed_extensions))}."
        )

    temp_file_path = save_temp_file(file_bytes, f".{file_extension}")
    try:
        if cancel_check is not None:
            cancel_check()
        recognition_result = analysis_service.extract_text_result(
            temp_file_path,
            file_extension,
            cancel_check=cancel_check,
        )
        if cancel_check is not None:
            cancel_check()
        # 精简识别内容，避免数据库膨胀
        recognition_result.pop("content", None)
        recognition_result.pop("pages", None)
        recognition_result["filename"] = file_name
        return recognition_result
    finally:
        cleanup_temp_file(temp_file_path)


# 核心流程：上传 + 识别 + 入库

async def upload_extract_and_create_document(
    *,
    file: UploadFile,
    document_type: DocumentType,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
    identifier_id: Optional[str] = None,
    document_name: Optional[str] = None,
    object_name: Optional[str] = None,
    raise_http_exception: bool = True,
) -> dict[str, Any]:
    """
    一体化文档处理流程：
    1) 上传文件至 MinIO
    2) 对文件执行 OCR 提取
    3) 在数据库中创建文档记录并写入识别内容
    失败时自动回滚已上传的对象。
    """
    upload_result: Optional[dict] = None
    resolved_file_name = (document_name or "").strip() or (file.filename or "").strip()
    try:
        # 步骤1：上传文件
        upload_result = await run_in_threadpool(oss_service.upload_file, file, object_name)
        resolved_file_name = (
            (document_name or "").strip()
            or (file.filename or "").strip()
            or upload_result.get("object_name", "")
        )
        if not resolved_file_name:
            raise ValueError("document_name cannot be empty")

        # 步骤2：读取并 OCR
        await file.seek(0)
        file_bytes = await file.read()
        if not file_bytes:
            raise ValueError("uploaded file content is empty")

        recognition_content = await run_in_threadpool(
            _extract_recognition_content,
            file_bytes,
            (file.filename or resolved_file_name),
            analysis_service,
            document_type=document_type,
        )

        # 步骤3：写入数据库
        creation_result = await run_in_threadpool(
            db_service.create_document_with_content,
            resolved_file_name,
            upload_result["file_url"],
            document_type,
            recognition_content,
            identifier_id,
        )
        document = creation_result["document"]
        return {
            "ok": True,
            "document": document,
            "document_summary": compact_document_payload(document),
            "upload": upload_result,
            "resolved_file_name": resolved_file_name,
        }
    except Exception as exc:
        _log_document_pipeline_exception(
            operation="upload_extract_and_create_document",
            file_name=resolved_file_name or (file.filename or ""),
            document_type=document_type,
            identifier_id=identifier_id,
            upload_result=upload_result,
        )
        # 失败时尝试回滚上传对象，并统一返回错误信息。
        rollback_error = _rollback_uploaded_object(upload_result, oss_service)
        status_code, detail = _format_upload_create_error(exc, rollback_error)
        if raise_http_exception:
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return {
            "ok": False,
            "status_code": status_code,
            "error": detail,
            "upload": upload_result,
        }


async def upload_and_create_document_without_ocr(
    *,
    file: UploadFile,
    document_type: DocumentType,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    identifier_id: Optional[str] = None,
    document_name: Optional[str] = None,
    object_name: Optional[str] = None,
    raise_http_exception: bool = True,
) -> dict[str, Any]:
    """
    仅上传文件并创建文档记录，不执行 OCR 提取。
    适用于延迟 OCR（如技术标分阶段处理）的场景。
    """
    upload_result: Optional[dict] = None
    resolved_file_name = (document_name or "").strip() or (file.filename or "").strip()
    try:
        upload_result = await run_in_threadpool(oss_service.upload_file, file, object_name)
        resolved_file_name = (
            (document_name or "").strip()
            or (file.filename or "").strip()
            or upload_result.get("object_name", "")
        )
        if not resolved_file_name:
            raise ValueError("document_name cannot be empty")

        creation_result = await run_in_threadpool(
            db_service.create_document,
            resolved_file_name,
            upload_result["file_url"],
            document_type,
            identifier_id,
        )
        return {
            "ok": True,
            "document": creation_result,
            "document_summary": compact_document_payload(creation_result),
            "upload": upload_result,
            "resolved_file_name": resolved_file_name,
        }
    except Exception as exc:
        _log_document_pipeline_exception(
            operation="upload_and_create_document_without_ocr",
            file_name=resolved_file_name or (file.filename or ""),
            document_type=document_type,
            identifier_id=identifier_id,
            upload_result=upload_result,
        )
        rollback_error = _rollback_uploaded_object(upload_result, oss_service)
        status_code, detail = _format_upload_create_error(exc, rollback_error)
        if raise_http_exception:
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return {
            "ok": False,
            "status_code": status_code,
            "error": detail,
            "upload": upload_result,
        }


# 已有文档追加 OCR

async def recognize_existing_document(
    *,
    document_identifier: str,
    project_identifier: str | None = None,
    cancel_check: Callable[[], None] | None = None,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
    raise_http_exception: bool = True,
) -> dict[str, Any]:
    document: dict[str, Any] | None = None
    object_context: Optional[dict[str, Any]] = None
    try:
        if cancel_check is not None:
            cancel_check()
        document = await run_in_threadpool(db_service.get_document_by_identifier, document_identifier)
        if not document:
            raise ValueError(f"document not found: {document_identifier}")

        file_url = str(document.get("file_url") or "").strip()
        if not file_url:
            raise ValueError(f"document file_url is empty: {document_identifier}")

        # 解析 MinIO 对象路径
        if file_url.startswith("minio://"):
            bucket_name, object_name = MinioService.bucket_and_object_from_file_url(file_url)
        elif MinioService.is_presigned_url(file_url):
            bucket_name, object_name = MinioService.bucket_and_object_from_presigned_url(file_url)
        else:
            raise ValueError(f"unsupported document file_url: {file_url}")
        object_context = {"bucket_name": bucket_name, "object_name": object_name}

        if cancel_check is not None:
            cancel_check()
        file_bytes, _ = await run_in_threadpool(
            oss_service.get_object_bytes,
            object_name,
            bucket_name,
        )
        if cancel_check is not None:
            cancel_check()
        recognition_content = await run_in_threadpool(
            _extract_recognition_content,
            file_bytes,
            str(document.get("file_name") or document_identifier),
            analysis_service,
            cancel_check,
            document_type=document.get("document_type"),
        )
        if cancel_check is not None:
            cancel_check()
        updated_document = await run_in_threadpool(
            db_service.update_document_content,
            document_identifier,
            recognition_content,
        )
        if not updated_document:
            raise ValueError(f"failed to update document content: {document_identifier}")
        return {
            "ok": True,
            "document": updated_document,
            "document_summary": compact_document_payload(updated_document),
            "recognition_content": recognition_content,
        }
    except ProjectTaskCancelledError:
        logger.info(
            "document recognition cancelled project_identifier=%s document_identifier=%s",
            str(project_identifier or "").strip() or "<none>",
            document_identifier,
        )
        raise
    except Exception as exc:
        _log_document_pipeline_exception(
            operation="recognize_existing_document",
            file_name=str((document or {}).get("file_name") or document_identifier),
            document_type=(document or {}).get("document_type"),
            identifier_id=document_identifier,
            upload_result=object_context,
        )
        if raise_http_exception:
            status_code, detail = _format_upload_create_error(exc, None)
            raise HTTPException(status_code=status_code, detail=detail) from exc
        status_code, detail = _format_upload_create_error(exc, None)
        return {
            "ok": False,
            "status_code": status_code,
            "error": detail,
        }
