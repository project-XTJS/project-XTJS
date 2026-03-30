"""文档上传、识别与入库的通用服务函数。"""

import logging
import os
from typing import Any, Optional

from fastapi import HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.core.document_types import DocumentType
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService
from app.utils.text_utils import cleanup_temp_file, save_temp_file

logger = logging.getLogger(__name__)


def normalize_file_url(file_url: str) -> str:
    """将预签名 URL 归一化为系统内部 file_url。"""
    normalized_file_url = file_url.strip()
    if not normalized_file_url:
        raise ValueError("file_url cannot be empty")

    if MinioService.is_presigned_url(normalized_file_url):
        bucket_name = MinioService.bucket_name_from_presigned_url(normalized_file_url)
        object_name = MinioService.object_name_from_presigned_url(normalized_file_url)
        normalized_file_url = MinioService.build_file_url(object_name, bucket_name)
    return normalized_file_url


def compact_document_payload(document: dict[str, Any]) -> dict[str, Any]:
    """提取文档核心字段，减少返回体体积。"""
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


def _rollback_uploaded_object(upload_result: Optional[dict], oss_service: MinioService) -> Optional[str]:
    """当后续步骤失败时，回滚已上传的 MinIO 对象。"""
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


def _extract_recognition_content(
    file_bytes: bytes,
    file_name: str,
    analysis_service,
) -> dict:
    """对单文件执行识别，并输出用于存储的精简识别结果。"""
    file_extension = os.path.splitext(file_name)[1].lower().lstrip(".")
    allowed_extensions = set(analysis_service.get_supported_extensions())
    if file_extension not in allowed_extensions:
        raise ValueError(
            f"Unsupported file type: {file_extension}. "
            f"Supported types: {', '.join(sorted(allowed_extensions))}."
        )

    temp_file_path = save_temp_file(file_bytes, f".{file_extension}")
    try:
        recognition_result = analysis_service.extract_text_result(
            temp_file_path,
            file_extension,
        )
        # 去掉大字段，避免把完整文本和分页内容直接写入数据库。
        recognition_result.pop("content", None)
        recognition_result.pop("pages", None)
        recognition_result["filename"] = file_name
        return recognition_result
    finally:
        cleanup_temp_file(temp_file_path)


def _format_upload_create_error(exc: Exception, rollback_error: Optional[str]) -> tuple[int, str]:
    """将内部异常统一映射为 HTTP 状态码和错误信息。"""
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
    """执行上传、识别、入库一体化流程。"""
    upload_result: Optional[dict] = None
    try:
        # 第一步：上传文件到对象存储。
        upload_result = await run_in_threadpool(oss_service.upload_file, file, object_name)
        resolved_file_name = (
            (document_name or "").strip()
            or (file.filename or "").strip()
            or upload_result.get("object_name", "")
        )
        if not resolved_file_name:
            raise ValueError("document_name cannot be empty")

        # 第二步：读取文件内容并执行识别。
        await file.seek(0)
        file_bytes = await file.read()
        if not file_bytes:
            raise ValueError("uploaded file content is empty")

        recognition_content = await run_in_threadpool(
            _extract_recognition_content,
            file_bytes,
            (file.filename or resolved_file_name),
            analysis_service,
        )

        # 第三步：写入文档记录与识别内容。
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
