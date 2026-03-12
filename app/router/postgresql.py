"""项目与文档元数据路由：对外暴露 PG 业务操作接口。"""

import logging
import os
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from psycopg2 import Error as PsycopgError

from app.model.postgresql_model import (
    DocumentUpdateRequest,
    ProjectBindDocumentsRequest,
    ProjectCreateRequest,
    ProjectRelationUpdateRequest,
    ProjectUpdateRequest,
)
from app.service.analysis_service import get_analysis_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService
from app.utils.text_utils import cleanup_temp_file, save_temp_file

router = APIRouter()
postgres_service = PostgreSQLService()
minio_service = MinioService()
analysis_service = get_analysis_service()
logger = logging.getLogger(__name__)


def _normalize_file_url(file_url: str) -> str:
    normalized_file_url = file_url.strip()
    if not normalized_file_url:
        raise ValueError("file_url cannot be empty")

    if MinioService.is_presigned_url(normalized_file_url):
        object_name = MinioService.object_name_from_presigned_url(normalized_file_url)
        normalized_file_url = MinioService.build_file_url(object_name)
    return normalized_file_url


def _rollback_uploaded_object(upload_result: Optional[dict]) -> Optional[str]:
    if not upload_result:
        return None
    object_name = upload_result.get("object_name")
    if not object_name:
        return None
    try:
        minio_service.delete_file(object_name)
        return None
    except Exception as cleanup_exc:  # pragma: no cover - defensive rollback
        logger.exception("MinIO upload rollback failed: object_name=%s", object_name)
        return str(cleanup_exc)


def _extract_recognition_content(file_bytes: bytes, file_name: str) -> dict:
    file_extension = os.path.splitext(file_name)[1].lower().lstrip(".")
    allowed_extensions = {"pdf", "docx", "doc"}
    if file_extension not in allowed_extensions:
        raise ValueError(
            f"Unsupported file type: {file_extension}. Only pdf/docx/doc are accepted."
        )

    temp_file_path = save_temp_file(file_bytes, f".{file_extension}")
    try:
        recognized_text = analysis_service.extract_text_with_ocr(temp_file_path, file_extension)
        return {
            "file_type": file_extension,
            "content": recognized_text,
            "text_length": len(recognized_text),
        }
    finally:
        cleanup_temp_file(temp_file_path)


@router.post("/projects", summary="新建项目")
async def create_project(payload: ProjectCreateRequest):
    """创建项目主记录。"""
    try:
        project = postgres_service.create_project(payload.identifier_id)
        return {"code": 200, "msg": "project created", "data": project}
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="project identifier already exists") from exc
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects", summary="查询项目列表")
async def list_projects(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """分页查询项目列表。"""
    try:
        result = postgres_service.list_projects(limit=limit, offset=offset)
        return {"code": 200, "msg": "ok", "data": result}
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}", summary="查询项目详情")
async def get_project_detail(identifier_id: str):
    """按业务标识查询项目详情及绑定文档信息。"""
    try:
        detail = postgres_service.get_project_detail(identifier_id)
        if not detail:
            raise HTTPException(status_code=404, detail="project not found")
        return {"code": 200, "msg": "ok", "data": detail}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/projects/{identifier_id}", summary="更新项目标识")
async def update_project(identifier_id: str, payload: ProjectUpdateRequest):
    """更新项目业务标识。"""
    try:
        updated = postgres_service.update_project_identifier(
            identifier_id=identifier_id,
            new_identifier_id=payload.new_identifier_id,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="project not found")
        return {"code": 200, "msg": "project updated", "data": updated}
    except HTTPException:
        raise
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="project identifier already exists") from exc
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/projects/{identifier_id}", summary="删除项目")
async def delete_project(identifier_id: str):
    """逻辑删除项目。"""
    try:
        deleted = postgres_service.soft_delete_project(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="project not found")
        return {"code": 200, "msg": "project deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/projects/{identifier_id}/bind-documents", summary="绑定招标/投标文档")
async def bind_project_documents(identifier_id: str, payload: ProjectBindDocumentsRequest):
    """为项目绑定招标文档与投标文档。"""
    try:
        relation = postgres_service.bind_project_documents(
            identifier_id,
            payload.tender_document_identifier,
            payload.bid_document_identifier,
        )
        return {"code": 200, "msg": "documents bound", "data": relation}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/relations/{relation_id}", summary="查询项目文档关联详情")
async def get_relation_detail(relation_id: int):
    """按关联 ID 查询绑定详情。"""
    try:
        relation = postgres_service.get_relation_by_id(relation_id)
        if not relation:
            raise HTTPException(status_code=404, detail="relation not found")
        return {"code": 200, "msg": "ok", "data": relation}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/relations/{relation_id}", summary="更新项目文档关联")
async def update_relation(relation_id: int, payload: ProjectRelationUpdateRequest):
    """更新关联中的招标/投标文档。"""
    try:
        updated = postgres_service.update_relation(
            relation_id=relation_id,
            tender_document_identifier=payload.tender_document_identifier,
            bid_document_identifier=payload.bid_document_identifier,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="relation not found")
        return {"code": 200, "msg": "relation updated", "data": updated}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/relations/{relation_id}", summary="删除项目文档关联")
async def delete_relation(relation_id: int):
    """删除项目文档关联。"""
    try:
        deleted = postgres_service.delete_relation(relation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="relation not found")
        return {"code": 200, "msg": "relation deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/documents", summary="上传并创建文档记录")
async def create_document(
    file: UploadFile = File(...),
    identifier_id: Optional[str] = Form(default=None),
    document_name: Optional[str] = Form(default=None),
    object_name: Optional[str] = Form(default=None),
):
    """上传文件到 MinIO，写入文档元数据并保存识别内容。"""
    upload_result: Optional[dict] = None
    try:
        upload_result = minio_service.upload_file(file, object_name)
        resolved_file_name = (
            (document_name or "").strip()
            or (file.filename or "").strip()
            or upload_result.get("object_name", "")
        )
        if not resolved_file_name:
            raise ValueError("document_name cannot be empty")

        await file.seek(0)
        file_bytes = await file.read()
        if not file_bytes:
            raise ValueError("uploaded file content is empty")

        recognition_content = _extract_recognition_content(
            file_bytes=file_bytes,
            file_name=(file.filename or resolved_file_name),
        )
        creation_result = postgres_service.create_document_with_content(
            file_name=resolved_file_name,
            file_url=upload_result["file_url"],
            recognition_content=recognition_content,
            identifier_id=identifier_id,
        )
        return {
            "code": 200,
            "msg": "document created",
            "data": {
                "document": creation_result["document"],
                "file_content": creation_result["file_content"],
                "upload": upload_result,
            },
        }
    except ValueError as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        if rollback_error:
            raise HTTPException(
                status_code=400,
                detail=f"{exc}; rollback uploaded object failed: {rollback_error}",
            ) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        detail = f"database error: {exc}"
        if rollback_error:
            detail = f"{detail}; rollback uploaded object failed: {rollback_error}"
        raise HTTPException(status_code=500, detail=detail) from exc
    except RuntimeError as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        detail = str(exc)
        if rollback_error:
            detail = f"{detail}; rollback uploaded object failed: {rollback_error}"
        raise HTTPException(status_code=500, detail=detail) from exc
    except Exception as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        logger.exception("upload-and-create document failed")
        detail = "upload and create document failed, please retry later"
        if rollback_error:
            detail = f"{detail}; rollback failed: {rollback_error}"
        raise HTTPException(status_code=500, detail=detail) from exc


@router.get("/documents", summary="查询文档列表")
async def list_documents(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """分页查询文档列表。"""
    try:
        result = postgres_service.list_documents(limit=limit, offset=offset)
        return {"code": 200, "msg": "ok", "data": result}
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/documents/{identifier_id}", summary="查询文档记录")
async def get_document(identifier_id: str):
    """按业务标识查询文档记录。"""
    try:
        document = postgres_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")
        return {"code": 200, "msg": "ok", "data": document}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/documents/{identifier_id}", summary="更新文档记录")
async def update_document(identifier_id: str, payload: DocumentUpdateRequest):
    """更新文档元数据。"""
    try:
        normalized_file_url = (
            _normalize_file_url(payload.file_url) if payload.file_url is not None else None
        )
        updated = postgres_service.update_document(
            identifier_id=identifier_id,
            file_name=payload.file_name,
            file_url=normalized_file_url,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="document not found")
        return {"code": 200, "msg": "document updated", "data": updated}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/documents/{identifier_id}", summary="删除文档记录")
async def delete_document(identifier_id: str):
    """逻辑删除文档记录。"""
    try:
        deleted = postgres_service.soft_delete_document(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="document not found")
        return {"code": 200, "msg": "document deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc
