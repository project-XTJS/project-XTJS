"""项目与文档元数据路由：提供项目、关联、文档的 CRUD 接口。"""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from psycopg2 import Error as PsycopgError

from app.router.dependencies import (
    RecognitionOptions,
    get_db_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.schemas.postgresql import (
    DocumentUpdateRequest,
    ProjectBindDocumentsRequest,
    ProjectCreateRequest,
    ProjectRelationUpdateRequest,
    ProjectUpdateRequest,
)
from app.service.document_ingest_service import normalize_file_url, upload_extract_and_create_document
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()


@router.post("/projects", summary="新建项目")
async def create_project(
    payload: ProjectCreateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """创建项目记录。"""
    try:
        return db_service.create_project(payload.identifier_id)
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="project identifier already exists") from exc
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects", summary="查询项目列表")
async def list_projects(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查询项目列表。"""
    try:
        return db_service.list_projects(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}", summary="查询项目详情")
async def get_project_detail(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """查询单个项目及其关联信息。"""
    try:
        detail = db_service.get_project_detail(identifier_id)
        if not detail:
            raise HTTPException(status_code=404, detail="project not found")
        return detail
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/projects/{identifier_id}", summary="更新项目标识")
async def update_project(
    identifier_id: str,
    payload: ProjectUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """更新项目业务标识。"""
    try:
        updated = db_service.update_project_identifier(
            identifier_id=identifier_id,
            new_identifier_id=payload.new_identifier_id,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="project not found")
        return updated
    except HTTPException:
        raise
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="project identifier already exists") from exc
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/projects/{identifier_id}", summary="删除项目")
async def delete_project(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """逻辑删除项目。"""
    try:
        deleted = db_service.soft_delete_project(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="project not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/projects/{identifier_id}/bind-documents", summary="绑定招标/投标文档")
async def bind_project_documents(
    identifier_id: str,
    payload: ProjectBindDocumentsRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """为项目绑定一对招标/投标文档。"""
    try:
        return db_service.bind_project_documents(
            identifier_id,
            payload.tender_document_identifier,
            payload.bid_document_identifier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/relations/{relation_id}", summary="查询关联详情")
async def get_relation_detail(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """根据关联 ID 查询关联详情。"""
    try:
        relation = db_service.get_relation_by_id(relation_id)
        if not relation:
            raise HTTPException(status_code=404, detail="relation not found")
        return relation
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/relations/{relation_id}", summary="更新关联")
async def update_relation(
    relation_id: int,
    payload: ProjectRelationUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """更新项目文档关联关系。"""
    try:
        updated = db_service.update_relation(
            relation_id=relation_id,
            tender_document_identifier=payload.tender_document_identifier,
            bid_document_identifier=payload.bid_document_identifier,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="relation not found")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/relations/{relation_id}", summary="删除关联")
async def delete_relation(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """删除项目文档关联记录。"""
    try:
        deleted = db_service.delete_relation(relation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="relation not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/documents", summary="上传并创建文档")
async def create_document(
    file: UploadFile = File(...),
    document_type: Literal["tender", "bid"] = Form(...),
    identifier_id: Optional[str] = Form(default=None),
    document_name: Optional[str] = Form(default=None),
    object_name: Optional[str] = Form(default=None),
    recognition_options: RecognitionOptions = Depends(get_form_recognition_options),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
    analysis_service=Depends(get_text_analysis_service),
):
    """上传单文档，执行识别并入库。"""
    result = await upload_extract_and_create_document(
        file=file,
        document_type=document_type,
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
        identifier_id=identifier_id,
        document_name=document_name,
        object_name=object_name,
        **recognition_options.as_kwargs(),
        raise_http_exception=True,
    )
    return {
        "document": result["document"],
        "upload": result["upload"],
    }


@router.get("/documents", summary="查询文档列表")
async def list_documents(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查询文档列表。"""
    try:
        return db_service.list_documents(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/documents/{identifier_id}", summary="查询文档")
async def get_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """按业务标识查询单个文档。"""
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")
        return document
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/documents/{identifier_id}", summary="更新文档")
async def update_document(
    identifier_id: str,
    payload: DocumentUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """更新文档名称或存储地址。"""
    try:
        normalized_file_url = (
            normalize_file_url(payload.file_url) if payload.file_url is not None else None
        )
        updated = db_service.update_document(
            identifier_id=identifier_id,
            file_name=payload.file_name,
            file_url=normalized_file_url,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="document not found")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/documents/{identifier_id}", summary="删除文档")
async def delete_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """逻辑删除文档。"""
    try:
        deleted = db_service.soft_delete_document(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="document not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc
