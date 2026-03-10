"""项目与文档元数据路由：对外暴露 PG 业务操作接口。"""

from fastapi import APIRouter, HTTPException
from psycopg2 import Error as PsycopgError

from app.model.postgresql_model import (
    DocumentCreateRequest,
    ProjectBindDocumentsRequest,
    ProjectCreateRequest,
)
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()
postgres_service = PostgreSQLService()


@router.post("/projects", summary="新建项目")
async def create_project(payload: ProjectCreateRequest):
    """创建项目主记录。"""
    try:
        project = postgres_service.create_project(payload.identifier_id)
        return {"code": 200, "msg": "project created", "data": project}
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


@router.post("/documents", summary="新建文档记录")
async def create_document(payload: DocumentCreateRequest):
    """创建文档元数据记录。"""
    try:
        document = postgres_service.create_document(
            payload.file_name, payload.file_url, payload.identifier_id
        )
        return {"code": 200, "msg": "document created", "data": document}
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
