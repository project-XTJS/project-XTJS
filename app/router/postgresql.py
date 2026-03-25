"""Project and document metadata routes."""

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


@router.post("/projects", summary="Create project")
async def create_project(
    payload: ProjectCreateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.create_project(payload.identifier_id)
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="project identifier already exists") from exc
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects", summary="List projects")
async def list_projects(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.list_projects(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}", summary="Get project detail")
async def get_project_detail(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        detail = db_service.get_project_detail(identifier_id)
        if not detail:
            raise HTTPException(status_code=404, detail="project not found")
        return detail
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/projects/{identifier_id}", summary="Update project identifier")
async def update_project(
    identifier_id: str,
    payload: ProjectUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
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


@router.delete("/projects/{identifier_id}", summary="Delete project")
async def delete_project(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.soft_delete_project(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="project not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/projects/{identifier_id}/bind-documents", summary="Bind tender and bid documents")
async def bind_project_documents(
    identifier_id: str,
    payload: ProjectBindDocumentsRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
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


@router.get("/relations/{relation_id}", summary="Get relation detail")
async def get_relation_detail(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        relation = db_service.get_relation_by_id(relation_id)
        if not relation:
            raise HTTPException(status_code=404, detail="relation not found")
        return relation
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/relations/{relation_id}", summary="Update relation")
async def update_relation(
    relation_id: int,
    payload: ProjectRelationUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
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


@router.delete("/relations/{relation_id}", summary="Delete relation")
async def delete_relation(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.delete_relation(relation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="relation not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/documents", summary="Upload and create document")
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


@router.get("/documents", summary="List documents")
async def list_documents(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.list_documents(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/documents/{identifier_id}", summary="Get document")
async def get_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")
        return document
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/documents/{identifier_id}", summary="Update document")
async def update_document(
    identifier_id: str,
    payload: DocumentUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
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


@router.delete("/documents/{identifier_id}", summary="Delete document")
async def delete_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.soft_delete_document(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="document not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc
