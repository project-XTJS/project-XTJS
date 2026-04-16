"""项目与文档 CRUD 路由。"""

from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from psycopg2 import Error as PsycopgError

from app.core.document_types import DocumentType
from app.router.dependencies import (
    RecognitionOptions,
    get_bid_document_review_service,
    get_db_service,
    get_duplicate_check_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.schemas.postgresql import (
    DuplicateCheckScope,
    DocumentUpdateRequest,
    ProjectBindDocumentsRequest,
    ProjectCreateRequest,
    ProjectDuplicateCheckRequest,
    ProjectRelationUpdateRequest,
    ProjectUpdateRequest,
)
from app.service.analysis import BidDocumentReviewService, DuplicateCheckService
from app.service.document_ingest_service import normalize_file_url, upload_extract_and_create_document
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()


def _document_types_from_scope(scope: DuplicateCheckScope) -> Optional[list[str]]:
    if scope == DuplicateCheckScope.ALL:
        return None
    return [scope.value]


def _run_project_duplicate_check(
    *,
    identifier_id: str,
    document_types: Optional[list[str]],
    max_evidence_sections: int,
    max_pairs_per_type: int,
    db_service: PostgreSQLService,
    duplicate_check_service: DuplicateCheckService,
):
    payload_data = db_service.get_project_documents_for_duplicate_check(identifier_id)
    if not payload_data:
        raise HTTPException(status_code=404, detail="项目不存在")

    duplicate_result = duplicate_check_service.check_project_documents(
        project_identifier=identifier_id,
        project=payload_data["project"],
        document_records=payload_data["documents"],
        document_types=document_types,
        max_evidence_sections=max_evidence_sections,
        max_pairs_per_type=max_pairs_per_type,
    )
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key="duplicate_check",
        result_value=duplicate_result,
    )
    return duplicate_result


def _run_project_bid_document_review(
    *,
    identifier_id: str,
    document_types: Optional[list[str]],
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
):
    payload_data = db_service.get_project_documents_for_duplicate_check(identifier_id)
    if not payload_data:
        raise HTTPException(status_code=404, detail="project not found")

    review_result = bid_document_review_service.check_project_documents(
        project_identifier=identifier_id,
        project=payload_data["project"],
        document_records=payload_data["documents"],
        document_types=document_types,
    )
    db_service.upsert_project_result_item(
        result_key="bid_document_review",
        result_value=review_result,
    )
    return review_result


@router.post("/projects", summary="创建项目")
async def create_project(
    payload: ProjectCreateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.create_project(payload.identifier_id)
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="项目标识已存在") from exc
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/projects", summary="查询项目列表")
async def list_projects(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.list_projects(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/projects/{identifier_id}", summary="查询项目详情")
async def get_project_detail(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        detail = db_service.get_project_detail(identifier_id)
        if not detail:
            raise HTTPException(status_code=404, detail="项目不存在")
        return detail
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/duplicate-check", summary="项目商务标/技术标查重")
async def project_duplicate_check(
    identifier_id: str = Query(..., description="选择需要执行查重的项目"),
    document_scope: DuplicateCheckScope = Query(
        default=DuplicateCheckScope.ALL,
        description="查重范围：business_bid=商务标，technical_bid=技术标，all=全部",
    ),
    max_evidence_sections: int = Query(
        default=5,
        ge=1,
        le=20,
        description="每组最多返回的证据章节数。",
    ),
    max_pairs_per_type: int = Query(
        default=0,
        ge=0,
        le=500,
        description="每类文档最多返回的对比对数，0 表示不截断。",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    try:
        return _run_project_duplicate_check(
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/bid-document-review", summary="Project bid document review")
async def project_bid_document_review(
    identifier_id: str = Query(..., description="Select the project to review business/technical bid documents."),
    document_scope: DuplicateCheckScope = Query(
        default=DuplicateCheckScope.ALL,
        description="Review scope: business_bid, technical_bid, or all.",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_bid_document_review(
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post(
    "/projects/technical-bid-review",
    summary="Legacy project bid document review",
    include_in_schema=False,
)
async def project_technical_bid_review_legacy(
    identifier_id: str = Query(..., description="Select the project to review business/technical bid documents."),
    document_scope: DuplicateCheckScope = Query(
        default=DuplicateCheckScope.ALL,
        description="Review scope: business_bid, technical_bid, or all.",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_bid_document_review(
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post(
    "/projects/{identifier_id}/duplicate-check",
    summary="Project business/technical duplicate check",
    include_in_schema=False,
)
async def project_duplicate_check_legacy(
    identifier_id: str,
    payload: Optional[ProjectDuplicateCheckRequest] = None,
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    request_payload = payload or ProjectDuplicateCheckRequest()
    try:
        return _run_project_duplicate_check(
            identifier_id=identifier_id,
            document_types=request_payload.document_types,
            max_evidence_sections=request_payload.max_evidence_sections,
            max_pairs_per_type=request_payload.max_pairs_per_type,
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/projects/{identifier_id}", summary="更新项目标识")
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
            raise HTTPException(status_code=404, detail="项目不存在")
        return updated
    except HTTPException:
        raise
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="项目标识已存在") from exc
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/projects/{identifier_id}", summary="删除项目")
async def delete_project(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.soft_delete_project(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="项目不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/{identifier_id}/bind-documents", summary="绑定招标/商务标/技术标文件")
async def bind_project_documents(
    identifier_id: str,
    payload: ProjectBindDocumentsRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.bind_project_documents(
            identifier_id,
            payload.tender_document_identifier,
            payload.business_bid_document_identifier,
            payload.technical_bid_document_identifier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/relations/{relation_id}", summary="查询关联详情")
async def get_relation_detail(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        relation = db_service.get_relation_by_id(relation_id)
        if not relation:
            raise HTTPException(status_code=404, detail="关联不存在")
        return relation
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/relations/{relation_id}", summary="更新关联")
async def update_relation(
    relation_id: int,
    payload: ProjectRelationUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        updated = db_service.update_relation(
            relation_id=relation_id,
            tender_document_identifier=payload.tender_document_identifier,
            business_bid_document_identifier=payload.business_bid_document_identifier,
            technical_bid_document_identifier=payload.technical_bid_document_identifier,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="关联不存在")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/relations/{relation_id}", summary="删除关联")
async def delete_relation(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.delete_relation(relation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="关联不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/documents", summary="上传并创建文档")
async def create_document(
    file: UploadFile = File(...),
    document_type: DocumentType = Form(...),
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


@router.get("/documents", summary="查询文档列表")
async def list_documents(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        return db_service.list_documents(limit=limit, offset=offset)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/documents/{identifier_id}", summary="查询文档")
async def get_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="文档不存在")
        return document
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/documents/{identifier_id}", summary="更新文档")
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
            raise HTTPException(status_code=404, detail="文档不存在")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/documents/{identifier_id}", summary="删除文档")
async def delete_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    try:
        deleted = db_service.soft_delete_document(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="文档不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc
