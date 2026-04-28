"""项目与文档 CRUD 路由。"""

from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from psycopg2 import Error as PsycopgError

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DocumentType,
)
from app.router.dependencies import (
    RecognitionOptions,
    get_bid_document_review_service,
    get_db_service,
    get_duplicate_check_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.router.uploaded_json_support import (
    build_uploaded_project_document_records,
    load_uploaded_bid_json_documents,
    persist_uploaded_json_project_documents,
    read_uploaded_json_file,
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
from app.service.analysis.unified_business_review import UnifiedBusinessReviewService
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
    result_key: str,
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
        result_key=result_key,
        result_value=duplicate_result,
    )
    return duplicate_result


def _run_project_bid_document_review(
    *,
    identifier_id: str,
    document_types: Optional[list[str]],
    result_key: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
):
    payload_data = db_service.get_project_documents_for_duplicate_check(identifier_id)
    if not payload_data:
        raise HTTPException(status_code=404, detail="项目不存在")

    review_result = bid_document_review_service.check_project_documents(
        project_identifier=identifier_id,
        project=payload_data["project"],
        document_records=payload_data["documents"],
        document_types=document_types,
    )
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key=result_key,
        result_value=review_result,
    )
    return review_result


def _build_personnel_reuse_result(review_result: dict) -> dict:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_personnel_count = 0
    total_reused_name_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        personnel_reuse_check = group.get("personnel_reuse_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_personnel_count = int(summary.get("personnel_count") or 0)
        group_reused_name_count = int(summary.get("reused_name_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "personnel_reuse_check": personnel_reuse_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "personnel_count": group_personnel_count,
                "reused_name_count": group_reused_name_count,
                "suspicious": bool(group_reused_name_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_personnel_count += group_personnel_count
        total_reused_name_count += group_reused_name_count

    config = review_result.get("config") or {}
    return {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "personnel_reuse_scope": config.get("personnel_reuse_scope"),
            "personnel_table_extraction_engine": config.get("personnel_table_extraction_engine"),
            "personnel_text_extraction_engine": config.get("personnel_text_extraction_engine"),
            "business_bid_personnel_scope": config.get("business_bid_personnel_scope"),
            "technical_bid_personnel_scope": config.get("technical_bid_personnel_scope"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "personnel_count": total_personnel_count,
            "reused_name_count": total_reused_name_count,
            "suspicious": bool(total_reused_name_count),
        },
    }


def _build_typo_check_result(review_result: dict) -> dict:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_typo_issue_count = 0
    total_shared_typo_issue_count = 0
    total_suspicious_typo_document_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        typo_check = group.get("typo_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_typo_issue_count = int(summary.get("typo_issue_count") or 0)
        group_shared_typo_issue_count = int(summary.get("shared_typo_issue_count") or 0)
        group_suspicious_document_count = int(summary.get("suspicious_typo_document_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "typo_check": typo_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "typo_issue_count": group_typo_issue_count,
                "shared_typo_issue_count": group_shared_typo_issue_count,
                "suspicious_typo_document_count": group_suspicious_document_count,
                "suspicious": bool(group_typo_issue_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_typo_issue_count += group_typo_issue_count
        total_shared_typo_issue_count += group_shared_typo_issue_count
        total_suspicious_typo_document_count += group_suspicious_document_count

    config = review_result.get("config") or {}
    result = {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "typo_detection_engine": config.get("typo_detection_engine"),
            "typo_model_name": config.get("typo_model_name"),
            "typo_model_threshold": config.get("typo_model_threshold"),
            "typo_engine_statuses": config.get("typo_engine_statuses") or [],
            "typo_model_load_error": config.get("typo_model_load_error"),
            "typo_stopword_dictionary_enabled": config.get("typo_stopword_dictionary_enabled"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "typo_issue_count": total_typo_issue_count,
            "shared_typo_issue_count": total_shared_typo_issue_count,
            "suspicious_typo_document_count": total_suspicious_typo_document_count,
            "suspicious": bool(total_typo_issue_count),
        },
    }
    return result


def _run_project_personnel_reuse_check(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
) -> dict:
    review_result = _run_project_bid_document_review(
        identifier_id=identifier_id,
        document_types=[DuplicateCheckScope.BUSINESS_BID.value],
        result_key="bid_document_review",
        db_service=db_service,
        bid_document_review_service=bid_document_review_service,
    )
    personnel_result = _build_personnel_reuse_result(review_result)
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key="personnel_reuse_check",
        result_value=personnel_result,
    )
    return personnel_result


def _run_project_typo_check(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
) -> dict:
    review_result = _run_project_bid_document_review(
        identifier_id=identifier_id,
        document_types=None,
        result_key="bid_document_review",
        db_service=db_service,
        bid_document_review_service=bid_document_review_service,
    )
    typo_result = _build_typo_check_result(review_result)
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key="typo_check",
        result_value=typo_result,
    )
    return typo_result


def _project_snapshot(project_identifier: str) -> dict:
    return {"identifier_id": project_identifier}


async def _persist_uploaded_analysis_documents(
    *,
    tender_json_file: UploadFile,
    business_bid_json_files: Optional[list[UploadFile]],
    technical_bid_json_files: Optional[list[UploadFile]],
    project_identifier: Optional[str],
    db_service: PostgreSQLService,
) -> tuple[dict, list[dict]]:
    tender_document = await read_uploaded_json_file(
        tender_json_file,
        field_name="tender_json_file",
    )
    business_documents = await load_uploaded_bid_json_documents(
        business_bid_json_files,
        field_name="business_bid_json_files",
        role=DOCUMENT_TYPE_BUSINESS_BID,
    )
    technical_documents = await load_uploaded_bid_json_documents(
        technical_bid_json_files,
        field_name="technical_bid_json_files",
        role=DOCUMENT_TYPE_TECHNICAL_BID,
    )
    persisted_documents = await persist_uploaded_json_project_documents(
        db_service=db_service,
        tender_document=tender_document,
        business_bid_documents=business_documents,
        technical_bid_documents=technical_documents,
        project_identifier=project_identifier,
    )
    document_records = build_uploaded_project_document_records(persisted_documents)
    return persisted_documents, document_records


def _persist_uploaded_result(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    result_key: str,
    result_value: dict,
) -> dict:
    return db_service.upsert_project_result_item(
        project_identifier_id=project_identifier,
        result_key=result_key,
        result_value=result_value,
    )


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
            result_key="duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/business-bid-format-review", summary="项目商务标形式审查")
async def project_business_bid_format_review(
    identifier_id: str = Query(..., description="选择需要执行商务标形式审查的项目"),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    review_service = UnifiedBusinessReviewService(db_service=db_service)
    try:
        return review_service.persist_project_business_review(
            project_identifier=identifier_id,
            result_key=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
        )
    except ValueError as exc:
        if "project not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail="项目不存在") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/business-bid-duplicate-check", summary="项目商务标内容查重")
async def project_business_bid_duplicate_check(
    identifier_id: str = Query(..., description="选择需要执行商务标内容查重的项目"),
    max_evidence_sections: int = Query(default=5, ge=1, le=20),
    max_pairs_per_type: int = Query(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    try:
        return _run_project_duplicate_check(
            identifier_id=identifier_id,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            result_key="business_bid_duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/business-bid-duplicate-check/upload-json",
    summary="上传 OCR JSON 并执行商务标内容查重",
)
async def upload_business_bid_duplicate_check(
    tender_json_file: UploadFile = File(..., description="招标文件 OCR JSON"),
    business_bid_json_files: list[UploadFile] = File(
        ...,
        description="一个或多个商务标 OCR JSON 文件",
    ),
    technical_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的技术标 OCR JSON 文件，按上传顺序与商务标对齐以便绑定项目关系",
    ),
    project_identifier: Optional[str] = Form(default=None),
    max_evidence_sections: int = Form(default=5, ge=1, le=20),
    max_pairs_per_type: int = Form(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    uploads = [upload for upload in business_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="business_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=uploads,
            technical_bid_json_files=technical_bid_json_files,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        duplicate_result = duplicate_check_service.check_project_documents(
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
        )
        result_record = _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="business_bid_duplicate_check",
            result_value=duplicate_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "business_bid_duplicate_check",
            "result": duplicate_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/technical-bid-duplicate-check", summary="项目技术标内容查重")
async def project_technical_bid_duplicate_check(
    identifier_id: str = Query(..., description="选择需要执行技术标内容查重的项目"),
    max_evidence_sections: int = Query(default=5, ge=1, le=20),
    max_pairs_per_type: int = Query(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    try:
        return _run_project_duplicate_check(
            identifier_id=identifier_id,
            document_types=[DuplicateCheckScope.TECHNICAL_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            result_key="technical_bid_duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/technical-bid-duplicate-check/upload-json",
    summary="上传 OCR JSON 并执行技术标内容查重",
)
async def upload_technical_bid_duplicate_check(
    tender_json_file: UploadFile = File(..., description="招标文件 OCR JSON"),
    technical_bid_json_files: list[UploadFile] = File(
        ...,
        description="一个或多个技术标 OCR JSON 文件",
    ),
    business_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的商务标 OCR JSON 文件，按上传顺序与技术标对齐以便绑定项目关系",
    ),
    project_identifier: Optional[str] = Form(default=None),
    max_evidence_sections: int = Form(default=5, ge=1, le=20),
    max_pairs_per_type: int = Form(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    uploads = [upload for upload in technical_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="technical_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=business_bid_json_files,
            technical_bid_json_files=uploads,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        duplicate_result = duplicate_check_service.check_project_documents(
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.TECHNICAL_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
        )
        result_record = _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="technical_bid_duplicate_check",
            result_value=duplicate_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "technical_bid_duplicate_check",
            "result": duplicate_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/personnel-reuse-check", summary="项目一人多用检查")
async def project_personnel_reuse_check(
    identifier_id: str = Query(..., description="选择需要执行一人多用检查的项目"),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_personnel_reuse_check(
            identifier_id=identifier_id,
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/personnel-reuse-check/upload-json",
    summary="上传 OCR JSON 并执行一人多用检查",
)
async def upload_personnel_reuse_check(
    tender_json_file: UploadFile = File(..., description="招标文件 OCR JSON"),
    business_bid_json_files: list[UploadFile] = File(
        ...,
        description="一个或多个商务标 OCR JSON 文件",
    ),
    technical_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的技术标 OCR JSON 文件，按上传顺序与商务标对齐以便绑定项目关系",
    ),
    project_identifier: Optional[str] = Form(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    uploads = [upload for upload in business_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="business_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=uploads,
            technical_bid_json_files=technical_bid_json_files,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        review_result = bid_document_review_service.check_project_documents(
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
        )
        personnel_result = _build_personnel_reuse_result(review_result)
        result_record = _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="personnel_reuse_check",
            result_value=personnel_result,
        )
        _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="bid_document_review",
            result_value=review_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "personnel_reuse_check",
            "result": personnel_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/typo-check", summary="项目错别字检查")
async def project_typo_check(
    identifier_id: str = Query(..., description="选择需要执行错别字检查的项目"),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_typo_check(
            identifier_id=identifier_id,
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/typo-check/upload-json",
    summary="上传 OCR JSON 并执行错别字检查",
)
async def upload_typo_check(
    tender_json_file: UploadFile = File(..., description="招标文件 OCR JSON"),
    business_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的商务标 OCR JSON 文件",
    ),
    technical_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的技术标 OCR JSON 文件",
    ),
    project_identifier: Optional[str] = Form(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    business_uploads = [upload for upload in (business_bid_json_files or []) if upload is not None]
    technical_uploads = [upload for upload in (technical_bid_json_files or []) if upload is not None]
    if not business_uploads and not technical_uploads:
        raise HTTPException(
            status_code=400,
            detail="business_bid_json_files 和 technical_bid_json_files 至少需要传一个。",
        )

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=business_uploads,
            technical_bid_json_files=technical_uploads,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        review_result = bid_document_review_service.check_project_documents(
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=None,
        )
        typo_result = _build_typo_check_result(review_result)
        result_record = _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="typo_check",
            result_value=typo_result,
        )
        _persist_uploaded_result(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="bid_document_review",
            result_value=review_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "typo_check",
            "result": typo_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/bid-document-review", summary="项目投标文件审查")
async def project_bid_document_review(
    identifier_id: str = Query(..., description="选择需要执行投标文件审查的项目。"),
    document_scope: DuplicateCheckScope = Query(
        default=DuplicateCheckScope.ALL,
        description="审查范围：business_bid=商务标，technical_bid=技术标，all=全部。",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_bid_document_review(
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            result_key="bid_document_review",
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/technical-bid-review",
    summary="旧版项目投标文件审查",
    include_in_schema=False,
)
async def project_technical_bid_review_legacy(
    identifier_id: str = Query(..., description="选择需要执行投标文件审查的项目。"),
    document_scope: DuplicateCheckScope = Query(
        default=DuplicateCheckScope.ALL,
        description="审查范围：business_bid=商务标，technical_bid=技术标，all=全部。",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    try:
        return _run_project_bid_document_review(
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            result_key="bid_document_review",
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post(
    "/projects/{identifier_id}/duplicate-check",
    summary="项目商务标/技术标查重",
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
            result_key="duplicate_check",
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
