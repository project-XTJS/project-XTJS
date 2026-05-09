"""项目批量识别与上传 JSON 商务标审查路由。"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.config.settings import settings
from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DOCUMENT_TYPE_TENDER,
)
from app.router.dependencies import (
    get_duplicate_check_service,
    RecognitionOptions,
    get_db_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.router.uploaded_json_support import (
    ensure_upload_project,
    load_uploaded_bid_json_documents,
    parse_optional_string_array_json,
    persist_uploaded_json_project_documents,
    read_uploaded_json_file,
)
from app.service.analysis.unified_business_review import UnifiedBusinessReviewService
from app.service.analysis import DuplicateCheckService
from app.service.analysis.duplicate_merge import build_duplicate_merge_results
from app.service.document_ingest_service import (
    upload_and_create_document_without_ocr,
    upload_extract_and_create_document,
    recognize_existing_document,
)
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()
logger = logging.getLogger(__name__)

PROJECT_BATCH_MIN_BID_GROUPS = max(
    1,
    int(
        getattr(
            settings,
            "PROJECT_BATCH_MIN_BID_GROUPS",
            getattr(settings, "PROJECT_BATCH_MIN_BID_FILES", 1),
        )
    ),
)
PROJECT_BATCH_MAX_BID_GROUPS = int(
    getattr(
        settings,
        "PROJECT_BATCH_MAX_BID_GROUPS",
        getattr(settings, "PROJECT_BATCH_MAX_BID_FILES", 0),
    )
)


async def _recognize_batch_documents(
    *,
    files: list[UploadFile],
    document_type: str,
    role_label: str,
    parallelism: int,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
    recognition_kwargs: dict,
) -> list[dict]:
    normalized_files = [upload for upload in (files or []) if upload is not None]
    if not normalized_files:
        return []

    semaphore = asyncio.Semaphore(max(1, parallelism))

    async def _handle_single(index: int, upload: UploadFile) -> dict:
        file_name = (upload.filename or "").strip() or f"{role_label}_{index}"
        async with semaphore:
            result = await upload_extract_and_create_document(
                file=upload,
                document_type=document_type,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
                **recognition_kwargs,
                raise_http_exception=False,
            )
        if not result["ok"]:
            logger.error(
                "batch recognize file failed index=%s role=%s file_name=%s status_code=%s error=%s",
                index,
                role_label,
                file_name,
                result["status_code"],
                result["error"],
            )
            return {
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": f"{role_label}_recognition",
                "error": result["error"],
                "status_code": result["status_code"],
            }
        return {
            "index": index,
            "file_name": file_name,
            "status": "success",
            "document": result["document_summary"],
            "document_identifier": result["document"]["identifier_id"],
            "upload": result["upload"],
        }

    return await asyncio.gather(
        *(_handle_single(index, upload) for index, upload in enumerate(normalized_files, start=1))
    )


async def _upload_batch_documents_without_ocr(
    *,
    files: list[UploadFile],
    document_type: str,
    role_label: str,
    parallelism: int,
    db_service: PostgreSQLService,
    oss_service: MinioService,
) -> list[dict]:
    normalized_files = [upload for upload in (files or []) if upload is not None]
    if not normalized_files:
        return []

    semaphore = asyncio.Semaphore(max(1, parallelism))

    async def _handle_single(index: int, upload: UploadFile) -> dict:
        file_name = (upload.filename or "").strip() or f"{role_label}_{index}"
        async with semaphore:
            result = await upload_and_create_document_without_ocr(
                file=upload,
                document_type=document_type,
                db_service=db_service,
                oss_service=oss_service,
                raise_http_exception=False,
            )
        if not result["ok"]:
            logger.error(
                "batch upload file failed index=%s role=%s file_name=%s status_code=%s error=%s",
                index,
                role_label,
                file_name,
                result["status_code"],
                result["error"],
            )
            return {
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": f"{role_label}_upload",
                "error": result["error"],
                "status_code": result["status_code"],
            }
        return {
            "index": index,
            "file_name": file_name,
            "status": "success",
            "document": result["document_summary"],
            "document_identifier": result["document"]["identifier_id"],
            "upload": result["upload"],
        }

    return await asyncio.gather(
        *(_handle_single(index, upload) for index, upload in enumerate(normalized_files, start=1))
    )


def _summarize_batch_items(items: list[dict]) -> dict:
    total = len(items)
    success = sum(1 for item in items if item.get("status") == "success")
    failed = total - success
    if failed == 0:
        status = "success"
    elif success == 0:
        status = "failed"
    else:
        status = "partial_success"
    return {
        "status": status,
        "total": total,
        "success": success,
        "failed": failed,
        "items": items,
    }


def _persist_result_item(
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


def _persist_merge_result_items(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    source_result_key: str,
    raw_result: dict,
) -> dict[str, dict]:
    merged_results = build_duplicate_merge_results(
        raw_result=raw_result,
        source_result_key=source_result_key,
    )
    for result_key, result_value in merged_results.items():
        db_service.upsert_project_result_item(
            project_identifier_id=project_identifier,
            result_key=result_key,
            result_value=result_value,
        )
    return merged_results


@router.post(
    "/projects/business-bid-format-review/upload-json",
    summary="上传 OCR JSON 并执行商务标形式审查",
)
async def upload_business_bid_format_review(
    tender_json_file: UploadFile = File(..., description="招标文件 OCR JSON"),
    business_bid_json_files: list[UploadFile] = File(
        ...,
        description="一个或多个商务标 OCR JSON 文件",
    ),
    technical_bid_json_files: Optional[list[UploadFile]] = File(
        default=None,
        description="可选的技术标 OCR JSON 文件，按上传顺序与商务标对齐以便绑定项目关系",
    ),
    project_identifier: Optional[str] = Form(
        default=None,
        description="可选项目标识；不传时自动创建",
    ),
    result_key: str = Form(
        default=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
        description="写入 xtjs_result.result 的结果键名",
    ),
    bidder_keys_json: Optional[str] = Form(
        default=None,
        description="可选 JSON 数组，需与 business_bid_json_files 一一对应",
    ),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    uploads = [upload for upload in business_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="business_bid_json_files 不能为空。")

    tender_document = await read_uploaded_json_file(
        tender_json_file,
        field_name="tender_json_file",
    )
    provided_bidder_keys = parse_optional_string_array_json(
        bidder_keys_json,
        field_name="bidder_keys_json",
        expected_length=len(uploads),
    )
    business_documents = await load_uploaded_bid_json_documents(
        uploads,
        field_name="business_bid_json_files",
        role=DOCUMENT_TYPE_BUSINESS_BID,
        provided_bidder_keys=provided_bidder_keys,
    )
    technical_documents = await load_uploaded_bid_json_documents(
        technical_bid_json_files,
        field_name="technical_bid_json_files",
        role=DOCUMENT_TYPE_TECHNICAL_BID,
    )

    try:
        persisted_documents = await persist_uploaded_json_project_documents(
            db_service=db_service,
            tender_document=tender_document,
            business_bid_documents=business_documents,
            technical_bid_documents=technical_documents,
            project_identifier=project_identifier,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]

        review_service = UnifiedBusinessReviewService(db_service=db_service)
        response = await run_in_threadpool(
            review_service.persist_uploaded_business_review,
            tender_file_name=tender_document["file_name"],
            tender_payload=tender_document["payload"],
            tender_raw_bytes=tender_document["raw_bytes"],
            business_bid_documents=business_documents,
            project_identifier=resolved_project_identifier,
            result_key=result_key,
        )
        response["document_binding"] = persisted_documents["binding"]
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/batch/recognize", summary="批量识别项目文档")
async def batch_recognize_project_documents(
    tender_file: UploadFile = File(...),
    business_bid_files: list[UploadFile] = File(...),
    technical_bid_files: list[UploadFile] = File(...),
    project_identifier: Optional[str] = Form(default=None),
    bid_group_parallelism: int = Form(default=4, ge=1, le=16),
    recognition_options: RecognitionOptions = Depends(get_form_recognition_options),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
    analysis_service=Depends(get_text_analysis_service),
):
    business_count = len(business_bid_files or [])
    technical_count = len(technical_bid_files or [])
    if business_count != technical_count:
        raise HTTPException(
            status_code=400,
            detail=(
                "business_bid_files 数量必须与 technical_bid_files 数量一致 "
                f"(business_bid={business_count}, technical_bid={technical_count})"
            ),
        )

    bid_group_count = business_count
    if bid_group_count < PROJECT_BATCH_MIN_BID_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"标书组数量不能少于 {PROJECT_BATCH_MIN_BID_GROUPS}",
        )
    if PROJECT_BATCH_MAX_BID_GROUPS > 0 and bid_group_count > PROJECT_BATCH_MAX_BID_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"标书组数量不能超过 {PROJECT_BATCH_MAX_BID_GROUPS}",
        )

    try:
        project, project_created = await ensure_upload_project(db_service, project_identifier)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc

    tender_result = await upload_extract_and_create_document(
        file=tender_file,
        document_type=DOCUMENT_TYPE_TENDER,
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
        **recognition_options.as_kwargs(),
        raise_http_exception=True,
    )
    tender_document_identifier = tender_result["document"]["identifier_id"]

    effective_parallelism = max(1, min(int(bid_group_parallelism), bid_group_count))
    semaphore = asyncio.Semaphore(effective_parallelism)

    async def _handle_single_bid_group(
        index: int,
        business_bid_file: UploadFile,
        technical_bid_file: UploadFile,
    ) -> dict:
        business_file_name = (business_bid_file.filename or "").strip() or f"business_bid_{index}"
        technical_file_name = (technical_bid_file.filename or "").strip() or f"technical_bid_{index}"

        async with semaphore:
            business_result = await upload_extract_and_create_document(
                file=business_bid_file,
                document_type=DOCUMENT_TYPE_BUSINESS_BID,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
                **recognition_options.as_kwargs(),
                raise_http_exception=False,
            )
            if not business_result["ok"]:
                return {
                    "index": index,
                    "business_bid_file_name": business_file_name,
                    "technical_bid_file_name": technical_file_name,
                    "status": "failed",
                    "stage": "business_bid_recognition",
                    "error": business_result["error"],
                    "status_code": business_result["status_code"],
                }

            technical_result = await upload_extract_and_create_document(
                file=technical_bid_file,
                document_type=DOCUMENT_TYPE_TECHNICAL_BID,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
                **recognition_options.as_kwargs(),
                raise_http_exception=False,
            )
            if not technical_result["ok"]:
                return {
                    "index": index,
                    "business_bid_file_name": business_file_name,
                    "technical_bid_file_name": technical_file_name,
                    "status": "failed",
                    "stage": "technical_bid_recognition",
                    "error": technical_result["error"],
                    "status_code": technical_result["status_code"],
                    "business_bid_document": business_result["document_summary"],
                    "business_bid_upload": business_result["upload"],
                }

        business_bid_document = business_result["document"]
        technical_bid_document = technical_result["document"]
        try:
            relation = await run_in_threadpool(
                db_service.bind_project_documents,
                project["identifier_id"],
                tender_document_identifier,
                business_bid_document["identifier_id"],
                technical_bid_document["identifier_id"],
            )
        except ValueError as exc:
            return {
                "index": index,
                "business_bid_file_name": business_file_name,
                "technical_bid_file_name": technical_file_name,
                "status": "failed",
                "stage": "binding",
                "error": str(exc),
                "status_code": 400,
                "business_bid_document": business_result["document_summary"],
                "business_bid_upload": business_result["upload"],
                "technical_bid_document": technical_result["document_summary"],
                "technical_bid_upload": technical_result["upload"],
            }
        except PsycopgError as exc:
            return {
                "index": index,
                "business_bid_file_name": business_file_name,
                "technical_bid_file_name": technical_file_name,
                "status": "failed",
                "stage": "binding",
                "error": f"数据库错误：{exc}",
                "status_code": 500,
                "business_bid_document": business_result["document_summary"],
                "business_bid_upload": business_result["upload"],
                "technical_bid_document": technical_result["document_summary"],
                "technical_bid_upload": technical_result["upload"],
            }

        return {
            "index": index,
            "business_bid_file_name": business_file_name,
            "technical_bid_file_name": technical_file_name,
            "status": "success",
            "business_bid_document": business_result["document_summary"],
            "business_bid_upload": business_result["upload"],
            "technical_bid_document": technical_result["document_summary"],
            "technical_bid_upload": technical_result["upload"],
            "relation": relation,
        }

    bid_group_items = await asyncio.gather(
        *(
            _handle_single_bid_group(index, business_bid_file, technical_bid_file)
            for index, (business_bid_file, technical_bid_file) in enumerate(
                zip(business_bid_files, technical_bid_files),
                start=1,
            )
        )
    )
    success_count = sum(1 for item in bid_group_items if item.get("status") == "success")
    failed_count = bid_group_count - success_count
    if failed_count == 0:
        batch_status = "success"
    elif success_count == 0:
        batch_status = "failed"
    else:
        batch_status = "partial_success"

    return {
        "status": batch_status,
        "project": project,
        "project_created": project_created,
        "parallel": {
            "requested_bid_group_parallelism": bid_group_parallelism,
            "effective_bid_group_parallelism": effective_parallelism,
        },
        "tender": {
            "status": "success",
            "document": tender_result["document_summary"],
            "upload": tender_result["upload"],
        },
        "bid_groups": {
            "total": bid_group_count,
            "success": success_count,
            "failed": failed_count,
            "items": bid_group_items,
        },
    }


@router.post("/projects/batch/ingest-recognize", summary="创建项目并上传全部文件后启动商务阶段 OCR")
async def ingest_and_recognize_project_documents(
    project_name: str = Form(..., description="项目名称；当前作为项目标识使用"),
    tender_file: UploadFile = File(..., description="招标文件"),
    business_bid_files: list[UploadFile] = File(
        ...,
        description="商务标文件列表；与 technical_bid_files 按顺序一一对应",
    ),
    technical_bid_files: list[UploadFile] = File(
        ...,
        description="技术标文件列表；本次先上传入库，后续再继续 OCR",
    ),
    bid_group_parallelism: int = Form(default=4, ge=1, le=16),
    recognition_options: RecognitionOptions = Depends(get_form_recognition_options),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
    analysis_service=Depends(get_text_analysis_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    normalized_project_name = (project_name or "").strip()
    if not normalized_project_name:
        raise HTTPException(status_code=400, detail="project_name 不能为空。")

    normalized_business_files = [upload for upload in (business_bid_files or []) if upload is not None]
    normalized_technical_files = [upload for upload in (technical_bid_files or []) if upload is not None]
    if not normalized_business_files or not normalized_technical_files:
        raise HTTPException(
            status_code=400,
            detail="business_bid_files 和 technical_bid_files 必须同时上传，且不能为空。",
        )
    if len(normalized_business_files) != len(normalized_technical_files):
        raise HTTPException(
            status_code=400,
            detail="business_bid_files 数量必须与 technical_bid_files 数量一致。",
        )

    bid_group_count = len(normalized_business_files)
    if PROJECT_BATCH_MIN_BID_GROUPS > 0 and bid_group_count < PROJECT_BATCH_MIN_BID_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"标书组数量不能少于 {PROJECT_BATCH_MIN_BID_GROUPS}。",
        )
    if PROJECT_BATCH_MAX_BID_GROUPS > 0 and bid_group_count > PROJECT_BATCH_MAX_BID_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"标书组数量不能超过 {PROJECT_BATCH_MAX_BID_GROUPS}。",
        )

    try:
        project, project_created = await ensure_upload_project(db_service, normalized_project_name)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc

    recognition_kwargs = recognition_options.as_kwargs()
    tender_result = await upload_extract_and_create_document(
        file=tender_file,
        document_type=DOCUMENT_TYPE_TENDER,
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
        **recognition_kwargs,
        raise_http_exception=True,
    )
    tender_document_identifier = tender_result["document"]["identifier_id"]

    effective_parallelism = max(1, min(int(bid_group_parallelism), bid_group_count))
    business_results, technical_results = await asyncio.gather(
        _recognize_batch_documents(
            files=normalized_business_files,
            document_type=DOCUMENT_TYPE_BUSINESS_BID,
            role_label="business_bid",
            parallelism=effective_parallelism,
            db_service=db_service,
            oss_service=oss_service,
            analysis_service=analysis_service,
            recognition_kwargs=recognition_kwargs,
        ),
        _upload_batch_documents_without_ocr(
            files=normalized_technical_files,
            document_type=DOCUMENT_TYPE_TECHNICAL_BID,
            role_label="technical_bid",
            parallelism=effective_parallelism,
            db_service=db_service,
            oss_service=oss_service,
        ),
    )

    binding_items: list[dict] = []
    for business_item, technical_item in zip(business_results, technical_results):
        index = int(business_item.get("index") or technical_item.get("index") or 0)
        if business_item.get("status") != "success":
            binding_items.append(
                {
                    "index": index,
                    "mode": "paired_upload_pending_technical_ocr",
                    "status": "failed",
                    "stage": business_item.get("stage"),
                    "error": business_item.get("error"),
                    "status_code": business_item.get("status_code"),
                }
            )
            continue
        if technical_item.get("status") != "success":
            binding_items.append(
                {
                    "index": index,
                    "mode": "paired_upload_pending_technical_ocr",
                    "status": "failed",
                    "stage": technical_item.get("stage"),
                    "error": technical_item.get("error"),
                    "status_code": technical_item.get("status_code"),
                    "business_bid_document_identifier": business_item.get("document_identifier"),
                }
            )
            continue

        try:
            relation = await run_in_threadpool(
                db_service.bind_project_documents,
                project["identifier_id"],
                tender_document_identifier,
                str(business_item["document_identifier"]),
                str(technical_item["document_identifier"]),
            )
            binding_items.append(
                {
                    "index": index,
                    "mode": "paired_upload_pending_technical_ocr",
                    "status": "success",
                    "relation": relation,
                    "business_bid_document_identifier": business_item.get("document_identifier"),
                    "technical_bid_document_identifier": technical_item.get("document_identifier"),
                }
            )
        except ValueError as exc:
            binding_items.append(
                {
                    "index": index,
                    "mode": "paired_upload_pending_technical_ocr",
                    "status": "failed",
                    "stage": "binding",
                    "error": str(exc),
                    "status_code": 400,
                    "business_bid_document_identifier": business_item.get("document_identifier"),
                    "technical_bid_document_identifier": technical_item.get("document_identifier"),
                }
            )
        except PsycopgError as exc:
            binding_items.append(
                {
                    "index": index,
                    "mode": "paired_upload_pending_technical_ocr",
                    "status": "failed",
                    "stage": "binding",
                    "error": f"数据库错误：{exc}",
                    "status_code": 500,
                    "business_bid_document_identifier": business_item.get("document_identifier"),
                    "technical_bid_document_identifier": technical_item.get("document_identifier"),
                }
            )

    business_summary = _summarize_batch_items(business_results)
    technical_summary = _summarize_batch_items(technical_results)
    binding_summary = _summarize_batch_items(binding_items)

    successful_binding_count = sum(1 for item in binding_items if item.get("status") == "success")
    business_phase: dict = {
        "status": "pending",
        "completed_steps": [
            "tender_ocr",
            "business_bid_ocr",
        ],
        "pending_steps": [
            "business_bid_format_review",
            "business_bid_duplicate_check",
            "technical_bid_ocr",
            "technical_bid_duplicate_check",
            "bid_document_review",
            "typo_check",
            "personnel_reuse_check",
        ],
    }
    if successful_binding_count > 0:
        try:
            business_review_service = UnifiedBusinessReviewService(db_service=db_service)
            business_review_response = await run_in_threadpool(
                business_review_service.persist_project_business_review,
                project_identifier=project["identifier_id"],
                result_key=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
            )
            duplicate_payload = await run_in_threadpool(
                db_service.get_project_documents_for_duplicate_check,
                project["identifier_id"],
            )
            business_duplicate_result = await run_in_threadpool(
                duplicate_check_service.check_project_documents,
                project_identifier=project["identifier_id"],
                project=(duplicate_payload or {}).get("project") or {"identifier_id": project["identifier_id"]},
                document_records=(duplicate_payload or {}).get("documents") or [],
                document_types=[DOCUMENT_TYPE_BUSINESS_BID],
                max_evidence_sections=5,
                max_pairs_per_type=0,
            )
            business_duplicate_record = await run_in_threadpool(
                _persist_result_item,
                db_service=db_service,
                project_identifier=project["identifier_id"],
                result_key="business_bid_duplicate_check",
                result_value=business_duplicate_result,
            )
            business_duplicate_merged_results = await run_in_threadpool(
                _persist_merge_result_items,
                db_service=db_service,
                project_identifier=project["identifier_id"],
                source_result_key="business_bid_duplicate_check",
                raw_result=business_duplicate_result,
            )
            business_phase = {
                "status": "completed",
                "completed_steps": [
                    "tender_ocr",
                    "business_bid_ocr",
                    "business_bid_format_review",
                    "business_bid_duplicate_check",
                ],
                "pending_steps": [
                    "technical_bid_ocr",
                    "technical_bid_duplicate_check",
                    "bid_document_review",
                    "typo_check",
                    "personnel_reuse_check",
                ],
                "format_review": {
                    "result_key": UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
                    "overview": business_review_response.get("overview"),
                    "result_record": business_review_response.get("result_record"),
                },
                "duplicate_check": {
                    "result_key": "business_bid_duplicate_check",
                    "result": business_duplicate_result,
                    "result_record": business_duplicate_record,
                    "merged_results": business_duplicate_merged_results,
                },
            }
        except (ValueError, PsycopgError) as exc:
            business_phase = {
                **business_phase,
                "status": "failed",
                "error": str(exc),
            }
    else:
        business_phase = {
            **business_phase,
            "status": "failed",
            "error": "未形成有效的项目绑定关系，商务阶段审查未启动。",
        }

    status_items = ["success"]
    status_items.extend(item.get("status") or "failed" for item in business_results)
    status_items.extend(item.get("status") or "failed" for item in technical_results)
    status_items.extend(item.get("status") or "failed" for item in binding_items)
    if all(item == "success" for item in status_items):
        batch_status = "success"
    elif any(item == "success" for item in status_items) and any(item == "failed" for item in status_items):
        batch_status = "partial_success"
    else:
        batch_status = "failed"

    return {
        "status": batch_status,
        "project": project,
        "project_created": project_created,
        "project_name": normalized_project_name,
        "parallel": {
            "requested_bid_group_parallelism": bid_group_parallelism,
            "effective_bid_group_parallelism": effective_parallelism,
        },
        "tender": {
            "status": "ocr_completed",
            "document_identifier": tender_document_identifier,
            "document": tender_result["document_summary"],
            "upload": tender_result["upload"],
        },
        "business_bid_documents": business_summary,
        "technical_bid_documents": {
            **technical_summary,
            "note": "已上传入库，当前未执行 OCR。",
        },
        "bindings": binding_summary,
        "business_phase": business_phase,
    }


@router.post("/projects/{identifier_id}/continue-technical-ocr", summary="继续执行项目技术标 OCR")
async def continue_project_technical_ocr(
    identifier_id: str,
    parallelism: int = Form(default=4, ge=1, le=16),
    analysis_service=Depends(get_text_analysis_service),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    payload = await run_in_threadpool(db_service.get_project_documents_for_duplicate_check, identifier_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")

    pending_technical_documents: list[dict] = []
    seen: set[str] = set()
    for record in payload.get("documents") or []:
        if str(record.get("document_type") or "").strip() != DOCUMENT_TYPE_TECHNICAL_BID:
            continue
        document_identifier = str(record.get("identifier_id") or "").strip()
        if not document_identifier or document_identifier in seen:
            continue
        seen.add(document_identifier)
        if bool(record.get("extracted")):
            continue
        pending_technical_documents.append(
            {
                "identifier_id": document_identifier,
                "file_name": record.get("file_name"),
            }
        )

    if not pending_technical_documents:
        return {
            "status": "success",
            "project_identifier": identifier_id,
            "summary": {
                "pending_count": 0,
                "success_count": 0,
                "failed_count": 0,
            },
            "items": [],
            "next_steps": [
                "technical_bid_duplicate_check",
                "bid_document_review",
                "typo_check",
                "personnel_reuse_check",
            ],
        }

    semaphore = asyncio.Semaphore(max(1, min(int(parallelism), len(pending_technical_documents))))

    async def _handle_single(document_meta: dict) -> dict:
        async with semaphore:
            result = await recognize_existing_document(
                document_identifier=document_meta["identifier_id"],
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
                raise_http_exception=False,
            )
        if not result["ok"]:
            return {
                "identifier_id": document_meta["identifier_id"],
                "file_name": document_meta.get("file_name"),
                "status": "failed",
                "error": result["error"],
                "status_code": result["status_code"],
            }
        return {
            "identifier_id": document_meta["identifier_id"],
            "file_name": document_meta.get("file_name"),
            "status": "success",
            "document": result["document_summary"],
        }

    items = await asyncio.gather(*(_handle_single(item) for item in pending_technical_documents))
    success_count = sum(1 for item in items if item.get("status") == "success")
    failed_count = len(items) - success_count
    if failed_count == 0:
        status = "success"
    elif success_count == 0:
        status = "failed"
    else:
        status = "partial_success"

    return {
        "status": status,
        "project_identifier": identifier_id,
        "summary": {
            "pending_count": len(pending_technical_documents),
            "success_count": success_count,
            "failed_count": failed_count,
        },
        "items": items,
        "next_steps": [
            "technical_bid_duplicate_check",
            "bid_document_review",
            "typo_check",
            "personnel_reuse_check",
        ],
    }
