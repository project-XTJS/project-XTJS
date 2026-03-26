"""项目批量识别：1 份招标文件加 N 组商务标/技术标文件。"""

import asyncio
from typing import Any, Optional

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
    RecognitionOptions,
    get_db_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.service.document_ingest_service import upload_extract_and_create_document
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()

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


async def _ensure_batch_project(
    db_service: PostgreSQLService,
    project_identifier: Optional[str],
) -> tuple[dict[str, Any], bool]:
    normalized_identifier = (project_identifier or "").strip()
    if normalized_identifier:
        existing = await run_in_threadpool(
            db_service.get_project_by_identifier,
            normalized_identifier,
        )
        if existing:
            return existing, False
        try:
            created = await run_in_threadpool(
                db_service.create_project,
                normalized_identifier,
            )
            return created, True
        except PsycopgError as exc:
            if getattr(exc, "pgcode", None) == "23505":
                existing = await run_in_threadpool(
                    db_service.get_project_by_identifier,
                    normalized_identifier,
                )
                if existing:
                    return existing, False
            raise

    created = await run_in_threadpool(db_service.create_project)
    return created, True


@router.post("/projects/batch/recognize", summary="项目批量识别")
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
                f"(商务标={business_count}，技术标={technical_count})"
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
        project, project_created = await _ensure_batch_project(db_service, project_identifier)
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
    ) -> dict[str, Any]:
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
