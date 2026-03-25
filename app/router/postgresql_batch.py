"""Batch project recognition routes."""

import asyncio
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

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

PROJECT_BATCH_MIN_BID_FILES = 5
PROJECT_BATCH_MAX_BID_FILES = 30


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


@router.post("/projects/batch/recognize", summary="Batch recognize project (1 tender + 5-30 bids)")
async def batch_recognize_project_documents(
    tender_file: UploadFile = File(...),
    bid_files: list[UploadFile] = File(...),
    project_identifier: Optional[str] = Form(default=None),
    bid_parallelism: int = Form(default=4, ge=1, le=16),
    recognition_options: RecognitionOptions = Depends(get_form_recognition_options),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
    analysis_service=Depends(get_text_analysis_service),
):
    bid_count = len(bid_files or [])
    if bid_count < PROJECT_BATCH_MIN_BID_FILES or bid_count > PROJECT_BATCH_MAX_BID_FILES:
        raise HTTPException(
            status_code=400,
            detail=(
                "bid_files count must be between "
                f"{PROJECT_BATCH_MIN_BID_FILES} and {PROJECT_BATCH_MAX_BID_FILES}"
            ),
        )

    try:
        project, project_created = await _ensure_batch_project(db_service, project_identifier)
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc

    tender_result = await upload_extract_and_create_document(
        file=tender_file,
        document_type="tender",
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
        **recognition_options.as_kwargs(),
        raise_http_exception=True,
    )
    tender_document_identifier = tender_result["document"]["identifier_id"]

    effective_parallelism = max(1, min(int(bid_parallelism), bid_count))
    semaphore = asyncio.Semaphore(effective_parallelism)

    async def _handle_single_bid(index: int, bid_file: UploadFile) -> dict[str, Any]:
        file_name = (bid_file.filename or "").strip() or f"bid_{index}"
        async with semaphore:
            bid_result = await upload_extract_and_create_document(
                file=bid_file,
                document_type="bid",
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
                **recognition_options.as_kwargs(),
                raise_http_exception=False,
            )

        if not bid_result["ok"]:
            return {
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": "recognition",
                "error": bid_result["error"],
                "status_code": bid_result["status_code"],
            }

        bid_document = bid_result["document"]
        try:
            relation = await run_in_threadpool(
                db_service.bind_project_documents,
                project["identifier_id"],
                tender_document_identifier,
                bid_document["identifier_id"],
            )
        except ValueError as exc:
            return {
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": "binding",
                "error": str(exc),
                "status_code": 400,
                "document": bid_result["document_summary"],
                "upload": bid_result["upload"],
            }
        except PsycopgError as exc:
            return {
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": "binding",
                "error": f"database error: {exc}",
                "status_code": 500,
                "document": bid_result["document_summary"],
                "upload": bid_result["upload"],
            }

        return {
            "index": index,
            "file_name": file_name,
            "status": "success",
            "document": bid_result["document_summary"],
            "upload": bid_result["upload"],
            "relation": relation,
        }

    bid_items = await asyncio.gather(
        *(
            _handle_single_bid(index, bid_file)
            for index, bid_file in enumerate(bid_files, start=1)
        )
    )
    success_count = sum(1 for item in bid_items if item.get("status") == "success")
    failed_count = bid_count - success_count
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
            "requested_bid_parallelism": bid_parallelism,
            "effective_bid_parallelism": effective_parallelism,
        },
        "tender": {
            "status": "success",
            "document": tender_result["document_summary"],
            "upload": tender_result["upload"],
        },
        "bids": {
            "total": bid_count,
            "success": success_count,
            "failed": failed_count,
            "items": bid_items,
        },
    }
