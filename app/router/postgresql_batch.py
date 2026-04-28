"""项目批量识别与上传 JSON 商务标审查路由。"""

import asyncio
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
