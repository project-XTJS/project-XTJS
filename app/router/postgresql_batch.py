# -*- coding: utf-8 -*-
"""
项目批量上传与手动 OCR 路由。

提供项目创建上传、分阶段 OCR 触发等接口，
包含串行处理、项目绑定、商务阶段自动审查等逻辑。
"""

import asyncio
import logging
from typing import Callable, Optional

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
    get_db_service,
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
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.analysis.duplicate_merge import build_duplicate_merge_results
from app.service.document_ingest_service import (
    upload_and_create_document_without_ocr,
    recognize_existing_document,
)
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService
from app.service.project_runtime import (
    ProjectTaskCancelledError,
    check_project_cancelled,
    ensure_project_cancel_event,
    register_project_task,
    unregister_project_task,
)

router = APIRouter()
logger = logging.getLogger(__name__)
_OCR_TASK_LOCK: Optional[asyncio.Lock] = None
_PROJECT_OCR_QUEUE_LOCK: Optional[asyncio.Lock] = None
# 记录每个项目当前队列尾任务，保证同一项目的新请求接在旧请求后面。
_PROJECT_OCR_QUEUE_TAILS: dict[str, asyncio.Task] = {}

_OCR_STAGE_TENDER = "tender"
_OCR_STAGE_BUSINESS = "business"
_OCR_STAGE_TECHNICAL = "technical"
_OCR_STAGE_SEQUENCE = (
    _OCR_STAGE_TENDER,
    _OCR_STAGE_BUSINESS,
    _OCR_STAGE_TECHNICAL,
)
_OCR_STAGE_LABELS = {
    _OCR_STAGE_TENDER: "招标文件",
    _OCR_STAGE_BUSINESS: "商务标",
    _OCR_STAGE_TECHNICAL: "技术标",
}
_OCR_STAGE_REQUIRED_STATUS = {
    _OCR_STAGE_TENDER: PostgreSQLService.PARSING_STATUS_TENDER_OCR_COMPLETED,
    _OCR_STAGE_BUSINESS: PostgreSQLService.PARSING_STATUS_BUSINESS_OCR_COMPLETED,
    _OCR_STAGE_TECHNICAL: PostgreSQLService.PARSING_STATUS_TECHNICAL_OCR_COMPLETED,
}

# 从配置读取批处理数量限制，兼容新旧字段名
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


def _get_ocr_task_lock() -> asyncio.Lock:
    """返回全局 OCR 串行锁，避免多个异步任务同时压到同一个 OCR 运行时。"""
    global _OCR_TASK_LOCK
    if _OCR_TASK_LOCK is None:
        _OCR_TASK_LOCK = asyncio.Lock()
    return _OCR_TASK_LOCK


def _get_project_ocr_queue_lock() -> asyncio.Lock:
    """项目级队列锁，用于串起同一项目的阶段任务。"""
    global _PROJECT_OCR_QUEUE_LOCK
    if _PROJECT_OCR_QUEUE_LOCK is None:
        _PROJECT_OCR_QUEUE_LOCK = asyncio.Lock()
    return _PROJECT_OCR_QUEUE_LOCK


def _collect_tender_documents(payload: dict) -> list[dict]:
    """从项目关系中收集唯一的招标文件列表。"""
    documents: list[dict] = []
    seen: set[str] = set()
    for record in payload.get("documents") or []:
        identifier = str(record.get("tender_identifier_id") or "").strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        documents.append(
            {
                "identifier_id": identifier,
                "file_name": record.get("tender_file_name"),
                "extracted": bool(record.get("tender_extracted")),
            }
        )
    return documents


def _collect_business_documents(payload: dict) -> list[dict]:
    """从项目关系中收集唯一的商务标文件列表。"""
    documents: list[dict] = []
    seen: set[str] = set()
    for record in payload.get("documents") or []:
        if str(record.get("relation_role") or "").strip() != DOCUMENT_TYPE_BUSINESS_BID:
            continue
        identifier = str(record.get("identifier_id") or "").strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        documents.append(
            {
                "identifier_id": identifier,
                "file_name": record.get("file_name"),
                "extracted": bool(record.get("extracted")),
            }
        )
    return documents


def _collect_technical_documents(payload: dict) -> list[dict]:
    """从项目关系中收集唯一的技术标文件列表。"""
    documents: list[dict] = []
    seen: set[str] = set()
    for record in payload.get("documents") or []:
        if str(record.get("relation_role") or "").strip() != DOCUMENT_TYPE_TECHNICAL_BID:
            continue
        identifier = str(record.get("identifier_id") or "").strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        documents.append(
            {
                "identifier_id": identifier,
                "file_name": record.get("file_name"),
                "extracted": bool(record.get("extracted")),
            }
        )
    return documents


def _collect_documents_by_stage(payload: dict, stage: str) -> list[dict]:
    """按阶段收集当前项目要做 OCR 的文档。"""
    if stage == _OCR_STAGE_TENDER:
        return _collect_tender_documents(payload)
    if stage == _OCR_STAGE_BUSINESS:
        return _collect_business_documents(payload)
    if stage == _OCR_STAGE_TECHNICAL:
        return _collect_technical_documents(payload)
    raise ValueError(f"unsupported ocr stage: {stage}")


def _pending_documents(documents: list[dict]) -> list[dict]:
    # 只把尚未 extracted 的文档继续送去 OCR。
    return [item for item in documents if not item.get("extracted")]


def _planned_ocr_stages(payload: dict, current_status: int, target_stage: str) -> list[dict]:
    """根据当前状态计算本次请求需要排队的阶段。"""
    planned: list[dict] = []
    target_status = _OCR_STAGE_REQUIRED_STATUS[target_stage]
    for stage in _OCR_STAGE_SEQUENCE:
        stage_status = _OCR_STAGE_REQUIRED_STATUS[stage]
        if stage_status <= current_status or stage_status > target_status:
            continue
        documents = _collect_documents_by_stage(payload, stage)
        if not documents:
            # 自动补前置阶段只补“已上传且已绑定”的文档，不会凭空补缺文件。
            raise HTTPException(
                status_code=409,
                detail=f"当前项目未绑定{_OCR_STAGE_LABELS[stage]}，无法继续执行 OCR。",
            )
        pending = _pending_documents(documents)
        if pending:
            planned.append(
                {
                    "stage": stage,
                    "label": _OCR_STAGE_LABELS[stage],
                    "documents": documents,
                    "pending_documents": pending,
                }
            )
    return planned


def _next_stage_to_run(current_status: int, target_stage: str) -> str | None:
    # 总是返回“目标阶段之前第一个尚未完成的阶段”。
    target_status = _OCR_STAGE_REQUIRED_STATUS[target_stage]
    for stage in _OCR_STAGE_SEQUENCE:
        stage_status = _OCR_STAGE_REQUIRED_STATUS[stage]
        if stage_status > current_status and stage_status <= target_status:
            return stage
    return None


def _format_stage_labels(stages: list[dict]) -> str:
    return " -> ".join(item["label"] for item in stages)


def _project_identifier_from_payload(payload: dict, fallback: str) -> str:
    project = payload.get("project") or {}
    return str(project.get("identifier_id") or fallback)


def _normalize_resume_target_stage(target_stage: str) -> str:
    raw_stage = str(target_stage or "").strip().lower()
    aliases = {
        "tender": _OCR_STAGE_TENDER,
        "招标": _OCR_STAGE_TENDER,
        "招标文件": _OCR_STAGE_TENDER,
        "business": _OCR_STAGE_BUSINESS,
        "business_bid": _OCR_STAGE_BUSINESS,
        "商务": _OCR_STAGE_BUSINESS,
        "商务标": _OCR_STAGE_BUSINESS,
        "technical": _OCR_STAGE_TECHNICAL,
        "technical_bid": _OCR_STAGE_TECHNICAL,
        "技术": _OCR_STAGE_TECHNICAL,
        "技术标": _OCR_STAGE_TECHNICAL,
    }
    normalized = aliases.get(raw_stage)
    if normalized:
        return normalized
    raise HTTPException(
        status_code=400,
        detail="target_stage 只能是 tender/business/technical，或 招标文件/商务标/技术标。",
    )


def _ocr_stage_progress(payload: dict) -> list[dict]:
    """汇总项目各 OCR 阶段的断点状态。"""
    progress: list[dict] = []
    for stage in _OCR_STAGE_SEQUENCE:
        documents = _collect_documents_by_stage(payload, stage)
        pending = _pending_documents(documents)
        completed = [item for item in documents if item.get("extracted")]
        progress.append(
            {
                "stage": stage,
                "label": _OCR_STAGE_LABELS[stage],
                "required_parsing_status": _OCR_STAGE_REQUIRED_STATUS[stage],
                "total_count": len(documents),
                "completed_count": len(completed),
                "pending_count": len(pending),
                "completed_documents": completed,
                "pending_documents": pending,
            }
        )
    return progress


def _ocr_progress_totals(stage_progress: list[dict]) -> dict:
    total_count = sum(int(item.get("total_count") or 0) for item in stage_progress)
    completed_count = sum(int(item.get("completed_count") or 0) for item in stage_progress)
    pending_count = sum(int(item.get("pending_count") or 0) for item in stage_progress)
    return {
        "total_count": total_count,
        "completed_count": completed_count,
        "pending_count": pending_count,
    }


def _planned_completed_count(planned_stages: list[dict]) -> int:
    return sum(
        max(0, len(item.get("documents") or []) - len(item.get("pending_documents") or []))
        for item in planned_stages
    )


async def _run_project_ocr_task(
    *,
    identifier_id: str,
    documents: list[dict],
    ocr_type: str,
    cancel_check: Callable[[], None],
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
) -> None:
    """后台串行执行指定类型 OCR，结束后刷新项目总状态。"""
    try:
        cancel_check()
        async with _get_ocr_task_lock():
            cancel_check()
            logger.info(
                "project ocr task started identifier=%s ocr_type=%s pending_count=%s",
                identifier_id,
                ocr_type,
                len(documents),
            )
            items = await _recognize_existing_documents_batch(
                documents=documents,
                parallelism=1,
                project_identifier=identifier_id,
                cancel_check=cancel_check,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
            )
        cancel_check()
        summary = _summarize_batch_items(items)
        refreshed_project = await run_in_threadpool(
            db_service.refresh_project_parsing_status,
            identifier_id,
        )
        logger.info(
            "project ocr task finished identifier=%s ocr_type=%s success=%s failed=%s parsing_status=%s",
            identifier_id,
            ocr_type,
            summary.get("success"),
            summary.get("failed"),
            (refreshed_project or {}).get("parsing_status"),
        )
    except asyncio.CancelledError:
        logger.info(
            "project ocr task cancelled identifier=%s ocr_type=%s",
            identifier_id,
            ocr_type,
        )
        raise
    except ProjectTaskCancelledError:
        logger.info(
            "project ocr task aborted due to project deletion identifier=%s ocr_type=%s",
            identifier_id,
            ocr_type,
        )
        raise
    except Exception:
        logger.exception(
            "project ocr task failed identifier=%s ocr_type=%s",
            identifier_id,
            ocr_type,
        )


async def _run_project_ocr_pipeline(
    *,
    previous_task: asyncio.Task | None,
    identifier_id: str,
    target_stage: str,
    cancel_check: Callable[[], None],
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
) -> None:
    """同一项目的 OCR 请求按阶段顺序串起来执行。"""
    async def _run_pipeline_impl() -> None:
        if previous_task is not None:
            try:
                cancel_check()
                await previous_task
            except Exception:
                logger.exception("previous project ocr task failed identifier=%s", identifier_id)

        while True:
            cancel_check()
            refreshed_project = await run_in_threadpool(
                db_service.refresh_project_parsing_status,
                identifier_id,
            )
            if not refreshed_project:
                logger.warning("project ocr pipeline aborted because project disappeared identifier=%s", identifier_id)
                return

            current_status = int(refreshed_project.get("parsing_status") or 0)
            target_status = _OCR_STAGE_REQUIRED_STATUS[target_stage]
            if current_status >= target_status:
                logger.info(
                    "project ocr pipeline completed identifier=%s target_stage=%s parsing_status=%s",
                    identifier_id,
                    target_stage,
                    current_status,
                )
                return

            payload = await run_in_threadpool(
                db_service.get_project_documents_for_duplicate_check,
                identifier_id,
            )
            if not payload:
                logger.warning("project ocr pipeline missing payload identifier=%s", identifier_id)
                return

            stage = _next_stage_to_run(current_status, target_stage)
            if stage is None:
                return
            documents = _collect_documents_by_stage(payload, stage)
            if not documents:
                logger.warning(
                    "project ocr pipeline missing stage documents identifier=%s stage=%s",
                    identifier_id,
                    stage,
                )
                return

            pending_documents = _pending_documents(documents)
            if not pending_documents:
                refreshed_project = await run_in_threadpool(
                    db_service.refresh_project_parsing_status,
                    identifier_id,
                )
                if not PostgreSQLService.parsing_status_reached(
                    (refreshed_project or {}).get("parsing_status"),
                    _OCR_STAGE_REQUIRED_STATUS[stage],
                ):
                    logger.warning(
                        "project ocr pipeline paused identifier=%s stage=%s reason=status_not_advanced",
                        identifier_id,
                        stage,
                    )
                    return
                continue

            await _run_project_ocr_task(
                identifier_id=identifier_id,
                documents=pending_documents,
                ocr_type=_OCR_STAGE_LABELS[stage],
                cancel_check=cancel_check,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
            )

            cancel_check()
            refreshed_project = await run_in_threadpool(
                db_service.refresh_project_parsing_status,
                identifier_id,
            )
            if not PostgreSQLService.parsing_status_reached(
                (refreshed_project or {}).get("parsing_status"),
                _OCR_STAGE_REQUIRED_STATUS[stage],
            ):
                logger.warning(
                    "project ocr pipeline paused identifier=%s stage=%s parsing_status=%s",
                    identifier_id,
                    stage,
                    (refreshed_project or {}).get("parsing_status"),
                )
                return

    try:
        return await _run_pipeline_impl()
    except asyncio.CancelledError:
        logger.info(
            "project ocr pipeline cancelled identifier=%s target_stage=%s",
            identifier_id,
            target_stage,
        )
        raise
    except ProjectTaskCancelledError:
        logger.info(
            "project ocr pipeline aborted due to project deletion identifier=%s target_stage=%s",
            identifier_id,
            target_stage,
        )
        return

    if previous_task is not None:
        try:
            # 同项目存在前序任务时，先等前序任务完成再继续。
            await previous_task
        except Exception:
            logger.exception("previous project ocr task failed identifier=%s", identifier_id)

    while True:
        refreshed_project = await run_in_threadpool(
            db_service.refresh_project_parsing_status,
            identifier_id,
        )
        if not refreshed_project:
            logger.warning("project ocr pipeline aborted because project disappeared identifier=%s", identifier_id)
            return

        current_status = int(refreshed_project.get("parsing_status") or 0)
        target_status = _OCR_STAGE_REQUIRED_STATUS[target_stage]
        if current_status >= target_status:
            logger.info(
                "project ocr pipeline completed identifier=%s target_stage=%s parsing_status=%s",
                identifier_id,
                target_stage,
                current_status,
            )
            return

        payload = await run_in_threadpool(
            db_service.get_project_documents_for_duplicate_check,
            identifier_id,
        )
        if not payload:
            logger.warning("project ocr pipeline missing payload identifier=%s", identifier_id)
            return

        stage = _next_stage_to_run(current_status, target_stage)
        if stage is None:
            return
        documents = _collect_documents_by_stage(payload, stage)
        if not documents:
            logger.warning(
                "project ocr pipeline missing stage documents identifier=%s stage=%s",
                identifier_id,
                stage,
            )
            return

        pending_documents = _pending_documents(documents)
        if not pending_documents:
            # 文档虽然都已抽取，但项目状态还没推进时，说明数据不一致，先停下来等人工处理。
            refreshed_project = await run_in_threadpool(
                db_service.refresh_project_parsing_status,
                identifier_id,
            )
            if not PostgreSQLService.parsing_status_reached(
                (refreshed_project or {}).get("parsing_status"),
                _OCR_STAGE_REQUIRED_STATUS[stage],
            ):
                logger.warning(
                    "project ocr pipeline paused identifier=%s stage=%s reason=status_not_advanced",
                    identifier_id,
                    stage,
                )
                return
            continue

        await _run_project_ocr_task(
            identifier_id=identifier_id,
            documents=pending_documents,
            ocr_type=_OCR_STAGE_LABELS[stage],
            db_service=db_service,
            oss_service=oss_service,
            analysis_service=analysis_service,
        )

        refreshed_project = await run_in_threadpool(
            db_service.refresh_project_parsing_status,
            identifier_id,
        )
        if not PostgreSQLService.parsing_status_reached(
            (refreshed_project or {}).get("parsing_status"),
            _OCR_STAGE_REQUIRED_STATUS[stage],
        ):
            logger.warning(
                "project ocr pipeline paused identifier=%s stage=%s parsing_status=%s",
                identifier_id,
                stage,
                (refreshed_project or {}).get("parsing_status"),
            )
            return


async def _enqueue_project_ocr_pipeline(
    *,
    identifier_id: str,
    target_stage: str,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
) -> None:
    """给项目追加一个 OCR 请求，后来的请求会接在前一个请求后面。"""
    async with _get_project_ocr_queue_lock():
        previous_task = _PROJECT_OCR_QUEUE_TAILS.get(identifier_id)
        cancel_event = ensure_project_cancel_event(identifier_id)
        cancel_check = lambda: check_project_cancelled(cancel_event, identifier_id=identifier_id)
        # 新任务直接挂到当前队尾，形成项目内串行链。
        task = asyncio.create_task(
            _run_project_ocr_pipeline(
                previous_task=previous_task,
                identifier_id=identifier_id,
                target_stage=target_stage,
                cancel_check=cancel_check,
                db_service=db_service,
                oss_service=oss_service,
                analysis_service=analysis_service,
            )
        )
        register_project_task(identifier_id, task)
        _PROJECT_OCR_QUEUE_TAILS[identifier_id] = task

    def _cleanup_queue_tail(finished_task: asyncio.Task) -> None:
        async def _cleanup() -> None:
            async with _get_project_ocr_queue_lock():
                current_task = _PROJECT_OCR_QUEUE_TAILS.get(identifier_id)
                # 只有自己还是队尾时才清掉，避免误删后来追加的新任务。
                if current_task is finished_task:
                    _PROJECT_OCR_QUEUE_TAILS.pop(identifier_id, None)
            unregister_project_task(identifier_id, finished_task)

        asyncio.create_task(_cleanup())

    task.add_done_callback(_cleanup_queue_tail)


async def _build_async_ocr_response(
    *,
    identifier_id: str,
    payload: dict,
    target_stage: str,
    ocr_type: str,
    endpoint_name: str,
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
) -> dict:
    """统一处理单类文档 OCR 的排队响应。"""
    refreshed_project = await run_in_threadpool(
        db_service.refresh_project_parsing_status,
        identifier_id,
    )
    project = refreshed_project or (payload.get("project") or {})
    current_status = int(project.get("parsing_status") or 0)
    stage_progress = _ocr_stage_progress(payload)
    # 这里会把缺失但已绑定的前置阶段一并规划进队列。
    planned_stages = _planned_ocr_stages(payload, current_status, target_stage)
    if not planned_stages:
        refreshed_project = await run_in_threadpool(
            db_service.refresh_project_parsing_status,
            identifier_id,
        )
        return {
            "status": "success",
            "mode": "async",
            "message": f"{ocr_type}所需阶段已全部完成 OCR，无需重复触发。",
            "project_identifier": identifier_id,
            "project": refreshed_project or (payload.get("project") or {}),
            "queued_count": 0,
            "skipped_count": _ocr_progress_totals(stage_progress)["completed_count"],
            "ocr_type": ocr_type,
            "endpoint": endpoint_name,
            "queued_stages": [],
            "ocr_progress": {
                "totals": _ocr_progress_totals(stage_progress),
                "stages": stage_progress,
            },
        }

    await _enqueue_project_ocr_pipeline(
        identifier_id=identifier_id,
        target_stage=target_stage,
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
    )
    return {
        "status": "accepted",
        "mode": "async",
        "message": (
            f"{ocr_type} OCR 已加入项目队列，系统会按阶段顺序执行："
            f"{_format_stage_labels(planned_stages)}。"
        ),
        "project_identifier": identifier_id,
        "project": project,
        "queued_count": sum(len(item["pending_documents"]) for item in planned_stages),
        "skipped_count": _planned_completed_count(planned_stages),
        "ocr_type": ocr_type,
        "endpoint": endpoint_name,
        "queued_stages": [item["stage"] for item in planned_stages],
        "ocr_progress": {
            "totals": _ocr_progress_totals(stage_progress),
            "stages": stage_progress,
        },
    }


# 批量上传文件（不执行 OCR）
async def _upload_batch_documents_without_ocr(
    *,
    files: list[UploadFile],
    document_type: str,
    role_label: str,
    parallelism: int,
    db_service: PostgreSQLService,
    oss_service: MinioService,
) -> list[dict]:
    """批量上传文件至 MinIO 并创建文档记录，但不触发 OCR 提取。"""
    normalized_files = [upload for upload in (files or []) if upload is not None]
    if not normalized_files:
        return []

    # 为了让项目创建链路更稳定，这里也统一按串行顺序上传。
    _ = parallelism
    items: list[dict] = []
    for index, upload in enumerate(normalized_files, start=1):
        file_name = (upload.filename or "").strip() or f"{role_label}_{index}"
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
            items.append({
                "index": index,
                "file_name": file_name,
                "status": "failed",
                "stage": f"{role_label}_upload",
                "error": result["error"],
                "status_code": result["status_code"],
            })
            continue
        items.append({
            "index": index,
            "file_name": file_name,
            "status": "success",
            "document": result["document_summary"],
            "document_identifier": result["document"]["identifier_id"],
            "upload": result["upload"],
        })

    return items


# 批量处理结果汇总
def _summarize_batch_items(items: list[dict]) -> dict:
    """根据条目列表统计成功/失败数量，返回带整体状态的汇总字典。"""
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


# 持久化项目分析结果
def _persist_result_item(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    result_key: str,
    result_value: dict,
) -> dict:
    """将单个分析结果写入项目结果存储。"""
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
    """将雷同检查的原始结果拆分为多个合并项并分别持久化。"""
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


# 路由：上传 OCR JSON 并执行商务标形式审查
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
    project_name: Optional[str] = Form(
        default=None,
        description="项目名称；不传时自动生成临时项目名",
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
    """上传招投标 OCR JSON 文件，创建项目并执行商务标形式审查。"""
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
            project_name=project_name,
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


# 路由：创建项目并上传全部文件后启动商务阶段 OCR 及审查
@router.post("/projects/batch/ingest-recognize", summary="创建项目并上传全部文件（后续 OCR 手动触发）")
async def ingest_and_recognize_project_documents(
    project_name: str = Form(..., description="项目名称；UUID 由系统自动生成"),
    tender_file: UploadFile = File(..., description="招标文件"),
    business_bid_files: list[UploadFile] = File(
        ...,
        description="商务标文件列表；与 technical_bid_files 按顺序一一对应",
    ),
    technical_bid_files: list[UploadFile] = File(
        ...,
        description="技术标文件列表；本接口仅上传入库，不自动执行 OCR",
    ),
    bid_group_parallelism: int = Form(default=1, ge=1, le=16),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """创建项目并上传全部文件，后续 OCR 由人工按阶段手动触发。"""
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

    tender_result = await upload_and_create_document_without_ocr(
        file=tender_file,
        document_type=DOCUMENT_TYPE_TENDER,
        db_service=db_service,
        oss_service=oss_service,
        raise_http_exception=True,
    )
    tender_document_identifier = tender_result["document"]["identifier_id"]

    # 当前统一退回串行，保留参数仅为兼容前端已有表单。
    effective_parallelism = 1
    # 先顺序上传商务标，再上传技术标，避免项目创建阶段再引入额外并发。
    business_results = await _upload_batch_documents_without_ocr(
        files=normalized_business_files,
        document_type=DOCUMENT_TYPE_BUSINESS_BID,
        role_label="business_bid",
        parallelism=effective_parallelism,
        db_service=db_service,
        oss_service=oss_service,
    )
    technical_results = await _upload_batch_documents_without_ocr(
        files=normalized_technical_files,
        document_type=DOCUMENT_TYPE_TECHNICAL_BID,
        role_label="technical_bid",
        parallelism=effective_parallelism,
        db_service=db_service,
        oss_service=oss_service,
    )

    # 绑定商务标与技术标文档关系
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

    # 只要形成了有效绑定，就先标记为“未全部完成 OCR”，待后续全部完成后再刷新为 1。
    if any(item.get("status") == "success" for item in binding_items):
        refreshed_project = await run_in_threadpool(
            db_service.update_project_parsing_status,
            project["identifier_id"],
            PostgreSQLService.PARSING_STATUS_UPLOADED,
        )
        if refreshed_project:
            project = refreshed_project

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
            "status": "uploaded",
            "document_identifier": tender_document_identifier,
            "document": tender_result["document_summary"],
            "upload": tender_result["upload"],
        },
        "business_bid_documents": {
            **business_summary,
            "note": "已上传入库，尚未执行 OCR。",
        },
        "technical_bid_documents": {
            **technical_summary,
            "note": "已上传入库，尚未执行 OCR。",
        },
        "bindings": binding_summary,
        "ocr_actions": {
            "run_tender_ocr_endpoint": f"/api/postgresql/projects/{project['identifier_id']}/run-tender-ocr",
            "run_business_ocr_endpoint": f"/api/postgresql/projects/{project['identifier_id']}/run-business-ocr",
            "run_technical_ocr_endpoint": f"/api/postgresql/projects/{project['identifier_id']}/continue-technical-ocr",
            "parsing_status": project.get("parsing_status"),
            "parsing_status_label": project.get("parsing_status_label"),
        },
    }

async def _recognize_existing_documents_batch(
    *,
    documents: list[dict],
    parallelism: int,
    project_identifier: str,
    cancel_check: Callable[[], None],
    db_service: PostgreSQLService,
    oss_service: MinioService,
    analysis_service,
) -> list[dict]:
    # 手动 OCR 路由共用的“对已有文档补做 OCR”批处理器。
    normalized_documents = [item for item in (documents or []) if item]
    if not normalized_documents:
        return []

    # OCR 共享同一个 OCRService / pdfium 运行时，当前固定串行执行更稳定。
    _ = parallelism
    items: list[dict] = []
    for document_meta in normalized_documents:
        cancel_check()
        result = await recognize_existing_document(
            document_identifier=document_meta["identifier_id"],
            project_identifier=project_identifier,
            cancel_check=cancel_check,
            db_service=db_service,
            oss_service=oss_service,
            analysis_service=analysis_service,
            raise_http_exception=False,
        )
        cancel_check()
        if not result["ok"]:
            logger.error(
                "manual recognize existing document failed identifier=%s file_name=%s status_code=%s error=%s",
                document_meta["identifier_id"],
                document_meta.get("file_name"),
                result["status_code"],
                result["error"],
            )
            items.append({
                "identifier_id": document_meta["identifier_id"],
                "file_name": document_meta.get("file_name"),
                "status": "failed",
                "error": result["error"],
                "status_code": result["status_code"],
            })
            continue
        items.append({
            "identifier_id": document_meta["identifier_id"],
            "file_name": document_meta.get("file_name"),
            "status": "success",
            "document": result["document_summary"],
        })

    return items


def _build_project_ocr_status_response(
    *,
    payload: dict,
    project_identifier: str,
) -> dict:
    project = payload.get("project") or {}
    stage_progress = _ocr_stage_progress(payload)
    active_task = _PROJECT_OCR_QUEUE_TAILS.get(project_identifier)
    return {
        "status": "success",
        "mode": "checkpoint",
        "project_identifier": project_identifier,
        "project": project,
        "is_queued": bool(active_task and not active_task.done()),
        "parsing_status": project.get("parsing_status"),
        "parsing_status_label": project.get("parsing_status_label"),
        "ocr_progress": {
            "totals": _ocr_progress_totals(stage_progress),
            "stages": stage_progress,
        },
        "resume_endpoint": f"/api/postgresql/projects/{project_identifier}/resume-ocr",
    }


@router.get("/projects/{identifier_id}/ocr-status", summary="查询项目 OCR 断点状态")
async def get_project_ocr_status(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """查询项目下各阶段文档 OCR 完成/待处理数量。"""
    refreshed_project = await run_in_threadpool(
        db_service.refresh_project_parsing_status,
        identifier_id,
    )
    if not refreshed_project:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")

    project_identifier = str(refreshed_project["identifier_id"])
    payload = await run_in_threadpool(
        db_service.get_project_documents_for_duplicate_check,
        project_identifier,
    )
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")
    return _build_project_ocr_status_response(
        payload=payload,
        project_identifier=project_identifier,
    )


@router.post("/projects/{identifier_id}/resume-ocr", summary="恢复项目 OCR（跳过已完成文档）")
async def resume_project_ocr(
    identifier_id: str,
    parallelism: int = Form(default=1, ge=1, le=16),
    target_stage: str = Form(
        default=_OCR_STAGE_TECHNICAL,
        description="恢复目标阶段：tender/business/technical，或 招标文件/商务标/技术标",
    ),
    analysis_service=Depends(get_text_analysis_service),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """恢复项目 OCR，按招标文件、商务标、技术标顺序只处理未 extracted 的文档。"""
    _ = parallelism
    payload = await run_in_threadpool(db_service.get_project_documents_for_duplicate_check, identifier_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")
    project_identifier = _project_identifier_from_payload(payload, identifier_id)
    normalized_target_stage = _normalize_resume_target_stage(target_stage)
    return await _build_async_ocr_response(
        identifier_id=project_identifier,
        payload=payload,
        target_stage=normalized_target_stage,
        ocr_type=f"项目{_OCR_STAGE_LABELS[normalized_target_stage]}",
        endpoint_name=f"/api/postgresql/projects/{project_identifier}/resume-ocr",
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
    )


@router.post("/projects/{identifier_id}/run-tender-ocr", summary="异步执行项目招标文件 OCR")
async def run_project_tender_ocr(
    identifier_id: str,
    parallelism: int = Form(default=1, ge=1, le=16),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """手动触发招标文件 OCR，接口会立即返回，后台按串行队列执行。"""
    _ = parallelism
    payload = await run_in_threadpool(db_service.get_project_documents_for_duplicate_check, identifier_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")
    project_identifier = _project_identifier_from_payload(payload, identifier_id)
    return await _build_async_ocr_response(
        identifier_id=project_identifier,
        payload=payload,
        target_stage=_OCR_STAGE_TENDER,
        ocr_type="招标文件",
        endpoint_name=f"/api/postgresql/projects/{project_identifier}/run-tender-ocr",
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=None,
    )


@router.post("/projects/{identifier_id}/run-business-ocr", summary="异步执行项目商务标 OCR")
async def run_project_business_ocr(
    identifier_id: str,
    parallelism: int = Form(default=1, ge=1, le=16),
    analysis_service=Depends(get_text_analysis_service),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """手动触发商务标 OCR，接口会立即返回，后台按串行队列执行。"""
    _ = parallelism
    payload = await run_in_threadpool(db_service.get_project_documents_for_duplicate_check, identifier_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")
    project_identifier = _project_identifier_from_payload(payload, identifier_id)
    return await _build_async_ocr_response(
        identifier_id=project_identifier,
        payload=payload,
        target_stage=_OCR_STAGE_BUSINESS,
        ocr_type="商务标",
        endpoint_name=f"/api/postgresql/projects/{project_identifier}/run-business-ocr",
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
    )


@router.post("/projects/{identifier_id}/continue-technical-ocr", summary="异步执行项目技术标 OCR")
async def continue_project_technical_ocr(
    identifier_id: str,
    parallelism: int = Form(default=1, ge=1, le=16),
    analysis_service=Depends(get_text_analysis_service),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """手动触发技术标 OCR，接口会立即返回，后台按串行队列执行。"""
    _ = parallelism
    payload = await run_in_threadpool(db_service.get_project_documents_for_duplicate_check, identifier_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"项目不存在：{identifier_id}")
    project_identifier = _project_identifier_from_payload(payload, identifier_id)
    return await _build_async_ocr_response(
        identifier_id=project_identifier,
        payload=payload,
        target_stage=_OCR_STAGE_TECHNICAL,
        ocr_type="技术标",
        endpoint_name=f"/api/postgresql/projects/{project_identifier}/continue-technical-ocr",
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
    )
