"""File object routes for MinIO operations."""

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.router.dependencies import get_oss_service
from app.service.minio_service import MinioService

router = APIRouter()
logger = logging.getLogger(__name__)


def _raise_minio_http_exception(
    exc: Exception,
    *,
    log_message: str,
    generic_detail: str,
) -> None:
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, RuntimeError):
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    logger.exception(log_message)
    raise HTTPException(status_code=500, detail=generic_detail) from exc


@router.get("/objects/{object_name:path}/presigned-url", summary="Generate MinIO presigned URL")
async def get_presigned_url(
    object_name: str,
    oss_service: MinioService = Depends(get_oss_service),
):
    try:
        presigned_url = oss_service.get_presigned_url(object_name)
        return {"object_name": object_name, "presigned_url": presigned_url}
    except Exception as exc:
        _raise_minio_http_exception(
            exc,
            log_message="Unexpected error in get_presigned_url",
            generic_detail="Failed to get presigned URL, please retry later.",
        )


@router.delete("/objects/{object_name:path}", summary="Delete MinIO object")
async def delete_file(
    object_name: str,
    oss_service: MinioService = Depends(get_oss_service),
):
    try:
        oss_service.delete_file(object_name)
        return {"status": "deleted"}
    except Exception as exc:
        _raise_minio_http_exception(
            exc,
            log_message="Unexpected error in delete_file",
            generic_detail="Failed to delete file, please retry later.",
        )
