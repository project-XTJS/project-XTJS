"""文件路由：负责 MinIO 文件上传与删除。"""

import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.service.minio_service import MinioService

router = APIRouter()
minio_service = MinioService()
logger = logging.getLogger(__name__)


@router.post("/upload", summary="MinIO 文件上传")
async def upload_file(
    file: UploadFile = File(...),
    object_name: Optional[str] = Form(default=None),
):
    """
    上传文件到 MinIO。
    """
    try:
        upload_result = minio_service.upload_file(file, object_name)
        return {
            "code": 200,
            "msg": "upload success",
            "data": upload_result,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 上传接口发生未预期异常")
        raise HTTPException(status_code=500, detail="文件上传服务异常，请稍后重试。") from exc


@router.delete("/objects/{object_name:path}", summary="删除 MinIO 文件")
async def delete_file(object_name: str):
    """按对象名删除 MinIO 文件。"""
    try:
        minio_service.delete_file(object_name)
        return {"code": 200, "msg": "delete success"}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 删除接口发生未预期异常")
        raise HTTPException(status_code=500, detail="文件删除服务异常，请稍后重试。") from exc
