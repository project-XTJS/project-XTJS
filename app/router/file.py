"""文件路由：负责 MinIO 文件上传与删除。"""

import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from psycopg2 import Error as PsycopgError

from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()
minio_service = MinioService()
postgres_service = PostgreSQLService()
logger = logging.getLogger(__name__)


def _rollback_uploaded_object(upload_result: Optional[dict]) -> Optional[str]:
    if not upload_result:
        return None
    object_name = upload_result.get("object_name")
    if not object_name:
        return None
    try:
        minio_service.delete_file(object_name)
        return None
    except Exception as cleanup_exc:  # pragma: no cover - defensive rollback
        logger.exception("MinIO 上传后回滚对象失败: object_name=%s", object_name)
        return str(cleanup_exc)


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
            "msg": "上传成功",
            "data": upload_result,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 上传接口发生未预期异常")
        raise HTTPException(status_code=500, detail="文件上传服务异常，请稍后重试。") from exc


@router.post("/upload-and-register", summary="上传文件并写入 PostgreSQL 文档记录")
async def upload_and_register_file(
    file: UploadFile = File(...),
    object_name: Optional[str] = Form(default=None),
    identifier_id: Optional[str] = Form(default=None),
    document_name: Optional[str] = Form(default=None),
):
    """
    先上传文件到 MinIO，再将文档元数据写入 PostgreSQL。
    若入库失败，会回滚已上传对象。
    """
    upload_result: Optional[dict] = None
    try:
        upload_result = minio_service.upload_file(file, object_name)
        resolved_file_name = (
            (document_name or "").strip()
            or (file.filename or "").strip()
            or upload_result.get("object_name", "")
        )
        if not resolved_file_name:
            raise ValueError("document_name cannot be empty")

        document = postgres_service.create_document(
            file_name=resolved_file_name,
            file_url=upload_result["file_url"],
            identifier_id=identifier_id,
        )
        return {
            "code": 200,
            "msg": "upload and register success",
            "data": {
                "upload": upload_result,
                "document": document,
            },
        }
    except ValueError as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        if rollback_error:
            raise HTTPException(
                status_code=400,
                detail=f"{exc}; rollback uploaded object failed: {rollback_error}",
            ) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        detail = f"database error: {exc}"
        if rollback_error:
            detail = f"{detail}; rollback uploaded object failed: {rollback_error}"
        raise HTTPException(status_code=500, detail=detail) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        rollback_error = _rollback_uploaded_object(upload_result)
        logger.exception("上传并入库接口发生未预期异常")
        detail = "上传并入库失败，请稍后重试。"
        if rollback_error:
            detail = f"{detail} 回滚失败: {rollback_error}"
        raise HTTPException(status_code=500, detail=detail) from exc


@router.get("/objects/{object_name:path}/presigned-url", summary="生成 MinIO 临时访问链接")
async def get_presigned_url(object_name: str):
    """按对象名生成预签名访问 URL。"""
    try:
        presigned_url = minio_service.get_presigned_url(object_name)
        return {
            "code": 200,
            "msg": "获取成功",
            "data": {"object_name": object_name, "presigned_url": presigned_url},
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 预签名链接接口发生未预期异常")
        raise HTTPException(status_code=500, detail="获取临时访问链接失败，请稍后重试。") from exc


@router.delete("/objects/{object_name:path}", summary="删除 MinIO 文件")
async def delete_file(object_name: str):
    """按对象名删除 MinIO 文件。"""
    try:
        minio_service.delete_file(object_name)
        return {"code": 200, "msg": "删除成功"}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 删除接口发生未预期异常")
        raise HTTPException(status_code=500, detail="文件删除服务异常，请稍后重试。") from exc
