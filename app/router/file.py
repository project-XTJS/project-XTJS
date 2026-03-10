"""文件路由：统一上传、删除与文档记录维护。"""

from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from psycopg2 import Error as PsycopgError

from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()
minio_service = MinioService()
postgres_service = PostgreSQLService()


@router.post("/upload", summary="统一文件上传接口")
async def upload_file(
    file: UploadFile = File(...),
    object_name: Optional[str] = Form(default=None),
    document_identifier: Optional[str] = Form(default=None),
):
    """
    统一文件上传：上传到 MinIO，同时写入 xtjs_documents。
    """
    upload_result = None
    try:
        # 先上传对象存储，获得可访问 URL 与对象名。
        upload_result = minio_service.upload_file(file, object_name)
        # 再落库记录，形成业务可追踪的文档元数据。
        document = postgres_service.create_document(
            file_name=file.filename,
            file_url=upload_result["file_url"],
            identifier_id=document_identifier,
        )
        return {
            "code": 0,
            "msg": "upload success",
            "data": {
                "document": document,
                "object_name": upload_result["object_name"],
                "size": upload_result["size"],
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        # DB 写入失败时执行补偿删除，避免 MinIO 孤儿文件。
        if upload_result:
            try:
                minio_service.delete_file(upload_result["object_name"])
            except RuntimeError:
                # 补偿失败不覆盖原异常，交由上层日志体系感知。
                pass
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{document_identifier}", summary="删除文件")
async def delete_file(document_identifier: str):
    """根据文档标识删除 MinIO 对象并逻辑删除数据库记录。"""
    try:
        document = postgres_service.get_document_by_identifier(document_identifier)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")

        # 文档记录中保存的是预签名 URL，这里反解为对象名。
        object_name = minio_service.object_name_from_presigned_url(document["file_url"])
        minio_service.delete_file(object_name)
        postgres_service.soft_delete_document(document_identifier)
        return {"code": 0, "msg": "delete success"}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
