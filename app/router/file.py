"""文件对象路由：负责 MinIO 对象链接与删除。"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from app.service.minio_service import MinioService
from app.router.dependencies import get_oss_service

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/objects/{object_name:path}/presigned-url", summary="生成 MinIO 临时访问链接")
async def get_presigned_url(
    object_name: str,
    oss_service: MinioService = Depends(get_oss_service)
):
    """按对象名生成预签名访问 URL。"""
    try:
        presigned_url = oss_service.get_presigned_url(object_name)
        return {"object_name": object_name, "presigned_url": presigned_url}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 预签名链接接口发生未预期异常")
        raise HTTPException(status_code=500, detail="获取临时访问链接失败，请稍后重试。") from exc

@router.delete("/objects/{object_name:path}", summary="删除 MinIO 文件")
async def delete_file(
    object_name: str,
    oss_service: MinioService = Depends(get_oss_service)
):
    """按对象名删除 MinIO 文件。"""
    try:
        oss_service.delete_file(object_name)
        return {"status": "deleted"}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("MinIO 删除接口发生未预期异常")
        raise HTTPException(status_code=500, detail="文件删除服务异常，请稍后重试。") from exc