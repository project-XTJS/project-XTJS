import uuid
import logging
from typing import Any
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)

# 1. 统一响应体包装器
class UnifiedResponse(JSONResponse):
    """
    在 FastAPI 将数据序列化成 JSON 之前，自动套上 {code, message, rid, data} 。
    """
    def render(self, content: Any) -> bytes:
        # 生成请求的唯一 ID
        rid = str(uuid.uuid4())
        
        # 1. 如果接口返回的已经是被包装过的字典（比如抛出特定格式错误），或者不需要包装
        if isinstance(content, dict) and {"code", "message"}.issubset(content.keys()):
            if "rid" not in content:
                content["rid"] = rid
            return super().render(content)

        # 2. 正常业务数据，自动进行标准包装
        status_code = self.status_code
        wrapped_content = {
            "code": status_code,
            "message": "success" if status_code < 400 else "error",
            "rid": rid,
            "data": content
        }
        
        # 交给父类进行极速的 JSON 序列化
        return super().render(wrapped_content)

# 2. 全局异常拦截器
def configure_exception_handlers(app: FastAPI) -> None:
    
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        """处理主动抛出的 HTTPException (如 404, 401)"""
        return UnifiedResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": str(exc.detail),
                "data": None
            }
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """处理 Pydantic 参数校验失败的异常 (422)"""
        return UnifiedResponse(
            status_code=422,
            content={
                "code": 422,
                "message": "参数校验失败 (Validation Error)",
                "data": exc.errors()
            }
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """兜底处理所有未捕获的严重 Bug (500)"""
        logger.exception(f"Unhandled Server Error: {exc}")
        return UnifiedResponse(
            status_code=500,
            content={
                "code": 500,
                "message": "服务器内部错误 (Internal Server Error)",
                "data": None
            }
        )