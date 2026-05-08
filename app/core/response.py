import logging
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


def _sanitize_validation_detail(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_sanitize_validation_detail(item) for item in value]
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"input", "url"}:
                continue
            sanitized[str(key)] = _sanitize_validation_detail(item)
        return sanitized
    return str(value)


class UnifiedResponse(JSONResponse):
    """在 JSON 序列化前统一包装为 {code, message, rid, data}。"""

    def render(self, content: Any) -> bytes:
        rid = str(uuid.uuid4())

        if isinstance(content, dict) and {"code", "message"}.issubset(content.keys()):
            if "rid" not in content:
                content["rid"] = rid
            return super().render(content)

        status_code = self.status_code
        wrapped_content = {
            "code": status_code,
            "message": "success" if status_code < 400 else "error",
            "rid": rid,
            "data": content,
        }
        return super().render(wrapped_content)


def configure_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        return UnifiedResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": str(exc.detail),
                "data": None,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return UnifiedResponse(
            status_code=422,
            content={
                "code": 422,
                "message": "参数校验失败 (Validation Error)",
                "data": [_sanitize_validation_detail(item) for item in exc.errors()],
            },
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled Server Error: %s", exc)
        return UnifiedResponse(
            status_code=500,
            content={
                "code": 500,
                "message": "服务器内部错误 (Internal Server Error)",
                "data": None,
            },
        )
