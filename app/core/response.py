# -*- coding: utf-8 -*-
"""
统一响应与异常处理模块

定义统一 JSON 响应格式，注册 FastAPI 全局异常处理器，
确保所有接口返回结构形如 {code, message, rid, data}。
"""

import logging
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


# 辅助函数：清洗验证错误详情中的敏感字段
def _sanitize_validation_detail(value: Any) -> Any:
    """
    递归清洗 pydantic 验证错误详情，移除包含敏感信息的字段（如 input、url），
    避免在日志或响应中泄露请求体或 URL 内容。

    参数：
        value: 验证错误列表 / 字典中的任意节点

    返回：
        清洗后的值：基本类型直接返回，列表/元组递归处理每个元素，
        字典递归处理每个键值对，但跳过键为 "input" 或 "url" 的项，
        其余类型转为字符串。
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_sanitize_validation_detail(item) for item in value]
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            # 跳过可能包含请求体的敏感字段
            if key in {"input", "url"}:
                continue
            sanitized[str(key)] = _sanitize_validation_detail(item)
        return sanitized
    # 未知类型转为字符串，防止序列化异常
    return str(value)


# 统一响应类
class UnifiedResponse(JSONResponse):
    """
    自定义 JSONResponse，在渲染内容前统一包装为 {code, message, rid, data}。
    若内容本身已包含 code 和 message 字段，则仅补充 rid；
    否则根据 HTTP 状态码构建标准结构。
    """

    def render(self, content: Any) -> bytes:
        # 为每次响应生成唯一请求 ID，便于日志追踪
        rid = str(uuid.uuid4())

        # 如果内容已经是标准包装格式，补充 rid 即可
        if isinstance(content, dict) and {"code", "message"}.issubset(content.keys()):
            if "rid" not in content:
                content["rid"] = rid
            return super().render(content)

        # 否则统一包装为 {code, message, rid, data}
        status_code = self.status_code
        wrapped_content = {
            "code": status_code,
            "message": "success" if status_code < 400 else "error",
            "rid": rid,
            "data": content,
        }
        return super().render(wrapped_content)


# 注册全局异常处理器
def configure_exception_handlers(app: FastAPI) -> None:
    """
    为 FastAPI 应用注册统一的异常处理回调：
    - HTTPException（如 404）
    - RequestValidationError（如 422 参数校验失败）
    - 兜底 Exception（转换为 500 内部错误）
    """

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        # HTTP 标准异常，使用其状态码和详情
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
        # 参数校验失败，返回 422 及清洗后的错误详情
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
        # 未捕获异常兜底，记录完整异常栈，返回 500 统一错误响应
        logger.exception("Unhandled Server Error: %s", exc)
        return UnifiedResponse(
            status_code=500,
            content={
                "code": 500,
                "message": "服务器内部错误 (Internal Server Error)",
                "data": None,
            },
        )