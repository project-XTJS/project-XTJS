import json
import logging
from typing import Any, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def _build_response(
    code: int = 200,
    message: str = "success",
    rid: Optional[str] = None,
    data: Any = None,
) -> dict:
    return {
        "code": code,
        "message": message,
        "rid": rid or str(uuid4()),
        "data": data,
    }


def _normalize_json_payload(payload: Any, status_code: int, rid: str) -> dict:
    if isinstance(payload, dict) and {"code", "message", "rid", "data"}.issubset(payload.keys()):
        normalized = dict(payload)
        code = normalized.get("code", status_code)
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = status_code
        normalized["code"] = code
        normalized["rid"] = normalized.get("rid") or rid
        return normalized

    if isinstance(payload, dict) and any(
        key in payload for key in ("code", "message", "data")
    ):
        code = payload.get("code", status_code)
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = status_code

        message = payload.get("message")
        if not message:
            message = "success" if status_code < 400 else "request failed"

        return _build_response(
            code=code,
            message=str(message),
            rid=rid,
            data=payload.get("data"),
        )

    if isinstance(payload, dict) and "detail" in payload and len(payload) == 1:
        detail = payload["detail"]
        if isinstance(detail, str):
            return _build_response(
                code=status_code,
                message=detail,
                rid=rid,
                data=None,
            )
        return _build_response(
            code=status_code,
            message="request failed",
            rid=rid,
            data=detail,
        )

    return _build_response(
        code=status_code,
        message="success" if status_code < 400 else "request failed",
        rid=rid,
        data=payload,
    )


def configure_response_handlers(app: FastAPI) -> None:
    passthrough_paths = {
        path
        for path in (
            app.openapi_url,
            app.docs_url,
            app.redoc_url,
            "/docs/oauth2-redirect",
        )
        if path
    }

    @app.middleware("http")
    async def unify_json_response(request: Request, call_next):
        rid = str(uuid4())
        request.state.rid = rid
        response = await call_next(request)

        if request.url.path in passthrough_paths:
            return response

        content_type = response.headers.get("content-type", "").lower()
        if "application/json" not in content_type:
            return response

        raw_body = b""
        async for chunk in response.body_iterator:
            raw_body += chunk

        payload: Any = None
        if raw_body:
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = None

        normalized_payload = _normalize_json_payload(payload, response.status_code, rid)
        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() != "content-length"
        }
        return JSONResponse(
            status_code=response.status_code,
            content=normalized_payload,
            headers=headers,
            background=response.background,
        )

    @app.exception_handler(HTTPException)
    async def handle_http_exception(request: Request, exc: HTTPException):
        rid = getattr(request.state, "rid", str(uuid4()))
        detail = exc.detail
        if isinstance(detail, str):
            message = detail
            data = None
        else:
            message = "request failed"
            data = detail
        return JSONResponse(
            status_code=exc.status_code,
            content=_build_response(code=exc.status_code, message=message, rid=rid, data=data),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_exception(request: Request, exc: RequestValidationError):
        rid = getattr(request.state, "rid", str(uuid4()))
        return JSONResponse(
            status_code=422,
            content=_build_response(
                code=422,
                message="request validation failed",
                rid=rid,
                data=exc.errors(),
            ),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(request: Request, exc: Exception):
        rid = getattr(request.state, "rid", str(uuid4()))
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content=_build_response(
                code=500,
                message="internal server error",
                rid=rid,
                data=None,
            ),
        )
