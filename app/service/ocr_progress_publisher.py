# -*- coding: utf-8 -*-
"""
OCR 实时进度发布器（最佳努力，绝不影响 OCR 主流程）。

支持多卡并发：同一时刻可能有多个文档分别在不同 GPU 上 OCR，因此用
**每文档一条 Redis 记录**(键 `xtjs:project:{pid}:ocr-live:{doc_id}`) 表达进度，
查询接口聚合返回正在进行中的多个文档。

"当前文档身份"通过 contextvars 在每个文档的并发协程内隔离设置；
OCRProgressMonitor 在构造时(运行在 run_in_threadpool 拷贝的上下文里)捕获该身份，
逐页发布到对应文档的键。所有函数对异常静默(缓存禁用/Redis 异常一律 no-op)。
"""

from __future__ import annotations

import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# 当前协程正在 OCR 的文档身份（按文档隔离，支持并发）
_ACTIVE: ContextVar[Optional[dict[str, Any]]] = ContextVar("xtjs_ocr_active_doc", default=None)

# 实时进度键存活时间：OCR 停止/崩溃后自动过期，陈旧数据自然消失。
_LIVE_TTL_SECONDS = 20


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache():
    """获取共享缓存服务；未启用或不可用时返回 None。"""
    try:
        from app.service.cache_service import get_cache_service

        service = get_cache_service()
        return service if getattr(service, "enabled", False) else None
    except Exception:  # pragma: no cover - 环境相关
        return None


def set_active_document(
    project_id: Any,
    document_id: Any,
    file_name: Any,
    document_type: Any,
) -> Optional[Token]:
    """在当前协程上下文登记正在 OCR 的文档，并发布一个"开始"状态(0 页)。返回 reset token。"""
    if not project_id or not document_id:
        return None
    ctx = {
        "project_id": str(project_id),
        "document_id": str(document_id),
        "file_name": file_name,
        "document_type": document_type,
    }
    token = _ACTIVE.set(ctx)
    publish_progress(ctx, stage="prepare", current_page=0, total_pages=0, percent=0.0)
    return token


def current_active() -> Optional[dict[str, Any]]:
    """返回当前上下文登记的文档身份(供监控器在构造时捕获)。"""
    return _ACTIVE.get()


def clear_active(
    token: Optional[Token] = None,
    *,
    project_id: Any = None,
    document_id: Any = None,
) -> None:
    """清除当前上下文标记，并删除该文档的实时进度键。"""
    ctx = _ACTIVE.get()
    if token is not None:
        try:
            _ACTIVE.reset(token)
        except Exception:
            _ACTIVE.set(None)
    else:
        _ACTIVE.set(None)

    pid = project_id if project_id is not None else (ctx or {}).get("project_id")
    did = document_id if document_id is not None else (ctx or {}).get("document_id")
    if not pid or not did:
        return
    try:
        cache = _cache()
        if cache is not None:
            cache.delete_patterns([cache.project_ocr_live_doc_key(str(pid), str(did))])
    except Exception as exc:  # pragma: no cover - 最佳努力
        logger.debug("clear ocr live progress failed: %s", exc)


def publish_progress(
    ctx: Optional[dict[str, Any]],
    *,
    stage: str,
    current_page: int,
    total_pages: int,
    percent: float,
) -> None:
    """把指定文档的逐页进度写入其 Redis 键(短 TTL)。ctx 为空则 no-op。"""
    if not ctx or not ctx.get("project_id") or not ctx.get("document_id"):
        return
    try:
        cache = _cache()
        if cache is None:
            return
        payload = {
            "document_id": ctx.get("document_id"),
            "file_name": ctx.get("file_name"),
            "document_type": ctx.get("document_type"),
            "stage": stage,
            "current_page": int(current_page or 0),
            "total_pages": int(total_pages or 0),
            "percent": round(float(percent or 0.0), 1),
            "updated_at": _now_iso(),
        }
        cache.set_json(
            cache.project_ocr_live_doc_key(ctx["project_id"], ctx["document_id"]),
            payload,
            ttl_seconds=_LIVE_TTL_SECONDS,
        )
    except Exception as exc:  # pragma: no cover - 最佳努力
        logger.debug("publish ocr live progress failed: %s", exc)


def read_live(project_id: Any) -> list[dict[str, Any]]:
    """聚合读取某项目下所有正在 OCR 文档的实时进度（可能多个，多卡并发）。"""
    if not project_id:
        return []
    try:
        cache = _cache()
        if cache is None:
            return []
        items = cache.scan_json(cache.project_ocr_live_pattern(str(project_id)))
        return [item for item in items if isinstance(item, dict)]
    except Exception as exc:  # pragma: no cover - 最佳努力
        logger.debug("read ocr live progress failed: %s", exc)
        return []
