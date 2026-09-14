"""Bound blocking application I/O without blocking the ASGI event loop."""
import asyncio
import inspect
import weakref
from functools import wraps
from fastapi import HTTPException
from starlette.concurrency import run_in_threadpool
from app.config.settings import settings

_limits = weakref.WeakKeyDictionary()

async def run_io(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    limit = _limits.setdefault(loop, asyncio.Semaphore(settings.XTJS_IO_CONCURRENCY))
    try:
        await asyncio.wait_for(limit.acquire(), timeout=settings.XTJS_IO_QUEUE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError as exc:
        raise HTTPException(503, "服务繁忙，请稍后重试") from exc
    try:
        return await run_in_threadpool(func, *args, **kwargs)
    finally:
        limit.release()

def bounded_sync(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await run_io(func, *args, **kwargs)
    wrapper.__signature__ = inspect.signature(func, eval_str=True)
    return wrapper
