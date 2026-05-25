# -*- coding: utf-8 -*-
"""
项目级后台运行时注册与取消工具。
"""

from __future__ import annotations

import asyncio
import threading


class ProjectTaskCancelledError(RuntimeError):
    """项目被删除或主动终止后，用于中断其后台流程。"""


_RUNTIME_LOCK = threading.RLock()
_PROJECT_CANCEL_EVENTS: dict[str, threading.Event] = {}
_PROJECT_TASKS: dict[str, set[asyncio.Task]] = {}


def _normalize_project_identifier(identifier_id: str) -> str:
    normalized = str(identifier_id or "").strip()
    if not normalized:
        raise ValueError("project identifier cannot be empty")
    return normalized


def ensure_project_cancel_event(identifier_id: str) -> threading.Event:
    """返回项目级取消事件，不存在时自动创建。"""
    normalized = _normalize_project_identifier(identifier_id)
    with _RUNTIME_LOCK:
        event = _PROJECT_CANCEL_EVENTS.get(normalized)
        if event is None:
            event = threading.Event()
            _PROJECT_CANCEL_EVENTS[normalized] = event
        return event


def register_project_task(identifier_id: str, task: asyncio.Task) -> threading.Event:
    """登记项目关联的后台任务，并返回对应取消事件。"""
    normalized = _normalize_project_identifier(identifier_id)
    event = ensure_project_cancel_event(normalized)
    with _RUNTIME_LOCK:
        _PROJECT_TASKS.setdefault(normalized, set()).add(task)
    return event


def _active_tasks_locked(identifier_id: str) -> tuple[asyncio.Task, ...]:
    """返回未完成任务，并顺手清理已经结束的任务登记。"""
    tasks = _PROJECT_TASKS.get(identifier_id)
    if not tasks:
        return ()

    active_tasks = {task for task in tasks if not task.done()}
    if active_tasks:
        if len(active_tasks) != len(tasks):
            _PROJECT_TASKS[identifier_id] = active_tasks
        return tuple(active_tasks)

    _PROJECT_TASKS.pop(identifier_id, None)
    _PROJECT_CANCEL_EVENTS.pop(identifier_id, None)
    return ()


def project_runtime_task_count(identifier_id: str) -> int:
    """返回项目当前仍在运行或排队的后台任务数量。"""
    normalized = _normalize_project_identifier(identifier_id)
    with _RUNTIME_LOCK:
        return len(_active_tasks_locked(normalized))


def is_project_runtime_active(identifier_id: str) -> bool:
    """判断项目是否存在仍在运行或排队的后台任务。"""
    return project_runtime_task_count(identifier_id) > 0


def active_project_runtime_identifiers() -> tuple[str, ...]:
    """返回当前仍有后台任务运行或排队的项目标识，并顺手清理已结束登记。"""
    with _RUNTIME_LOCK:
        active_identifiers: list[str] = []
        for identifier_id in list(_PROJECT_TASKS):
            if _active_tasks_locked(identifier_id):
                active_identifiers.append(identifier_id)
        return tuple(active_identifiers)


def unregister_project_task(identifier_id: str, task: asyncio.Task) -> None:
    """移除项目后台任务登记；当项目没有活跃任务后自动清理运行时状态。"""
    normalized = _normalize_project_identifier(identifier_id)
    with _RUNTIME_LOCK:
        tasks = _PROJECT_TASKS.get(normalized)
        if tasks is not None:
            tasks.discard(task)
            if not tasks:
                _PROJECT_TASKS.pop(normalized, None)
                _PROJECT_CANCEL_EVENTS.pop(normalized, None)


def cancel_project_runtime(identifier_id: str) -> int:
    """触发项目级取消，并尽力取消该项目已登记的所有后台任务。"""
    normalized = _normalize_project_identifier(identifier_id)
    with _RUNTIME_LOCK:
        event = _PROJECT_CANCEL_EVENTS.get(normalized)
        if event is None:
            event = threading.Event()
            _PROJECT_CANCEL_EVENTS[normalized] = event
        event.set()
        tasks = tuple(_PROJECT_TASKS.get(normalized) or ())

    for task in tasks:
        task.cancel()
    return len(tasks)


def check_project_cancelled(
    cancel_event: threading.Event | None,
    *,
    identifier_id: str | None = None,
) -> None:
    """若项目已被取消，则抛出统一异常供上层快速中断。"""
    if cancel_event is None or not cancel_event.is_set():
        return
    project_identifier = str(identifier_id or "").strip() or "unknown"
    raise ProjectTaskCancelledError(f"project runtime cancelled: {project_identifier}")
