"""Persistent coordination for project business-review tasks."""

from __future__ import annotations

import logging
import time
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException
from psycopg2.extras import Json, RealDictCursor

from app.config.settings import settings
from app.core.consistency import ConsistencyConflict
from app.service.manual_review_state import (
    MANUAL_REVIEW_RESULTS_KEY,
    manual_review_results_from_record,
    utc_now_iso,
)
from app.service.postgresql_service import PostgreSQLService

logger = logging.getLogger(__name__)

OPERATION = "business_review"
ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed", "interrupted", "stale"}


def _public_task(row: dict[str, Any] | None, *, reused: bool = False) -> dict[str, Any] | None:
    if not row:
        return None
    result = {
        "task_id": str(row["identifier_id"]),
        "project_identifier_id": str(row["project_identifier_id"]),
        "operation": row["operation"],
        "request_id": str(row["request_id"]),
        "input_revision": int(row["input_revision"]),
        "status": row["status"],
        "stage": row["stage"],
        "progress": dict(row.get("progress") or {}),
        "result_version": row.get("result_version"),
        "error": row.get("error_message"),
        "created_at": row.get("create_time"),
        "started_at": row.get("started_at"),
        "updated_at": row.get("update_time"),
        "finished_at": row.get("finished_at"),
        "heartbeat_at": row.get("heartbeat_at"),
        "reused": bool(reused),
    }
    return result


class BusinessReviewTaskService:
    def __init__(self, db_service: PostgreSQLService | None = None) -> None:
        self.db_service = db_service or PostgreSQLService()

    def submit(
        self,
        *,
        project_identifier: str,
        request_id: str,
        expected_input_revision: int,
        requested_by: str,
        request_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        UUID(str(request_id))
        project = self.db_service.get_project_by_identifier(project_identifier)
        if not project:
            raise ValueError(f"project not found: {project_identifier}")
        project_id = str(project["identifier_id"])
        if int(project.get("input_revision") or 0) != int(expected_input_revision):
            raise ConsistencyConflict("项目材料已变化，请刷新后重试")

        with self.db_service._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT identifier_id,input_revision FROM xtjs_projects "
                    "WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE",
                    (project_id,),
                )
                current = cursor.fetchone()
                if not current:
                    raise ValueError(f"project not found: {project_identifier}")
                if int(current["input_revision"]) != int(expected_input_revision):
                    raise ConsistencyConflict("项目材料已变化，请刷新后重试")

                cursor.execute(
                    "SELECT * FROM xtjs_analysis_tasks "
                    "WHERE project_identifier_id=%s AND operation=%s AND request_id=%s",
                    (project_id, OPERATION, request_id),
                )
                existing_request = cursor.fetchone()
                if existing_request:
                    return _public_task(dict(existing_request), reused=True), False

                cursor.execute(
                    "SELECT * FROM xtjs_analysis_tasks "
                    "WHERE project_identifier_id=%s AND operation=%s "
                    "AND status IN ('queued','running') ORDER BY create_time DESC LIMIT 1",
                    (project_id, OPERATION),
                )
                active = cursor.fetchone()
                if active:
                    if int(active["input_revision"]) != int(expected_input_revision):
                        raise HTTPException(409, "已有其他材料版本的商务审查任务正在运行")
                    active_mode = str((active.get("request_payload") or {}).get("mode") or "standard")
                    requested_mode = str((request_payload or {}).get("mode") or "standard")
                    if active_mode != requested_mode:
                        raise HTTPException(409, "已有不同类型的商务审查任务正在运行")
                    return _public_task(dict(active), reused=True), False

                cursor.execute(
                    "SELECT result_version FROM xtjs_result WHERE project_identifier_id=%s",
                    (project_id,),
                )
                result_row = cursor.fetchone()
                start_result_version = result_row.get("result_version") if result_row else None
                # The project row lock serializes submissions for this project.
                cursor.execute(
                    """INSERT INTO xtjs_analysis_tasks
                       (project_identifier_id,operation,request_id,input_revision,
                        start_result_version,requested_by,request_payload,progress)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (
                        project_id,
                        OPERATION,
                        request_id,
                        int(expected_input_revision),
                        start_result_version,
                        requested_by,
                        Json(request_payload or {}),
                        Json({"completed": 0, "total": 0, "message": "任务已进入队列"}),
                    ),
                )
                row = dict(cursor.fetchone())
        logger.info(
            "business review task accepted task_id=%s project_id=%s input_revision=%s",
            row["identifier_id"], project_id, expected_input_revision,
        )
        return _public_task(row), True

    def get_latest(self, project_identifier: str) -> dict[str, Any] | None:
        project = self.db_service.get_project_by_identifier(project_identifier)
        if not project:
            raise ValueError(f"project not found: {project_identifier}")
        with self.db_service._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM xtjs_analysis_tasks WHERE project_identifier_id=%s "
                "AND operation=%s ORDER BY create_time DESC LIMIT 1",
                (str(project["identifier_id"]), OPERATION),
            )
            row = cursor.fetchone()
        return _public_task(dict(row)) if row else None

    def get(self, project_identifier: str, task_id: str) -> dict[str, Any] | None:
        project = self.db_service.get_project_by_identifier(project_identifier)
        if not project:
            raise ValueError(f"project not found: {project_identifier}")
        with self.db_service._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM xtjs_analysis_tasks WHERE identifier_id=%s "
                "AND project_identifier_id=%s AND operation=%s",
                (task_id, str(project["identifier_id"]), OPERATION),
            )
            row = cursor.fetchone()
        return _public_task(dict(row)) if row else None

    def mark_published(self, task_id: str, error: str | None = None) -> None:
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """UPDATE xtjs_analysis_tasks SET
                     published_at=CASE WHEN %s IS NULL THEN CURRENT_TIMESTAMP ELSE published_at END,
                     publish_attempts=publish_attempts+1,last_publish_error=%s,
                     update_time=CURRENT_TIMESTAMP WHERE identifier_id=%s AND status='queued'""",
                (error, error, task_id),
            )

    def pending_for_dispatch(self, limit: int = 50) -> list[str]:
        self.interrupt_stale()
        grace = int(settings.BUSINESS_REVIEW_DISPATCH_INTERVAL_SECONDS)
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """SELECT identifier_id FROM xtjs_analysis_tasks
                   WHERE status='queued' AND (
                     (published_at IS NULL AND create_time < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second'))
                     OR published_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'
                   ) ORDER BY create_time LIMIT %s""",
                (grace, limit),
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def interrupt_stale(self) -> int:
        seconds = int(settings.BUSINESS_REVIEW_TASK_STALE_SECONDS)
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """UPDATE xtjs_analysis_tasks SET status='interrupted',stage='interrupted',
                   error_message='任务心跳中断，请重新运行',finished_at=CURRENT_TIMESTAMP,
                   update_time=CURRENT_TIMESTAMP
                   WHERE status='running' AND heartbeat_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second')""",
                (seconds,),
            )
            return int(cursor.rowcount or 0)

    def claim(self, task_id: str) -> dict[str, Any] | None:
        token = str(uuid4())
        with self.db_service._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """UPDATE xtjs_analysis_tasks SET status='running',stage='preparing',
                   execution_token=%s,started_at=CURRENT_TIMESTAMP,heartbeat_at=CURRENT_TIMESTAMP,
                   update_time=CURRENT_TIMESTAMP,error_message=NULL
                   WHERE identifier_id=%s AND status='queued' RETURNING *""",
                (token, task_id),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def heartbeat(
        self,
        task_id: str,
        execution_token: str,
        *,
        stage: str | None = None,
        completed: int | None = None,
        total: int | None = None,
        message: str | None = None,
    ) -> bool:
        progress = None
        if any(value is not None for value in (completed, total, message)):
            progress = {
                "completed": int(completed or 0),
                "total": int(total or 0),
                "message": str(message or ""),
            }
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """UPDATE xtjs_analysis_tasks SET heartbeat_at=CURRENT_TIMESTAMP,
                   stage=COALESCE(%s,stage),progress=COALESCE(%s,progress),
                   update_time=CURRENT_TIMESTAMP
                   WHERE identifier_id=%s AND execution_token=%s AND status='running'""",
                (stage, Json(progress) if progress is not None else None, task_id, execution_token),
            )
            return bool(cursor.rowcount)

    def fail(self, task_id: str, execution_token: str, message: str) -> None:
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """UPDATE xtjs_analysis_tasks SET status='failed',stage='failed',
                   error_message=%s,finished_at=CURRENT_TIMESTAMP,heartbeat_at=CURRENT_TIMESTAMP,
                   update_time=CURRENT_TIMESTAMP
                   WHERE identifier_id=%s AND execution_token=%s AND status='running'""",
                (str(message)[:2000], task_id, execution_token),
            )

    def stale(self, task_id: str, execution_token: str, message: str) -> bool:
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            self._mark_stale(cursor, task_id, execution_token, message)
            return bool(cursor.rowcount)

    def requester_can_access(self, task: dict[str, Any]) -> bool:
        with self.db_service._get_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """SELECT u.is_active AND (
                         u.role_level <> 2 OR xtjs_can_access_project(%s, u.identifier_id)
                       )
                   FROM xtjs_users u
                   WHERE u.identifier_id=%s AND u.deleted=FALSE""",
                (task["project_identifier_id"], task["requested_by"]),
            )
            row = cursor.fetchone()
            return bool(row and row[0])

    def complete(self, task_id: str, execution_token: str, review: dict[str, Any]) -> dict[str, Any]:
        """Persist result and task success atomically, rejecting stale inputs/results."""
        with self.db_service._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM xtjs_analysis_tasks WHERE identifier_id=%s FOR UPDATE",
                    (task_id,),
                )
                task = cursor.fetchone()
                if not task or task["status"] != "running" or str(task["execution_token"]) != execution_token:
                    raise RuntimeError("business review execution lease is no longer valid")
                cursor.execute(
                    """SELECT u.is_active AND (
                             u.role_level <> 2 OR xtjs_can_access_project(%s, u.identifier_id)
                           ) AS can_access
                       FROM xtjs_users u WHERE u.identifier_id=%s AND u.deleted=FALSE""",
                    (task["project_identifier_id"], task["requested_by"]),
                )
                permission = cursor.fetchone()
                if not permission or not permission["can_access"]:
                    self._mark_stale(cursor, task_id, execution_token, "发起人权限已变化，结果未写入")
                    return {"status": "stale"}
                cursor.execute(
                    """SELECT identifier_id,project_name,upload_manifest,input_revision FROM xtjs_projects
                       WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE""",
                    (task["project_identifier_id"],),
                )
                project = cursor.fetchone()
                if not project or int(project["input_revision"]) != int(task["input_revision"]):
                    self._mark_stale(cursor, task_id, execution_token, "项目材料已变化，结果未写入")
                    return {"status": "stale"}
                cursor.execute(
                    "SELECT * FROM xtjs_result WHERE project_identifier_id=%s",
                    (task["project_identifier_id"],),
                )
                row = cursor.fetchone()
                current_version = row.get("result_version") if row else None
                if current_version != task.get("start_result_version"):
                    self._mark_stale(cursor, task_id, execution_token, "项目结果已被更新，结果未写入")
                    return {"status": "stale"}
                existing = self.db_service._sanitize_project_result_record(dict(row)) if row else {}
                existing_result = dict(existing.get("result") or {})
                existing_result.pop("project_file_urls", None)
                existing_result["business_bid_format_review"] = review
                manual = manual_review_results_from_record(existing)
                latest = dict(manual.get("latest") or {})
                latest.pop("business_bid_format_review", None)
                manual["latest"] = latest
                manual["updated_at"] = utc_now_iso()
                if latest or manual.get("workflow_scope"):
                    existing_result[MANUAL_REVIEW_RESULTS_KEY] = manual
                else:
                    existing_result.pop(MANUAL_REVIEW_RESULTS_KEY, None)
                payload = self.db_service._prepare_project_result_for_persistence(
                    str(project["identifier_id"]), existing_result, existing=existing
                )
                result_record = self.db_service._persist_project_result(cursor, dict(project), payload)
                cursor.execute(
                    """UPDATE xtjs_analysis_tasks SET status='succeeded',stage='succeeded',
                       progress=%s,result_version=%s,finished_at=CURRENT_TIMESTAMP,
                       heartbeat_at=CURRENT_TIMESTAMP,update_time=CURRENT_TIMESTAMP
                       WHERE identifier_id=%s AND execution_token=%s AND status='running'""",
                    (
                        Json({"completed": 1, "total": 1, "message": "商务审查结果已生成"}),
                        result_record.get("result_version"),
                        task_id,
                        execution_token,
                    ),
                )
                if not cursor.rowcount:
                    raise RuntimeError("business review execution lease expired before commit")
        return {"status": "succeeded", "result_record": result_record}

    @staticmethod
    def _mark_stale(cursor, task_id: str, token: str, message: str) -> None:
        cursor.execute(
            """UPDATE xtjs_analysis_tasks SET status='stale',stage='stale',error_message=%s,
               finished_at=CURRENT_TIMESTAMP,update_time=CURRENT_TIMESTAMP
               WHERE identifier_id=%s AND execution_token=%s AND status='running'""",
            (message, task_id, token),
        )


def publish_task(task_id: str) -> bool:
    service = BusinessReviewTaskService()
    try:
        from app.tasks import celery_app

        celery_app.send_task(
            "xtjs.business_review.run",
            args=[str(task_id)],
            task_id=str(task_id),
            queue=settings.BUSINESS_REVIEW_QUEUE,
        )
        service.mark_published(task_id)
        return True
    except Exception as exc:
        logger.exception("business review task publish failed task_id=%s", task_id)
        service.mark_published(task_id, error=str(exc)[:1000])
        return False


def dispatch_pending_tasks() -> int:
    service = BusinessReviewTaskService()
    count = 0
    for task_id in service.pending_for_dispatch():
        count += int(publish_task(task_id))
    return count


def run_business_review_compatibility(
    *,
    project_identifier: str,
    requested_by: str,
    expected_input_revision: int,
    mode: str = "standard",
    request_id: str | None = None,
) -> dict[str, Any]:
    """Execute via the coordinator while preserving the legacy response shape."""
    service = BusinessReviewTaskService()
    task, created = service.submit(
        project_identifier=project_identifier,
        request_id=request_id or str(uuid4()),
        expected_input_revision=expected_input_revision,
        requested_by=requested_by,
        request_payload={"mode": mode, "compatibility_wait": True},
    )
    if created:
        publish_task(task["task_id"])

    deadline = time.monotonic() + int(settings.BUSINESS_REVIEW_TASK_TIMEOUT_SECONDS) + 90
    while time.monotonic() < deadline:
        current = service.get(project_identifier, task["task_id"])
        if not current:
            raise RuntimeError("商务审查任务不存在")
        status = current["status"]
        if status == "succeeded":
            record = service.db_service.get_project_result(project_identifier)
            result = dict((record or {}).get("result") or {})
            return {
                "project_identifier_id": str(current["project_identifier_id"]),
                "result_key": "business_bid_format_review",
                "review": result.get("business_bid_format_review") or {},
                "result_record": record,
                "task": current,
            }
        if status == "stale":
            raise ConsistencyConflict(current.get("error") or "项目材料或结果已变化，旧任务未写入")
        if status in {"failed", "interrupted"}:
            raise RuntimeError(current.get("error") or "商务审查任务执行失败")
        time.sleep(1)
    raise RuntimeError("商务审查任务状态等待超时，请使用任务编号继续查询")
