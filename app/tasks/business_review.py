"""Celery execution entrypoint for persistent business-review tasks."""

from __future__ import annotations

import logging
import threading

from celery.exceptions import SoftTimeLimitExceeded

from app.config.settings import settings
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.business_review_tasks import BusinessReviewTaskService
from app.service.postgresql_service import PostgreSQLService
from app.service.resource_access import actor_context
from app.service.user_service import UserService
from app.tasks import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="xtjs.business_review.run",
    bind=True,
    acks_late=True,
    soft_time_limit=max(30, settings.BUSINESS_REVIEW_TASK_TIMEOUT_SECONDS - 30),
    time_limit=settings.BUSINESS_REVIEW_TASK_TIMEOUT_SECONDS,
)
def run_business_review_task(self, task_id: str) -> dict:
    coordinator = BusinessReviewTaskService()
    task = coordinator.claim(task_id)
    if task is None:
        return {"status": "ignored", "task_id": task_id}
    token = str(task["execution_token"])
    stop = threading.Event()

    def heartbeat_loop() -> None:
        while not stop.wait(settings.BUSINESS_REVIEW_TASK_HEARTBEAT_SECONDS):
            try:
                if not coordinator.heartbeat(task_id, token):
                    return
            except Exception:
                logger.exception("business review heartbeat failed task_id=%s", task_id)

    heartbeat = threading.Thread(target=heartbeat_loop, name=f"business-review-heartbeat-{task_id}", daemon=True)
    heartbeat.start()
    actor_token = None
    try:
        user = UserService().get_session_record(str(task["requested_by"]))
        if not user or not user.get("is_active"):
            raise PermissionError("发起任务的账号不存在或已停用")
        actor_token = actor_context.set(UserService._public_view(user))
        if not coordinator.requester_can_access(task):
            coordinator.stale(task_id, token, "发起人已无项目权限，结果未写入")
            return {"status": "stale", "task_id": task_id}
        db_service = PostgreSQLService()
        project = db_service.get_project_by_identifier(str(task["project_identifier_id"]))
        if not project:
            raise PermissionError("无权访问项目，或项目已删除")
        if int(project.get("input_revision") or 0) != int(task["input_revision"]):
            coordinator.stale(task_id, token, "项目材料已变化，结果未写入")
            return {"status": "stale", "task_id": task_id}

        review_service = UnifiedBusinessReviewService(db_service=db_service)

        def progress(stage: str, completed: int, total: int, message: str) -> None:
            if not coordinator.heartbeat(
                task_id,
                token,
                stage=stage,
                completed=completed,
                total=total,
                message=message,
            ):
                raise RuntimeError("business review execution lease expired")

        review = review_service.review_project_business_documents(
            project_identifier=str(task["project_identifier_id"]),
            progress_callback=progress,
            force_sequential=True,
        )
        progress("saving", 0, 1, "正在保存商务审查结果")
        outcome = coordinator.complete(task_id, token, review)
        if outcome.get("status") == "succeeded":
            try:
                from app.service.cache_service import get_cache_service

                get_cache_service().invalidate_project(str(task["project_identifier_id"]))
            except Exception:
                logger.exception("business review cache invalidation failed task_id=%s", task_id)
        logger.info(
            "business review task finished task_id=%s project_id=%s input_revision=%s status=%s",
            task_id, task["project_identifier_id"], task["input_revision"], outcome.get("status"),
        )
        return outcome
    except SoftTimeLimitExceeded:
        coordinator.fail(task_id, token, "商务审查超过 15 分钟，已终止，请检查附件范围后重试")
        raise
    except Exception as exc:
        logger.exception("business review task failed task_id=%s", task_id)
        coordinator.fail(task_id, token, str(exc) or exc.__class__.__name__)
        raise
    finally:
        stop.set()
        heartbeat.join(timeout=1)
        if actor_token is not None:
            actor_context.reset(actor_token)
