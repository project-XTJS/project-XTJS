# -*- coding: utf-8 -*-
"""
PostgreSQL 数据访问服务模块。

提供连接池管理及项目、文档、关联关系、分析结果的 CRUD 操作。
"""

import logging
import re
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from uuid import uuid4

import json
from fastapi.encoders import jsonable_encoder
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool, PoolError
from threading import Lock
from fastapi import HTTPException

from app.config.settings import settings
from app.core.document_types import (
    ACTIVE_DOCUMENT_TYPES,
    BUSINESS_BID_COMPATIBLE_TYPES,
    DOCUMENT_TYPE_TENDER,
    SUPPORTED_DOCUMENT_TYPES,
    TECHNICAL_BID_COMPATIBLE_TYPES,
    get_document_type_label,
)
from app.service.analysis.location_utils import (
    append_location,
    collect_locations,
    make_location,
    normalize_locations,
)
from app.service.minio_service import MinioService
from app.service import document_blob_store
from app.service.analysis.duplicate_merge.review_projection import project_duplicate_payload
from app.service.project_result_summary import build_project_result_summary, is_result_key_visible
from app.service.review_index import (
    REMOVED_BUSINESS_SCOPE_ISSUE_TITLE,
    build_result_version,
    prepare_review_storage,
)
from app.service.upload_manifest import upload_summary
from app.core.consistency import ConsistencyConflict
from app.service.upload_recovery import UploadRecoveryMixin
from app.service.manual_review_state import (
    MANUAL_REVIEW_RESULTS_KEY,
    build_manual_review_results,
    build_review_content,
    effective_document_content,
    manual_review_results_from_record,
    normalize_review_content,
    utc_now_iso,
)
from app.service.workflow_scope import (
    filter_document_records,
    workflow_scope_from_result_record,
)

logger = logging.getLogger(__name__)

UUID_TEXT = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID_TEXT_PATTERN = re.compile(rf"(?i)\b{UUID_TEXT}\b")
UUID_SUFFIX_PATTERN = re.compile(rf"(?i)\(({UUID_TEXT})\)\s*$")
MISSING_UUID_SENTINEL = "00000000-0000-0000-0000-000000000000"
LEGACY_NOT_APPLICABLE_STATUSES = {"not_applicable", "skipped", "optional"}
DUPLICATE_REVIEW_KEYS = frozenset({"business_bid_duplicate_check", "technical_bid_duplicate_check"})
_duplicate_projection_cache: OrderedDict[tuple[str, str, str], list[dict[str, Any]] | None] = OrderedDict()
_duplicate_projection_cache_lock = Lock()


def _remember_duplicate_projection(key: tuple[str, str, str], value: list[dict[str, Any]] | None):
    with _duplicate_projection_cache_lock:
        _duplicate_projection_cache[key] = value
        _duplicate_projection_cache.move_to_end(key)
        while len(_duplicate_projection_cache) > 4:
            _duplicate_projection_cache.popitem(last=False)
    return value


def _canonical_review_issue_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"pass", "passed", "success", "ok"}:
        return "pass"
    if raw in {"fail", "failed", "missing", "error"}:
        return "fail"
    if raw in LEGACY_NOT_APPLICABLE_STATUSES:
        return "not_applicable"
    if raw in {"unclear", "pending", "ambiguous", "review"}:
        return "unclear"
    return raw


def _canonical_review_status_counts(value: Any) -> dict[str, int]:
    counts = value if isinstance(value, dict) else {}
    result = {"pass": 0, "fail": 0, "unclear": 0, "not_applicable": 0}
    for status, count in counts.items():
        canonical = _canonical_review_issue_status(status)
        if canonical in result:
            try:
                result[canonical] += int(count or 0)
            except (TypeError, ValueError):
                continue
    return result


def _canonical_review_summary(value: dict[str, Any]) -> dict[str, Any]:
    """Map legacy N/A buckets at read time without rewriting stored indexes."""
    summary = dict(value)
    counts = _canonical_review_status_counts(summary.get("status_counts"))
    summary["status_counts"] = counts
    summary["review_item_count"] = sum(counts.values())
    summary["inconsistent_count"] = counts["fail"]
    summary["unclear_count"] = counts["unclear"]
    summary["not_applicable_count"] = counts["not_applicable"]
    categories = []
    for category in summary.get("categories") or []:
        if not isinstance(category, dict):
            continue
        normalized = dict(category)
        category_counts = _canonical_review_status_counts(normalized.get("status_counts"))
        normalized["status_counts"] = category_counts
        normalized["review_item_count"] = sum(category_counts.values())
        normalized["inconsistent_count"] = category_counts["fail"]
        normalized["unclear_count"] = category_counts["unclear"]
        normalized["not_applicable_count"] = category_counts["not_applicable"]
        categories.append(normalized)
    summary["categories"] = categories
    return summary


def _subtract_removed_review_issue_counts(
    value: dict[str, Any],
    removed_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Keep legacy persisted summaries consistent with retired issue rows."""
    summary = dict(value)
    summary["risk_counts"] = dict(summary.get("risk_counts") or {})
    summary["status_counts"] = dict(summary.get("status_counts") or {})
    categories = [dict(item) for item in summary.get("categories") or [] if isinstance(item, dict)]
    by_key = {str(item.get("result_key") or ""): item for item in categories}
    for category in categories:
        category["risk_counts"] = dict(category.get("risk_counts") or {})
        category["status_counts"] = dict(category.get("status_counts") or {})

    def decrement(target: dict[str, Any], key: str, amount: int) -> None:
        try:
            current = int(target.get(key) or 0)
        except (TypeError, ValueError):
            current = 0
        target[key] = max(0, current - amount)

    for row in removed_rows:
        try:
            count = max(0, int(row.get("count") or 0))
        except (TypeError, ValueError):
            count = 0
        if count == 0:
            continue
        risk = str(row.get("risk_level") or "").strip().lower()
        status = _canonical_review_issue_status(row.get("status"))
        decrement(summary, "issue_count", count)
        decrement(summary["risk_counts"], risk, count)
        decrement(summary["status_counts"], status, count)
        category = by_key.get(str(row.get("result_key") or ""))
        if category is None:
            continue
        decrement(category, "issue_count", count)
        decrement(category["risk_counts"], risk, count)
        decrement(category["status_counts"], status, count)

    for target in [summary, *categories]:
        counts = _canonical_review_status_counts(target.get("status_counts"))
        target["status_counts"] = counts
        target["review_item_count"] = sum(counts.values())
        target["inconsistent_count"] = counts["fail"]
        target["unclear_count"] = counts["unclear"]
        target["not_applicable_count"] = counts["not_applicable"]
        if target is not summary:
            risks = target.get("risk_counts") or {}
            target["has_risk"] = sum(int(risks.get(key) or 0) for key in ("high", "medium", "low")) > 0
    summary["categories"] = categories
    return summary


def _canonical_review_issue_row(value: dict[str, Any]) -> dict[str, Any]:
    row = dict(value)
    payload = dict(row.get("list_payload") or {})
    raw = payload.get("status") or row.get("status")
    canonical = _canonical_review_issue_status(raw)
    if canonical:
        row["status"] = canonical
        payload["status"] = canonical
    if str(raw or "").strip().lower() in LEGACY_NOT_APPLICABLE_STATUSES:
        payload.setdefault("applicability_status", "not_applicable")
        description = str(row.get("description") or payload.get("summary") or "当前材料不适用该检查").strip()
        if not description.startswith("该项不适用"):
            description = f"该项不适用：{description}"
        row["description"] = description
        payload["summary"] = description
    if payload:
        row["list_payload"] = payload
    return row

# 全局连接池（模块级单例）
_db_pool = None
_db_pool_lock = Lock()


def get_db_pool():
    """返回 PostgreSQL 线程安全连接池，首次调用时初始化。"""
    global _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            try:
                _db_pool = ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    dsn=settings.DATABASE_URL,
                )
                logger.info("PostgreSQL 连接池初始化成功。")
            except Exception as exc:
                logger.error("PostgreSQL 连接池初始化失败: %s", exc)
                raise
    return _db_pool


from app.service.resource_access import ResourceAccessMixin, access_check, actor_id, scope_sql


class PostgreSQLService(ResourceAccessMixin, UploadRecoveryMixin):
    """PostgreSQL 数据库服务层，封装项目、文档、关系及结果操作。"""

    ACTIVE_DOCUMENT_TYPES = set(ACTIVE_DOCUMENT_TYPES)
    SUPPORTED_DOCUMENT_TYPES = set(SUPPORTED_DOCUMENT_TYPES)
    # 0=未开始 OCR，1=招标文件 OCR 完成，2=商务标 OCR 完成，3=技术标 OCR 完成。
    PARSING_STATUS_PENDING = 0
    PARSING_STATUS_TENDER_OCR_COMPLETED = 1
    PARSING_STATUS_BUSINESS_OCR_COMPLETED = 2
    PARSING_STATUS_TECHNICAL_OCR_COMPLETED = 3
    # 保留 uploaded 常量名，兼容旧调用方。
    PARSING_STATUS_UPLOADED = PARSING_STATUS_PENDING
    PARSING_STATUS_LABELS = {
        PARSING_STATUS_PENDING: "pending",
        PARSING_STATUS_TENDER_OCR_COMPLETED: "tender_ocr_completed",
        PARSING_STATUS_BUSINESS_OCR_COMPLETED: "business_ocr_completed",
        PARSING_STATUS_TECHNICAL_OCR_COMPLETED: "technical_ocr_completed",
    }
    # 给接口和报错复用的人类可读状态文案。
    PARSING_STATUS_TEXTS = {
        PARSING_STATUS_PENDING: "未开始OCR",
        PARSING_STATUS_TENDER_OCR_COMPLETED: "招标文件OCR完成",
        PARSING_STATUS_BUSINESS_OCR_COMPLETED: "商务标OCR完成",
        PARSING_STATUS_TECHNICAL_OCR_COMPLETED: "技术标OCR完成",
    }

    # 连接管理
    @contextmanager
    def _get_connection(self):
        """获取数据库连接上下文，使用完毕后自动归还连接池。"""
        pool = get_db_pool()
        try:
            conn = pool.getconn()
        except PoolError as exc:
            raise HTTPException(503, "数据库连接繁忙，请稍后重试") from exc
        try:
            with document_blob_store.transaction_objects(), conn:
                with conn.cursor() as actor_cursor:
                    actor_cursor.execute("SELECT set_config('xtjs.actor_id', %s, true)", (actor_id(),))
                yield conn
        finally:
            pool.putconn(conn)

    # 标识/字段清理工具
    @staticmethod
    def _extract_identifier(value: Optional[str]) -> str:
        """从 Swagger 展示值中提取 UUID，兼容“名称 (UUID)”格式。"""
        text = (value or "").strip()
        if UUID_TEXT_PATTERN.fullmatch(text):
            return text
        match = UUID_SUFFIX_PATTERN.search(text)
        return match.group(1) if match else text


    @staticmethod
    def _normalize_required_identifier(identifier_id: str, field_name: str) -> str:
        """验证标识非空并返回清理后的值。"""
        normalized = PostgreSQLService._extract_identifier(identifier_id)
        if not normalized:
            raise ValueError(f"{field_name} cannot be empty")
        return normalized

    @staticmethod
    def _normalize_file_value(value: Optional[str], field_name: str) -> str:
        """验证文件名字段非空并返回清理后的值。"""
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError(f"{field_name} cannot be empty")
        return normalized

    @staticmethod
    def _normalize_project_name(project_name: Optional[str]) -> str:
        """验证项目名称非空并返回清理后的值。"""
        normalized = (project_name or "").strip()
        if not normalized:
            raise ValueError("project_name cannot be empty")
        return normalized

    @classmethod
    def _normalize_parsing_status(cls, parsing_status: Optional[int]) -> int:
        # 兼容异常值，并将状态收敛到 0~3。
        try:
            normalized = int(parsing_status or 0)
        except (TypeError, ValueError):
            normalized = cls.PARSING_STATUS_PENDING
        if normalized < cls.PARSING_STATUS_PENDING:
            return cls.PARSING_STATUS_PENDING
        if normalized > cls.PARSING_STATUS_TECHNICAL_OCR_COMPLETED:
            return cls.PARSING_STATUS_TECHNICAL_OCR_COMPLETED
        return normalized

    @classmethod
    def parsing_status_reached(cls, parsing_status: Optional[int], required_status: int) -> bool:
        """判断当前项目 OCR 状态是否达到某个分析前置阶段。"""
        return cls._normalize_parsing_status(parsing_status) >= cls._normalize_parsing_status(required_status)

    @classmethod
    def get_parsing_status_text(cls, parsing_status: Optional[int]) -> str:
        # 未知状态一律回落到“未开始”，避免对外暴露脏值。
        normalized = cls._normalize_parsing_status(parsing_status)
        return cls.PARSING_STATUS_TEXTS.get(
            normalized,
            cls.PARSING_STATUS_TEXTS[cls.PARSING_STATUS_PENDING],
        )

    @classmethod
    def _decorate_project_record(cls, project: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not project:
            return project
        decorated = dict(project)
        if decorated.get("report_url") is None:
            decorated["report_url"] = ""
        # 对所有项目查询结果补充状态标签，避免路由层重复拼装。
        normalized = cls._normalize_parsing_status(decorated.get("parsing_status"))
        decorated["parsing_status"] = normalized
        decorated["parsing_status_label"] = cls.PARSING_STATUS_LABELS[normalized]
        decorated["parsing_status_text"] = cls.get_parsing_status_text(normalized)
        decorated.update(upload_summary(decorated.get("upload_manifest")))
        if decorated.get("material_reference_missing"):
            decorated["upload_complete"] = False
            if not decorated["upload_issues"]:
                decorated["upload_issues"] = [{"status":"unbound", "name":"关联文件不存在或已删除，请补齐或解除关联"}]
        return decorated

    @staticmethod
    def _build_paginated_response(
        *,
        total: int,
        limit: int,
        offset: int,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """构建标准分页响应字典。"""
        page_size = max(1, limit)
        page = max(1, (offset // page_size) + 1)
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "limit": limit,
            "offset": offset,
            "items": items,
        }

    @classmethod
    def _normalize_document_type(cls, document_type: str) -> str:
        """验证文档类型是否在活跃列表中，返回小写。"""
        normalized = (document_type or "").strip().lower()
        if normalized not in cls.ACTIVE_DOCUMENT_TYPES:
            allowed = ", ".join(sorted(cls.ACTIVE_DOCUMENT_TYPES))
            raise ValueError(f"document_type 必须是以下之一：{allowed}")
        return normalized

    # 内部记录获取
    def _resolve_project_identifier(self, cursor, identifier_or_name: str) -> str:
        """将项目 UUID、Swagger 展示值或项目名解析为项目 UUID。"""
        self.assert_resource_access("project", identifier_or_name, cursor=cursor)
        normalized = self._normalize_required_identifier(identifier_or_name, "identifier_id")
        if UUID_TEXT_PATTERN.fullmatch(normalized):
            return normalized

        cursor.execute(
            f"""
            SELECT identifier_id
            FROM xtjs_projects
            WHERE project_name = %s AND deleted = FALSE AND {scope_sql("project", "xtjs_projects")}
            LIMIT 2
            """,
            (normalized,),
        )
        rows = cursor.fetchall()
        if len(rows) == 1:
            return str(rows[0]["identifier_id"] if isinstance(rows[0], dict) else rows[0][0])
        if len(rows) > 1:
            raise ValueError(f"项目名匹配到多个项目，请使用 UUID：{normalized}")
        return MISSING_UUID_SENTINEL

    def _resolve_document_identifier(self, cursor, identifier_or_file_name: str) -> str:
        """将文档 UUID、Swagger 展示值或文件名解析为文档 UUID。"""
        self.assert_resource_access("document", identifier_or_file_name, cursor=cursor)
        normalized = self._normalize_required_identifier(identifier_or_file_name, "identifier_id")
        if UUID_TEXT_PATTERN.fullmatch(normalized):
            return normalized

        cursor.execute(
            f"""
            SELECT identifier_id
            FROM xtjs_documents
            WHERE file_name = %s AND deleted = FALSE AND {scope_sql("document", "xtjs_documents")}
            LIMIT 2
            """,
            (normalized,),
        )
        rows = cursor.fetchall()
        if len(rows) == 1:
            return str(rows[0]["identifier_id"] if isinstance(rows[0], dict) else rows[0][0])
        if len(rows) > 1:
            raise ValueError(f"文件名匹配到多个文档，请选择带 UUID 的选项：{normalized}")
        return MISSING_UUID_SENTINEL

    def _get_project_record(self, cursor, identifier_id: str) -> Optional[Dict[str, Any]]:
        resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
        cursor.execute(
            """
            SELECT identifier_id, project_name, parsing_status, report_url
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
            """,
            (resolved_identifier,),
        )
        project = cursor.fetchone()
        return self._decorate_project_record(dict(project)) if project else None

    def _get_document_record(self, cursor, identifier_id: str) -> Optional[Dict[str, Any]]:
        resolved_identifier = self._resolve_document_identifier(cursor, identifier_id)
        cursor.execute(
            """
            SELECT identifier_id, document_type
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
            LIMIT 1
            """,
            (resolved_identifier,),
        )
        document = cursor.fetchone()
        return dict(document) if document else None

    def _get_required_document_record(
        self,
        cursor,
        identifier_id: str,
        *,
        role_label: str,
        allowed_types: set[str],
    ) -> Dict[str, Any]:
        """获取文档记录并校验其类型是否符合预期角色。"""
        document = self._get_document_record(cursor, identifier_id)
        if not document:
            raise ValueError(f"{role_label}不存在：{identifier_id}")

        document_type = str(document.get("document_type") or "").strip().lower()
        if document_type not in allowed_types:
            actual_label = get_document_type_label(document_type)
            expected = ", ".join(get_document_type_label(item) for item in sorted(allowed_types))
            raise ValueError(
                f"文档 '{identifier_id}' 必须是{role_label}，当前类型为 {actual_label}。"
                f"允许的类型：{expected}"
            )
        return document

    # 项目 CRUD
    @access_check(identifier_id='project')
    def initialize_upload_manifest(self, identifier_id, manifest):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE xtjs_projects SET upload_manifest=%s WHERE identifier_id=%s AND deleted=FALSE",
                               (Json(manifest), identifier_id))

    @access_check(identifier_id='project')
    def record_upload_file(self, identifier_id, slot, *, document_id=None, error=None):
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT upload_manifest FROM xtjs_projects WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE",
                               (identifier_id,))
                row = cursor.fetchone()
                if not row:
                    raise ValueError("项目不存在或已删除")
                manifest = row["upload_manifest"] or {}
                item = next((f for f in manifest.get("files", []) if f.get("slot") == slot), None)
                if item is None:
                    raise ValueError("文件不在项目上传清单内")
                item.update(status="uploaded" if document_id else "failed",
                            document_id=str(document_id) if document_id else None, error=error)
                cursor.execute("UPDATE xtjs_projects SET upload_manifest=%s, update_time=CURRENT_TIMESTAMP WHERE identifier_id=%s",
                               (Json(manifest), identifier_id))

    def _reconcile_upload_manifest(self, cursor, identifier_id):
        cursor.execute("SELECT xtjs_sync_materials(%s::uuid)", (str(identifier_id),))

    @access_check(identifier_id='project')
    def reconcile_upload_manifest(self, identifier_id):
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                self._reconcile_upload_manifest(cursor, identifier_id)

    def create_project(
        self,
        project_name: Optional[str] = None,
        identifier_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建项目，项目 UUID 默认由数据库生成。"""
        normalized_project_name = self._normalize_project_name(
            f"project-{uuid4().hex[:8]}" if project_name is None else project_name
        )
        normalized_identifier = (identifier_id or "").strip() or None
        if normalized_identifier:
            query = """
                INSERT INTO xtjs_projects (identifier_id, project_name, parsing_status)
                VALUES (%s, %s, %s)
                RETURNING identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
            """
            values = (normalized_identifier, normalized_project_name, self.PARSING_STATUS_UPLOADED)
        else:
            query = """
                INSERT INTO xtjs_projects (project_name, parsing_status)
                VALUES (%s, %s)
                RETURNING identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
            """
            values = (normalized_project_name, self.PARSING_STATUS_UPLOADED)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, values)
                return self._decorate_project_record(dict(cursor.fetchone()))

    def get_project_by_name(self, project_name: str) -> Optional[Dict[str, Any]]:
        """根据项目名称获取未删除项目。"""
        normalized_project_name = self._normalize_project_name(project_name)
        query = f"""
            SELECT EXISTS(SELECT 1 FROM xtjs_project_documents links
                   LEFT JOIN xtjs_documents td ON td.identifier_id=links.tender_document_id AND NOT td.deleted
                   LEFT JOIN xtjs_documents bd ON bd.identifier_id=links.business_bid_document_id AND NOT bd.deleted
                   LEFT JOIN xtjs_documents vd ON vd.identifier_id=links.technical_bid_document_id AND NOT vd.deleted
                   WHERE links.project_id=xtjs_projects.identifier_id AND
                   (td.identifier_id IS NULL OR bd.identifier_id IS NULL OR (links.technical_bid_document_id IS NOT NULL AND vd.identifier_id IS NULL))) AS material_reference_missing,
                EXISTS(SELECT 1 FROM xtjs_result r WHERE r.project_identifier_id=xtjs_projects.identifier_id AND r.input_revision<>xtjs_projects.input_revision) AS results_stale, identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE project_name = %s AND deleted = FALSE AND {scope_sql("project", "xtjs_projects")}
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized_project_name,))
                result = cursor.fetchone()
                return self._decorate_project_record(dict(result)) if result else None

    def list_projects(
        self,
        limit: int = 20,
        offset: int = 0,
        keyword: Optional[str] = None,
    ) -> Dict[str, Any]:
        """分页查询项目列表，支持关键字搜索。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        normalized_keyword = (keyword or "").strip()
        conditions = ["p.deleted = FALSE", scope_sql("project", "p")]
        values: List[Any] = []
        if normalized_keyword:
            conditions.append("(p.identifier_id::text ILIKE %s OR p.project_name ILIKE %s)")
            keyword_like = f"%{normalized_keyword}%"
            values.extend([keyword_like, keyword_like])
        where_clause = " AND ".join(conditions)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) AS total
                    FROM xtjs_projects p
                    WHERE {where_clause}
                    """,
                    tuple(values),
                )
                total = int(cursor.fetchone()["total"])
                cursor.execute(
                    f"""
                    SELECT
                        p.identifier_id,
                        p.project_name,
                        p.owner_user_id,
                        (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=p.owner_user_id) AS uploader_name,
                        p.parsing_status,
                        p.upload_manifest,
                        p.input_revision,
                        EXISTS(SELECT 1 FROM xtjs_project_documents links
                           LEFT JOIN xtjs_documents td ON td.identifier_id=links.tender_document_id AND NOT td.deleted
                           LEFT JOIN xtjs_documents bd ON bd.identifier_id=links.business_bid_document_id AND NOT bd.deleted
                           LEFT JOIN xtjs_documents vd ON vd.identifier_id=links.technical_bid_document_id AND NOT vd.deleted
                           WHERE links.project_id=p.identifier_id AND
                             (td.identifier_id IS NULL OR bd.identifier_id IS NULL OR (links.technical_bid_document_id IS NOT NULL AND vd.identifier_id IS NULL))) AS material_reference_missing,
                        EXISTS(SELECT 1 FROM xtjs_result stale WHERE stale.project_identifier_id=p.identifier_id AND stale.input_revision<>p.input_revision) AS results_stale,
                        p.report_url,
                        p.deleted,
                        p.create_time,
                        p.update_time,
                        COALESCE(rel.relation_count, 0) AS relation_count,
                        COALESCE(rel.tender_count, 0) AS tender_count,
                        COALESCE(rel.business_bid_count, 0) AS business_bid_count,
                        COALESCE(rel.technical_bid_count, 0) AS technical_bid_count,
                        COALESCE(rel.document_count, 0) AS document_count,
                        COALESCE(rel.extracted_document_count, 0) AS extracted_document_count,
                        COALESCE(rel.pending_document_count, 0) AS pending_document_count,
                        COALESCE(res.result_available, FALSE) AS result_available,
                        COALESCE(res.analysis_result_count, 0) AS analysis_result_count,
                        COALESCE(res.available_result_keys, '[]'::jsonb) AS available_result_keys,
                        res.result_update_time,
                        res.result_summary
                    FROM xtjs_projects p
                    LEFT JOIN LATERAL (
                        SELECT
                            COUNT(DISTINCT pd.id) AS relation_count,
                            COUNT(DISTINCT pd.tender_document_id) AS tender_count,
                            COUNT(DISTINCT pd.business_bid_document_id) AS business_bid_count,
                            COUNT(DISTINCT pd.technical_bid_document_id) AS technical_bid_count,
                            COUNT(DISTINCT docs.doc_id) AS document_count,
                            COUNT(DISTINCT CASE WHEN doc_meta.extracted = TRUE THEN docs.doc_id END) AS extracted_document_count,
                            COUNT(DISTINCT CASE WHEN doc_meta.extracted = FALSE THEN docs.doc_id END) AS pending_document_count
                        FROM xtjs_project_documents pd
                        LEFT JOIN (
                            SELECT pd2.tender_document_id AS doc_id
                            FROM xtjs_project_documents pd2
                            WHERE pd2.project_id = p.identifier_id
                            UNION
                            SELECT pd2.business_bid_document_id AS doc_id
                            FROM xtjs_project_documents pd2
                            WHERE pd2.project_id = p.identifier_id
                            UNION
                            SELECT pd2.technical_bid_document_id AS doc_id
                            FROM xtjs_project_documents pd2
                            WHERE pd2.project_id = p.identifier_id AND pd2.technical_bid_document_id IS NOT NULL
                        ) docs ON TRUE
                        LEFT JOIN xtjs_documents doc_meta
                            ON doc_meta.identifier_id = docs.doc_id
                           AND doc_meta.deleted = FALSE
                        WHERE pd.project_id = p.identifier_id
                    ) rel ON TRUE
                    LEFT JOIN LATERAL (
                        -- 结果外置后 result 列为 NULL：用轻量 result_keys 推导分析项统计；
                        -- 兼容未迁移历史行（result_keys 为空时回退用 result 顶层键）。
                        SELECT
                            (
                                jsonb_typeof(COALESCE(r.result_keys, '[]'::jsonb)) = 'array'
                                AND jsonb_array_length(COALESCE(r.result_keys, '[]'::jsonb)) > 0
                            )
                            OR (COALESCE(r.result, '{{}}'::jsonb) <> '{{}}'::jsonb) AS result_available,
                            CASE
                                WHEN jsonb_array_length(COALESCE(r.result_keys, '[]'::jsonb)) > 0
                                    THEN jsonb_array_length(r.result_keys)
                                ELSE COALESCE(
                                    (
                                        SELECT COUNT(*)
                                        FROM jsonb_object_keys(COALESCE(r.result, '{{}}'::jsonb)) AS result_key
                                    ),
                                    0
                                )
                            END AS analysis_result_count,
                            CASE
                                WHEN jsonb_array_length(COALESCE(r.result_keys, '[]'::jsonb)) > 0
                                    THEN r.result_keys
                                ELSE COALESCE(
                                    (
                                        SELECT jsonb_agg(result_key ORDER BY result_key)
                                        FROM jsonb_object_keys(COALESCE(r.result, '{{}}'::jsonb)) AS result_key
                                    ),
                                    '[]'::jsonb
                                )
                            END AS available_result_keys,
                            r.update_time AS result_update_time,
                            r.result_summary
                        FROM xtjs_result r
                        WHERE r.project_identifier_id = p.identifier_id AND r.input_revision=p.input_revision
                        LIMIT 1
                    ) res ON TRUE
                    WHERE {where_clause}
                    ORDER BY p.create_time DESC, p.identifier_id DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(values + [normalized_limit, normalized_offset]),
                )
                items: List[Dict[str, Any]] = [
                    self._decorate_project_record(dict(item)) for item in cursor.fetchall()
                ]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )




    @access_check(identifier_id='project')
    def get_project_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """根据标识获取项目记录。"""
        query = """
            SELECT EXISTS(SELECT 1 FROM xtjs_project_documents links
                   LEFT JOIN xtjs_documents td ON td.identifier_id=links.tender_document_id AND NOT td.deleted
                   LEFT JOIN xtjs_documents bd ON bd.identifier_id=links.business_bid_document_id AND NOT bd.deleted
                   LEFT JOIN xtjs_documents vd ON vd.identifier_id=links.technical_bid_document_id AND NOT vd.deleted
                   WHERE links.project_id=xtjs_projects.identifier_id AND
                   (td.identifier_id IS NULL OR bd.identifier_id IS NULL OR (links.technical_bid_document_id IS NOT NULL AND vd.identifier_id IS NULL))) AS material_reference_missing,
                EXISTS(SELECT 1 FROM xtjs_result r WHERE r.project_identifier_id=xtjs_projects.identifier_id AND r.input_revision<>xtjs_projects.input_revision) AS results_stale, identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (resolved_identifier,))
                result = cursor.fetchone()
                return self._decorate_project_record(dict(result)) if result else None

    @access_check(identifier_id='project')
    def update_project(
        self,
        identifier_id: str,
        project_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新项目名称。项目 UUID 不允许被业务接口修改。"""
        updates: List[str] = []
        values: List[Any] = []
        if project_name is not None:
            updates.append("project_name = %s")
            values.append(self._normalize_project_name(project_name))
        if not updates:
            raise ValueError("at least one project field must be provided")

        query = f"""
            UPDATE xtjs_projects
            SET {", ".join(updates)}, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, tuple(values + [resolved_identifier]))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    @access_check(identifier_id='project')
    def update_project_report_url(
        self,
        identifier_id: str,
        report_url: str,
    ) -> Optional[Dict[str, Any]]:
        """更新项目关联的前端 Word 报告地址。"""
        normalized_report_url = str(report_url or "").strip()
        query = """
            UPDATE xtjs_projects
            SET report_url = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute("SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE", (resolved_identifier,))
                current = cursor.fetchone()
                if current is None:
                    return None
                self.assert_input_revision(resolved_identifier, current["input_revision"])
                cursor.execute(query, (normalized_report_url, resolved_identifier))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    @access_check(identifier_id='project')
    def update_project_parsing_status(
        self,
        identifier_id: str,
        parsing_status: int,
        expected_input_revision: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        # 路由层统一通过这里同步项目 OCR 阶段状态。
        normalized_status = self._normalize_parsing_status(parsing_status)
        query = """
            UPDATE xtjs_projects
            SET parsing_status = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE AND (%s IS NULL OR input_revision=%s)
            RETURNING identifier_id, project_name, owner_user_id, (SELECT COALESCE(u.display_name,u.username) FROM xtjs_users u WHERE u.identifier_id=xtjs_projects.owner_user_id) AS uploader_name, parsing_status, report_url, upload_manifest, input_revision, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (normalized_status, normalized_identifier, expected_input_revision, expected_input_revision))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    @access_check(identifier_id='project')
    def get_project_ocr_metadata(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """Load OCR/workflow state without downloading OCR bodies or analysis reports."""
        project = self.get_project_by_identifier(identifier_id)
        if not project:
            return None
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT pd.id AS relation_id, slot.role AS relation_role,
                           d.identifier_id, d.document_type, d.file_name, d.extracted, d.ocr_last_error,
                           td.identifier_id AS tender_identifier_id, td.file_name AS tender_file_name,
                           td.extracted AS tender_extracted, td.ocr_last_error AS tender_ocr_last_error
                    FROM xtjs_project_documents pd
                    CROSS JOIN LATERAL (VALUES ('business_bid', pd.business_bid_document_id),
                                               ('technical_bid', pd.technical_bid_document_id)) AS slot(role, doc_id)
                    JOIN xtjs_documents d ON d.identifier_id=slot.doc_id AND d.deleted=FALSE
                    JOIN xtjs_documents td ON td.identifier_id=pd.tender_document_id AND td.deleted=FALSE
                    WHERE pd.project_id=%s ORDER BY pd.id,slot.role
                """, (project["identifier_id"],))
                documents = [dict(row) for row in cursor.fetchall()]
                cursor.execute("""SELECT workflow_scope,result_keys,result_summary,result_object_key,result,input_revision
                    FROM xtjs_result WHERE project_identifier_id=%s""", (project["identifier_id"],))
                result_meta = dict(cursor.fetchone() or {})
                if result_meta.get("input_revision", 0) != project.get("input_revision", 0):
                    result_meta["result_keys"] = []
                    result_meta["result_summary"] = {}
                    project["results_stale"] = True
        scope = result_meta.get("workflow_scope")
        if scope is None:
            # Compatibility for rows created before the metadata migration.
            scope = workflow_scope_from_result_record(self._sanitize_project_result_record(result_meta)) if result_meta else {}
        return {"project": project, "documents": documents, "workflow_scope": scope,
                "result_keys": result_meta.get("result_keys") or [],
                "result_summary": result_meta.get("result_summary") or {}}

    @access_check(identifier_id='document_write')
    def record_document_ocr_failure(self, identifier_id: str, failure: dict) -> bool:
        """Persist failures without changing materials, OCR bodies or old results.

        Late failures cannot mark recognized/deleted files as failed. Live progress
        takes precedence over the retained last failure while a retry runs.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""UPDATE xtjs_documents SET ocr_last_error=%s::jsonb
                    WHERE identifier_id=%s AND deleted=FALSE AND extracted=FALSE""",
                    (json.dumps(failure, ensure_ascii=False), identifier_id))
                return cursor.rowcount > 0

    @access_check(identifier_id='project')
    def refresh_project_parsing_status(self, identifier_id: str, payload=None) -> Optional[Dict[str, Any]]:
        """按项目下文档 extracted 状态重算 0/1/2/3 的 OCR 阶段。"""
        payload = payload if payload is not None else self.get_project_ocr_metadata(identifier_id)
        if not payload:
            return None

        project = payload.get("project") or {}
        normalized_identifier = str(project.get("identifier_id") or identifier_id)
        documents = list(payload.get("documents") or [])
        workflow_scope = payload.get("workflow_scope") or {}
        active_documents = filter_document_records(documents, workflow_scope)

        def unique_records(records: list[dict[str, Any]], role: str) -> dict[str, dict[str, Any]]:
            values: dict[str, dict[str, Any]] = {}
            for record in records:
                if str(record.get("relation_role") or "") != role:
                    continue
                doc_id = str(record.get("identifier_id") or "").strip()
                if doc_id:
                    values.setdefault(doc_id, record)
            return values

        tender_docs: dict[str, dict[str, Any]] = {}
        for record in documents:
            tender_id = str(record.get("tender_identifier_id") or "").strip()
            if tender_id:
                tender_docs.setdefault(
                    tender_id,
                    {"extracted": bool(record.get("tender_extracted"))},
                )
        business_docs = unique_records(documents, "business_bid")
        technical_docs = unique_records(active_documents, "technical_bid")

        def extracted_count(records: dict[str, dict[str, Any]]) -> int:
            return sum(1 for item in records.values() if bool(item.get("extracted")))

        if project.get("upload_complete") is False or not tender_docs or extracted_count(tender_docs) < len(tender_docs):
            next_status = self.PARSING_STATUS_PENDING
        elif not business_docs or extracted_count(business_docs) < len(business_docs):
            next_status = self.PARSING_STATUS_TENDER_OCR_COMPLETED
        elif not technical_docs or extracted_count(technical_docs) < len(technical_docs):
            next_status = self.PARSING_STATUS_BUSINESS_OCR_COMPLETED
        else:
            next_status = self.PARSING_STATUS_TECHNICAL_OCR_COMPLETED

        if int(project.get("parsing_status") or 0) == next_status:
            return project
        updated = self.update_project_parsing_status(normalized_identifier, next_status,
            expected_input_revision=project.get("input_revision"))
        if updated is None:
            raise ConsistencyConflict("材料在状态刷新期间发生变化，请刷新项目")
        return updated

    @access_check(identifier_id='project')
    def soft_delete_project(self, identifier_id: str) -> bool:
        """软删除项目（设置删除标记）。"""
        query = """
            UPDATE xtjs_projects
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (resolved_identifier,))
                return cursor.rowcount > 0

    @access_check(identifier_ids='project')
    def soft_delete_projects(self, identifier_ids: list[str]) -> int:
        """批量软删除项目。"""
        normalized_ids = [
            self._normalize_required_identifier(identifier_id, "identifier_id")
            for identifier_id in identifier_ids
            if str(identifier_id or "").strip()
        ]
        if not normalized_ids:
            return 0
        query = """
            UPDATE xtjs_projects
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = ANY(%s::uuid[]) AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM xtjs_review_issues WHERE project_identifier_id = ANY(%s::uuid[])",
                    (normalized_ids,),
                )
                cursor.execute(
                    "DELETE FROM xtjs_result_components WHERE project_identifier_id = ANY(%s::uuid[])",
                    (normalized_ids,),
                )
                cursor.execute(query, (normalized_ids,))
                return int(cursor.rowcount or 0)

    # 文档 CRUD
    def create_document(
        self,
        file_name: str,
        file_url: str,
        document_type: str,
        identifier_id: Optional[str] = None,
        source_file_hash: Optional[str] = None,
        source_file_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """创建文档记录（不含识别内容），文档 UUID 默认由数据库生成。"""
        identifier = (identifier_id or "").strip() or None
        normalized_file_name = self._normalize_file_value(file_name, "file_name")
        normalized_file_url = self._normalize_file_value(file_url, "file_url")
        normalized_document_type = self._normalize_document_type(document_type)

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if identifier:
                    existing = self._get_document_record(cursor, identifier)
                    if existing:
                        raise ValueError(f"文档标识已存在：{identifier}")
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            source_file_hash,
                            source_file_size
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            extracted,
                            content,
                            review_content,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id,
                            deleted,
                            create_time,
                            update_time
                        """,
                        (
                            identifier,
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                            source_file_hash,
                            source_file_size,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (
                            document_type,
                            file_name,
                            file_url,
                            source_file_hash,
                            source_file_size
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            extracted,
                            content,
                            review_content,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id,
                            deleted,
                            create_time,
                            update_time
                        """,
                        (
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                            source_file_hash,
                            source_file_size,
                        ),
                    )
                return dict(cursor.fetchone())

    def create_document_with_content(
        self,
        file_name: str,
        file_url: str,
        document_type: str,
        recognition_content: Dict[str, Any],
        identifier_id: Optional[str] = None,
        source_file_hash: Optional[str] = None,
        source_file_size: Optional[int] = None,
        ocr_cache_key: Optional[str] = None,
        ocr_engine_version: Optional[str] = None,
        ocr_config_hash: Optional[str] = None,
        ocr_cache_source_document_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建文档记录并同时写入识别内容，文档 UUID 默认由数据库生成。"""
        identifier = (identifier_id or "").strip() or None
        normalized_file_name = self._normalize_file_value(file_name, "file_name")
        normalized_file_url = self._normalize_file_value(file_url, "file_url")
        normalized_document_type = self._normalize_document_type(document_type)

        if not isinstance(recognition_content, dict):
            raise ValueError("recognition_content 必须是 JSON 对象")

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if identifier:
                    existing = self._get_document_record(cursor, identifier)
                    if existing:
                        raise ValueError(f"文档标识已存在：{identifier}")
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING identifier_id, document_type
                        """,
                        (
                            identifier,
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (
                            document_type,
                            file_name,
                            file_url,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING identifier_id, document_type
                        """,
                        (
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                            source_file_hash,
                            source_file_size,
                            ocr_cache_key,
                            ocr_engine_version,
                            ocr_config_hash,
                            ocr_cache_source_document_id,
                        ),
                    )
                document = dict(cursor.fetchone())

                # DB 瘦身：识别内容写 MinIO，库里只存对象键，content 置 NULL。
                # MinIO 写失败会抛异常 → 事务回滚（连带 INSERT），不会留下坏记录。
                content_object_key = document_blob_store.save_document_content(
                    recognition_content,
                    identifier_id=document["identifier_id"],
                    file_name=normalized_file_name,
                )

                cursor.execute(
                    """
                    UPDATE xtjs_documents
                    SET
                        content = NULL,
                        content_object_key = %s,
                        extracted = TRUE,
                        ocr_last_error = NULL,
                        source_file_hash = COALESCE(%s, source_file_hash),
                        source_file_size = COALESCE(%s, source_file_size),
                        ocr_cache_key = COALESCE(%s, ocr_cache_key),
                        ocr_engine_version = COALESCE(%s, ocr_engine_version),
                        ocr_config_hash = COALESCE(%s, ocr_config_hash),
                        ocr_cache_source_document_id = %s,
                        update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s
                    RETURNING
                        identifier_id,
                        document_type,
                        file_name,
                        file_url,
                        extracted,
                        content,
                        content_object_key,
                        review_content,
                        source_file_hash,
                        source_file_size,
                        ocr_cache_key,
                        ocr_engine_version,
                        ocr_config_hash,
                        ocr_cache_source_document_id,
                        deleted,
                        create_time,
                        update_time
                    """,
                    (
                        content_object_key,
                        source_file_hash,
                        source_file_size,
                        ocr_cache_key,
                        ocr_engine_version,
                        ocr_config_hash,
                        ocr_cache_source_document_id,
                        document["identifier_id"],
                    ),
                )
                updated_document = dict(cursor.fetchone())
                identity = self._homepage_identity(recognition_content)
                cursor.execute("UPDATE xtjs_documents SET bidder_identity=%s WHERE identifier_id=%s AND document_type IN ('business_bid','technical_bid')", (Json(identity), document['identifier_id']))
                updated_document['bidder_identity'] = identity if normalized_document_type in ('business_bid','technical_bid') else None
                # 返回给上层时补回 content，保持既有契约（上层会 compact 剥离）。
                updated_document["content"] = recognition_content

                return {"document": updated_document}

    def list_documents(
        self,
        limit: int = 20,
        offset: int = 0,
        keyword: Optional[str] = None,
        document_type: Optional[str] = None,
        extracted: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """分页查询文档列表，支持多种过滤。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        normalized_keyword = (keyword or "").strip()
        normalized_document_type = (document_type or "").strip().lower()
        conditions = ["deleted = FALSE", scope_sql("document", "xtjs_documents")]
        values: List[Any] = []
        if normalized_keyword:
            conditions.append("(identifier_id::text ILIKE %s OR file_name ILIKE %s)")
            keyword_like = f"%{normalized_keyword}%"
            values.extend([keyword_like, keyword_like])
        if normalized_document_type:
            conditions.append("document_type = %s")
            values.append(self._normalize_document_type(normalized_document_type))
        if extracted is not None:
            conditions.append("extracted = %s")
            values.append(bool(extracted))
        where_clause = " AND ".join(conditions)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT COUNT(*) AS total
                    FROM xtjs_documents
                    WHERE {where_clause}
                    """,
                    tuple(values),
                )
                total = int(cursor.fetchone()["total"])
                cursor.execute(
                    f"""
                    SELECT
                        identifier_id,
                        document_type,
                bidder_identity,
                        file_name,
                        file_url,
                        extracted,
                        content,
                        review_content,
                        source_file_hash,
                        source_file_size,
                        ocr_cache_key,
                        ocr_engine_version,
                        ocr_config_hash,
                        ocr_cache_source_document_id,
                        deleted,
                        create_time,
                        update_time
                    FROM xtjs_documents
                    WHERE {where_clause}
                    ORDER BY create_time DESC, identifier_id DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(values + [normalized_limit, normalized_offset]),
                )
                items: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )

    @access_check(identifier_id='document')
    def get_document_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """根据标识获取文档完整信息。"""
        query = """
            SELECT
                identifier_id,
                document_type,
                bidder_identity,
                file_name,
                file_url,
                extracted,
                content,
                content_object_key,
                review_content,
                review_content_object_key,
                source_file_hash,
                source_file_size,
                ocr_cache_key,
                ocr_engine_version,
                ocr_config_hash,
                ocr_cache_source_document_id,
                deleted,
                create_time,
                update_time
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(query, (resolved_identifier,))
                result = cursor.fetchone()
                if not result:
                    return None
                # content / review_content 外置后从 MinIO 取回填充，保持透明可用。
                document = document_blob_store.hydrate_document_content(dict(result))
                return document_blob_store.hydrate_document_review_content(document)

    @access_check(identifier_id='document')
    def get_document_review_content(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """Return the normalized manual OCR working copy for a document."""
        document = self.get_document_by_identifier(identifier_id)
        if not document:
            return None
        return {
            "identifier_id": str(document["identifier_id"]),
            "document_type": document.get("document_type"),
            "file_name": document.get("file_name"),
            "review_content": normalize_review_content(
                document.get("review_content"),
                content=document.get("content"),
            ),
        }

    @access_check(identifier_id='document_write')
    def update_document_review_content(
        self,
        identifier_id: str,
        *,
        effective_content: Dict[str, Any],
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Save a manual OCR working copy while keeping raw content unchanged."""
        if not isinstance(effective_content, dict):
            raise ValueError("effective_content must be a JSON object")
        if inputs is not None and not isinstance(inputs, dict):
            raise ValueError("inputs must be a JSON object")

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(
                    """
                    SELECT identifier_id, file_name, content, content_object_key,
                           review_content, review_content_object_key
                    FROM xtjs_documents
                    WHERE identifier_id = %s AND deleted = FALSE
                    FOR UPDATE
                    """,
                    (normalized_identifier,),
                )
                document = cursor.fetchone()
                if not document:
                    return None
                # content / review_content 外置后从 MinIO 取回，供工作副本基线与合并使用。
                document = document_blob_store.hydrate_document_content(dict(document))
                document = document_blob_store.hydrate_document_review_content(document)
                existing = normalize_review_content(
                    document.get("review_content"),
                    content=document.get("content"),
                )
                next_inputs = dict(existing.get("inputs") or {})
                next_inputs.update(dict(inputs or {}))
                review_content = build_review_content(
                    content=document.get("content"),
                    existing_review_content=document.get("review_content"),
                    effective_content=effective_content,
                )
                review_content["inputs"] = next_inputs
                # review_content 外置：写 MinIO，库里只存对象键、review_content 置 NULL。
                review_object_key = document_blob_store.save_document_review_content(
                    jsonable_encoder(review_content),
                    identifier_id=normalized_identifier,
                    file_name=document.get("file_name"),
                )
                cursor.execute(
                    """
                    UPDATE xtjs_documents
                    SET review_content = NULL,
                        review_content_object_key = %s,
                        update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s AND deleted = FALSE
                    RETURNING identifier_id
                    """,
                    (review_object_key, normalized_identifier),
                )
                updated = cursor.fetchone()
                if not updated:
                    return None
                return {
                    "identifier_id": str(updated["identifier_id"]),
                    "review_content": normalize_review_content(
                        review_content,
                        content=document.get("content"),
                    ),
                }

    @access_check(identifier_id='document_write')
    def update_document_review_input(
        self,
        identifier_id: str,
        *,
        input_key: str,
        input_value: Dict[str, Any],
        effective_content: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update one business-domain input in a document working copy."""
        normalized_input_key = str(input_key or "").strip()
        if not normalized_input_key:
            raise ValueError("input_key is required")
        if not isinstance(input_value, dict):
            raise ValueError("input_value must be a JSON object")

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(
                    """
                    SELECT identifier_id, file_name, content, content_object_key,
                           review_content, review_content_object_key
                    FROM xtjs_documents
                    WHERE identifier_id = %s AND deleted = FALSE
                    FOR UPDATE
                    """,
                    (normalized_identifier,),
                )
                document = cursor.fetchone()
                if not document:
                    return None
                # content / review_content 外置后从 MinIO 取回，供工作副本基线与合并使用。
                document = document_blob_store.hydrate_document_content(dict(document))
                document = document_blob_store.hydrate_document_review_content(document)
                next_effective = (
                    effective_content
                    if effective_content is not None
                    else effective_document_content(dict(document))
                )
                review_content = build_review_content(
                    content=document.get("content"),
                    existing_review_content=document.get("review_content"),
                    effective_content=next_effective,
                    input_key=normalized_input_key,
                    input_value=input_value,
                )
                # review_content 外置：写 MinIO，库里只存对象键、review_content 置 NULL。
                review_object_key = document_blob_store.save_document_review_content(
                    jsonable_encoder(review_content),
                    identifier_id=normalized_identifier,
                    file_name=document.get("file_name"),
                )
                cursor.execute(
                    """
                    UPDATE xtjs_documents
                    SET review_content = NULL,
                        review_content_object_key = %s,
                        update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s AND deleted = FALSE
                    RETURNING identifier_id
                    """,
                    (review_object_key, normalized_identifier),
                )
                updated = cursor.fetchone()
                if not updated:
                    return None
                return {
                    "identifier_id": str(updated["identifier_id"]),
                    "review_content": normalize_review_content(
                        review_content,
                        content=document.get("content"),
                    ),
                }

    @access_check(identifier_id='document_write')
    def update_document(
        self,
        identifier_id: str,
        file_name: Optional[str] = None,
        file_url: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新文档的文件名或存储 URL。"""
        updates: List[str] = []
        values: List[Any] = []

        if file_name is not None:
            updates.append("file_name = %s")
            values.append(self._normalize_file_value(file_name, "file_name"))
        if file_url is not None:
            updates.append("file_url = %s")
            values.append(self._normalize_file_value(file_url, "file_url"))
        if not updates:
            raise ValueError("file_name 和 file_url 至少需要提供一个")

        updates.append("update_time = CURRENT_TIMESTAMP")
        query = f"""
            UPDATE xtjs_documents
            SET {", ".join(updates)}
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
                review_content,
                source_file_hash,
                source_file_size,
                ocr_cache_key,
                ocr_engine_version,
                ocr_config_hash,
                ocr_cache_source_document_id,
                deleted,
                create_time,
                update_time
        """

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                did = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute("SELECT file_url FROM xtjs_documents WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE", (did,))
                current = cursor.fetchone()
                if current is None:
                    return None
                if file_url is not None:
                    from app.service.document_ingest_service import normalize_file_url
                    if normalize_file_url(file_url) != normalize_file_url(current["file_url"]):
                        raise ConsistencyConflict("原件地址不能原地替换，请上传新文档并替换项目关联")
                values.append(did)
                cursor.execute(query, tuple(values))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    @access_check(identifier_id='document_write')
    def update_document_content(
        self,
        identifier_id: str,
        recognition_content: Dict[str, Any],
        source_file_hash: Optional[str] = None,
        source_file_size: Optional[int] = None,
        ocr_cache_key: Optional[str] = None,
        ocr_engine_version: Optional[str] = None,
        ocr_config_hash: Optional[str] = None,
        ocr_cache_source_document_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """覆盖写入文档的识别内容，并标记为已提取。"""
        if not isinstance(recognition_content, dict):
            raise ValueError("recognition_content 必须是 JSON 对象")

        query = """
            UPDATE xtjs_documents
            SET
                content = NULL,
                content_object_key = %s,
                extracted = TRUE,
                ocr_last_error = NULL,
                source_file_hash = COALESCE(%s, source_file_hash),
                source_file_size = COALESCE(%s, source_file_size),
                ocr_cache_key = COALESCE(%s, ocr_cache_key),
                ocr_engine_version = COALESCE(%s, ocr_engine_version),
                ocr_config_hash = COALESCE(%s, ocr_config_hash),
                ocr_cache_source_document_id = %s,
                update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
                content_object_key,
                review_content,
                source_file_hash,
                source_file_size,
                ocr_cache_key,
                ocr_engine_version,
                ocr_config_hash,
                ocr_cache_source_document_id,
                deleted,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute("SELECT identifier_id FROM xtjs_documents WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE", (normalized_identifier,))
                if cursor.fetchone() is None:
                    return None
                # DB 瘦身：识别内容写 MinIO，库里只存对象键。MinIO 失败抛错→事务回滚。
                content_object_key = document_blob_store.save_document_content(
                    recognition_content,
                    identifier_id=normalized_identifier,
                )
                cursor.execute(
                    query,
                    (
                        content_object_key,
                        source_file_hash,
                        source_file_size,
                        ocr_cache_key,
                        ocr_engine_version,
                        ocr_config_hash,
                        ocr_cache_source_document_id,
                        normalized_identifier,
                    ),
                )
                updated = cursor.fetchone()
                if not updated:
                    return None
                updated = dict(updated)
                identity = self._homepage_identity(recognition_content)
                cursor.execute("UPDATE xtjs_documents SET bidder_identity=%s WHERE identifier_id=%s AND document_type IN ('business_bid','technical_bid')", (Json(identity), normalized_identifier))
                updated['bidder_identity'] = identity if updated.get('document_type') in ('business_bid','technical_bid') else None
                updated["content"] = recognition_content
                return updated

    @access_check(identifier_id='document_write')
    def update_document_source_metadata(
        self,
        identifier_id: str,
        *,
        source_file_hash: str,
        source_file_size: int,
    ) -> Optional[Dict[str, Any]]:
        """Persist source file hash/size for documents created before OCR runs."""
        query = """
            UPDATE xtjs_documents
            SET
                source_file_hash = %s,
                source_file_size = %s,
                update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
                review_content,
                source_file_hash,
                source_file_size,
                ocr_cache_key,
                ocr_engine_version,
                ocr_config_hash,
                ocr_cache_source_document_id,
                deleted,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(
                    query,
                    (
                        source_file_hash,
                        source_file_size,
                        normalized_identifier,
                    ),
                )
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def find_reusable_ocr_document(
        self,
        *,
        source_file_hash: str,
        document_type: str,
        ocr_engine_version: str,
        ocr_config_hash: str,
        exclude_identifier_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Find an extracted document with equivalent source and OCR configuration."""
        normalized_hash = str(source_file_hash or "").strip()
        normalized_type = self._normalize_document_type(document_type)
        normalized_engine = str(ocr_engine_version or "").strip()
        normalized_config = str(ocr_config_hash or "").strip()
        if not normalized_hash or not normalized_engine or not normalized_config:
            return None

        conditions = [scope_sql("document", "xtjs_documents"),
            "deleted = FALSE",
            "extracted = TRUE",
            "(content IS NOT NULL OR content_object_key IS NOT NULL)",
            "source_file_hash = %s",
            "document_type = %s",
            "ocr_engine_version = %s",
            "ocr_config_hash = %s",
        ]
        values: list[Any] = [
            normalized_hash,
            normalized_type,
            normalized_engine,
            normalized_config,
        ]
        normalized_exclude = self._extract_identifier(exclude_identifier_id)
        if normalized_exclude:
            conditions.append("identifier_id <> %s")
            values.append(normalized_exclude)

        query = f"""
            SELECT
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
                content_object_key,
                review_content,
                source_file_hash,
                source_file_size,
                ocr_cache_key,
                ocr_engine_version,
                ocr_config_hash,
                ocr_cache_source_document_id,
                deleted,
                create_time,
                update_time
            FROM xtjs_documents
            WHERE {" AND ".join(conditions)}
            ORDER BY update_time DESC, create_time DESC, identifier_id DESC
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(values))
                result = cursor.fetchone()
                if not result:
                    return None
                # OCR 缓存复用需要完整 content：外置后从 MinIO 取回。
                return document_blob_store.hydrate_document_content(dict(result))

    @access_check(identifier_id='document_write')
    def soft_delete_document(self, identifier_id: str) -> bool:
        """软删除文档。"""
        query = """
            UPDATE xtjs_documents
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                resolved_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(query, (resolved_identifier,))
                return cursor.rowcount > 0

    @access_check(identifier_ids='document_write')
    def soft_delete_documents(self, identifier_ids: list[str]) -> int:
        """批量软删除文档。"""
        normalized_ids = [
            self._normalize_required_identifier(identifier_id, "identifier_id")
            for identifier_id in identifier_ids
            if str(identifier_id or "").strip()
        ]
        if not normalized_ids:
            return 0
        query = """
            UPDATE xtjs_documents
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = ANY(%s::uuid[]) AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (normalized_ids,))
                return int(cursor.rowcount or 0)

    # 独立招标文件审查
    @access_check(document_identifier_id='document')
    def create_tender_review(self, document_identifier_id: str) -> Dict[str, Any]:
        """为一个独立招标文档创建审查记录。"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                document_id = self._resolve_document_identifier(cursor, document_identifier_id)
                cursor.execute(
                    """
                    INSERT INTO xtjs_tender_reviews (document_identifier_id, status)
                    VALUES (%s, 'running')
                    RETURNING *
                    """,
                    (document_id,),
                )
                return dict(cursor.fetchone())

    @access_check(review_identifier_id='review')
    def complete_tender_review(
        self,
        review_identifier_id: str,
        *,
        result_object_key: str,
        summary: Dict[str, Any],
        page_count: int = 0,
        text_length: int = 0,
    ) -> Dict[str, Any]:
        """写入最新审查汇总和结果对象键。"""
        review_id = self._normalize_required_identifier(review_identifier_id, "review_identifier_id")
        query = """
            UPDATE xtjs_tender_reviews
            SET status = 'completed',
                overall_status = %s,
                passed_count = %s,
                failed_count = %s,
                unclear_count = %s,
                page_count = %s,
                text_length = %s,
                result_object_key = %s,
                error_message = NULL,
                update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING *
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        str(summary.get("overall_status") or "unclear"),
                        int(summary.get("passed") or 0),
                        int(summary.get("failed") or 0),
                        int(summary.get("unclear") or 0),
                        max(0, int(page_count or 0)),
                        max(0, int(text_length or 0)),
                        str(result_object_key or "").strip(),
                        review_id,
                    ),
                )
                result = cursor.fetchone()
                if not result:
                    raise ValueError("招标文件审查记录不存在")
                return dict(result)

    @access_check(review_identifier_id='review')
    def fail_tender_review(self, review_identifier_id: str, error_message: str) -> Optional[Dict[str, Any]]:
        """记录审查失败，保留文档供用户重新审查。"""
        review_id = self._normalize_required_identifier(review_identifier_id, "review_identifier_id")
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE xtjs_tender_reviews
                    SET status = 'failed', error_message = %s, update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s AND deleted = FALSE
                    RETURNING *
                    """,
                    (str(error_message or "审查失败")[:2000], review_id),
                )
                result = cursor.fetchone()
                return dict(result) if result else None

    @access_check(review_identifier_id='review')
    def get_tender_review(self, review_identifier_id: str) -> Optional[Dict[str, Any]]:
        """读取审查记录及其独立文档摘要。"""
        review_id = self._normalize_required_identifier(review_identifier_id, "review_identifier_id")
        query = """
            SELECT
                r.*,
                d.file_name,
                d.file_url,
                d.document_type,
                d.extracted,
                d.source_file_hash,
                d.source_file_size,
                d.create_time AS document_create_time
            FROM xtjs_tender_reviews r
            JOIN xtjs_documents d ON d.identifier_id = r.document_identifier_id
            WHERE r.identifier_id = %s
              AND r.deleted = FALSE
              AND d.deleted = FALSE
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (review_id,))
                result = cursor.fetchone()
                return dict(result) if result else None

    def list_tender_reviews(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """分页读取所有登录用户共享的独立审查历史。"""
        normalized_limit = max(1, min(int(limit or 20), 200))
        normalized_offset = max(0, int(offset or 0))
        where_clause = "r.deleted = FALSE AND d.deleted = FALSE AND " + scope_sql("document", "d")
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""SELECT COUNT(*) AS total
                        FROM xtjs_tender_reviews r
                        JOIN xtjs_documents d ON d.identifier_id = r.document_identifier_id
                        WHERE {where_clause}"""
                )
                total = int(cursor.fetchone()["total"])
                cursor.execute(
                    f"""
                    SELECT
                        r.identifier_id,
                        r.document_identifier_id,
                        r.status,
                        r.overall_status,
                        r.passed_count,
                        r.failed_count,
                        r.unclear_count,
                        r.page_count,
                        r.text_length,
                        r.error_message,
                        r.create_time,
                        r.update_time,
                        d.file_name,
                        d.source_file_size
                    FROM xtjs_tender_reviews r
                    JOIN xtjs_documents d ON d.identifier_id = r.document_identifier_id
                    WHERE {where_clause}
                    ORDER BY r.update_time DESC, r.identifier_id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (normalized_limit, normalized_offset),
                )
                items = [dict(item) for item in cursor.fetchall()]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )

    @access_check(review_identifier_id='review')
    def soft_delete_tender_review(self, review_identifier_id: str) -> bool:
        """在同一事务内软删除审查记录及其独立文档。"""
        review_id = self._normalize_required_identifier(review_identifier_id, "review_identifier_id")
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT document_identifier_id
                    FROM xtjs_tender_reviews
                    WHERE identifier_id = %s AND deleted = FALSE
                    FOR UPDATE
                    """,
                    (review_id,),
                )
                record = cursor.fetchone()
                if not record:
                    return False
                self.assert_resource_access("document_write", record["document_identifier_id"], cursor=cursor)
                cursor.execute(
                    """UPDATE xtjs_tender_reviews
                       SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
                       WHERE identifier_id = %s""",
                    (review_id,),
                )
                cursor.execute(
                    """UPDATE xtjs_documents
                       SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
                       WHERE identifier_id = %s AND deleted = FALSE""",
                    (record["document_identifier_id"],),
                )
                return True

    # 项目-文档关系管理
    @access_check(project_identifier='project')
    @access_check(tender_document_identifier='document', business_bid_document_identifier='document', technical_bid_document_identifier='document')
    def bind_project_documents(
        self,
        project_identifier: str,
        tender_document_identifier: str,
        business_bid_document_identifier: str,
        technical_bid_document_identifier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """绑定招标、商务标、技术标到项目，并校验文档类型。"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                project = self._get_project_record(cursor, project_identifier)
                if not project:
                    raise ValueError(f"项目不存在：{project_identifier}")

                cursor.execute("SELECT identifier_id FROM xtjs_projects WHERE identifier_id=%s FOR UPDATE", (project["identifier_id"],))

                tender = self._get_required_document_record(
                    cursor,
                    tender_document_identifier,
                    role_label="招标文件",
                    allowed_types={DOCUMENT_TYPE_TENDER},
                )
                business_bid = self._get_required_document_record(
                    cursor,
                    business_bid_document_identifier,
                    role_label="商务标文件",
                    allowed_types=set(BUSINESS_BID_COMPATIBLE_TYPES),
                )
                normalized_technical_identifier = (
                    (technical_bid_document_identifier or "").strip() or None
                )
                technical_bid = None
                if normalized_technical_identifier:
                    technical_bid = self._get_required_document_record(
                        cursor,
                        normalized_technical_identifier,
                        role_label="技术标文件",
                        allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                    )

                # 检查是否已存在完全相同的绑定关系
                cursor.execute(
                    """
                    SELECT *
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND business_bid_document_id = %s
                      AND technical_bid_document_id IS NOT DISTINCT FROM %s
                    LIMIT 1
                    """,
                    (
                        project["identifier_id"],
                        tender["identifier_id"],
                        business_bid["identifier_id"],
                        technical_bid["identifier_id"] if technical_bid else None,
                    ),
                )
                duplicated = cursor.fetchone()
                if duplicated:
                    return {**dict(duplicated), "project_identifier": project["identifier_id"],
                            "tender_document_identifier": tender["identifier_id"],
                            "business_bid_document_identifier": business_bid["identifier_id"],
                            "technical_bid_document_identifier": technical_bid["identifier_id"] if technical_bid else None}

                cursor.execute(
                    """
                    INSERT INTO xtjs_project_documents (
                        project_id,
                        tender_document_id,
                        business_bid_document_id,
                        technical_bid_document_id
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING
                        id,
                        project_id,
                        tender_document_id,
                        business_bid_document_id,
                        technical_bid_document_id,
                        create_time
                    """,
                    (
                        project["identifier_id"],
                        tender["identifier_id"],
                        business_bid["identifier_id"],
                        technical_bid["identifier_id"] if technical_bid else None,
                    ),
                )
                binding = dict(cursor.fetchone())
                self._reconcile_upload_manifest(cursor, project["identifier_id"])
                return {
                    **binding,
                    "project_identifier": project["identifier_id"],
                    "tender_document_identifier": tender["identifier_id"],
                    "business_bid_document_identifier": business_bid["identifier_id"],
                    "technical_bid_document_identifier": (
                        technical_bid["identifier_id"] if technical_bid else None
                    ),
                }

    @staticmethod
    def _homepage_identity(content):
        from app.service.analysis.bidder_identity import identify
        payload = content.get('data') if isinstance(content.get('data'), dict) else content
        return identify(payload, payload.get('layout_sections') or [])

    @staticmethod
    def _relation_identity(row):
        from app.service.analysis.bidder_identity import combine
        result = dict(row)
        result['bidder_identity'] = combine(
            ('business', result.pop('business_bid_identity', None) or {}),
            ('technical', result.pop('technical_bid_identity', None) or {}))
        return result

    @access_check(relation_id='relation')
    def get_relation_by_id(self, relation_id: int) -> Optional[Dict[str, Any]]:
        """根据关系 ID 获取绑定详情。"""
        query = """
            SELECT
                pd.id AS relation_id,
                p.identifier_id AS project_identifier,
                p.project_name,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                bbd.identifier_id AS business_bid_identifier_id,
                bbd.document_type AS business_bid_document_type,
                bbd.file_name AS business_bid_file_name,
                bbd.file_url AS business_bid_file_url,
                bbd.bidder_identity AS business_bid_identity,
                tbd.bidder_identity AS technical_bid_identity,
                tbd.identifier_id AS technical_bid_identifier_id,
                tbd.document_type AS technical_bid_document_type,
                tbd.file_name AS technical_bid_file_name,
                tbd.file_url AS technical_bid_file_url,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_projects p ON pd.project_id = p.identifier_id AND p.deleted = FALSE
            JOIN xtjs_documents td ON pd.tender_document_id = td.identifier_id AND td.deleted = FALSE
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.identifier_id AND bbd.deleted = FALSE
            LEFT JOIN xtjs_documents tbd
                ON pd.technical_bid_document_id = tbd.identifier_id AND tbd.deleted = FALSE
            WHERE pd.id = %s
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (relation_id,))
                relation = cursor.fetchone()
                return self._relation_identity(relation) if relation else None

    @access_check(project_identifier='project')
    def list_relations(
        self,
        limit: int = 20,
        offset: int = 0,
        keyword: Optional[str] = None,
        project_identifier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """分页查询项目文档绑定关系列表。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        normalized_keyword = (keyword or "").strip()
        normalized_project_identifier = (project_identifier or "").strip()
        conditions = [
            scope_sql("project", "p"),
            "p.deleted = FALSE",
            "td.deleted = FALSE",
            "bbd.deleted = FALSE",
        ]
        values: List[Any] = []
        if normalized_project_identifier:
            conditions.append("(p.identifier_id::text = %s OR p.project_name = %s)")
            values.extend([
                self._extract_identifier(normalized_project_identifier),
                normalized_project_identifier,
            ])
        if normalized_keyword:
            keyword_like = f"%{normalized_keyword}%"
            conditions.append(
                """
                (
                    p.identifier_id::text ILIKE %s
                    OR p.project_name ILIKE %s
                    OR td.identifier_id::text ILIKE %s
                    OR td.file_name ILIKE %s
                    OR bbd.identifier_id::text ILIKE %s
                    OR bbd.file_name ILIKE %s
                    OR COALESCE(tbd.identifier_id::text, '') ILIKE %s
                    OR COALESCE(tbd.file_name, '') ILIKE %s
                )
                """
            )
            values.extend([keyword_like] * 8)
        where_clause = " AND ".join(conditions)

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM xtjs_project_documents pd
            JOIN xtjs_projects p ON pd.project_id = p.identifier_id
            JOIN xtjs_documents td ON pd.tender_document_id = td.identifier_id
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.identifier_id
            LEFT JOIN xtjs_documents tbd ON pd.technical_bid_document_id = tbd.identifier_id
            WHERE {where_clause}
        """
        data_query = f"""
            SELECT
                pd.id AS relation_id,
                p.identifier_id AS project_identifier,
                p.project_name,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                bbd.identifier_id AS business_bid_identifier_id,
                bbd.document_type AS business_bid_document_type,
                bbd.file_name AS business_bid_file_name,
                bbd.file_url AS business_bid_file_url,
                bbd.bidder_identity AS business_bid_identity,
                tbd.bidder_identity AS technical_bid_identity,
                tbd.identifier_id AS technical_bid_identifier_id,
                tbd.document_type AS technical_bid_document_type,
                tbd.file_name AS technical_bid_file_name,
                tbd.file_url AS technical_bid_file_url,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_projects p ON pd.project_id = p.identifier_id
            JOIN xtjs_documents td ON pd.tender_document_id = td.identifier_id
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.identifier_id
            LEFT JOIN xtjs_documents tbd ON pd.technical_bid_document_id = tbd.identifier_id
            WHERE {where_clause}
            ORDER BY pd.create_time DESC, pd.id DESC
            LIMIT %s OFFSET %s
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(count_query, tuple(values))
                total = int(cursor.fetchone()["total"])
                cursor.execute(data_query, tuple(values + [normalized_limit, normalized_offset]))
                items: List[Dict[str, Any]] = [self._relation_identity(item) for item in cursor.fetchall()]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )

    @access_check(project_identifier='project', old_identifier='document', new_identifier='document')
    def replace_project_document(self, project_identifier, old_identifier, new_identifier, role, expected_revision):
        """Replace one role within one project atomically; retain both document records."""
        columns = {"tender": "tender_document_id", "business_bid": "business_bid_document_id", "technical_bid": "technical_bid_document_id"}
        if role not in columns:
            raise ValueError("不支持替换的文件类型")
        column = columns[role]
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s AND NOT deleted FOR UPDATE", (project_identifier,))
            project = cursor.fetchone()
            if not project or project["input_revision"] != expected_revision:
                raise ConsistencyConflict("项目材料已变化，请刷新后重新替换。原关联未修改。")
            cursor.execute("SELECT document_type,extracted FROM xtjs_documents WHERE identifier_id=%s AND NOT deleted FOR SHARE", (new_identifier,))
            new_doc = cursor.fetchone()
            if not new_doc or new_doc["document_type"] != role or not new_doc["extracted"]:
                raise ValueError("新文件尚未完成识别，原关联未修改")
            cursor.execute(f"UPDATE xtjs_project_documents SET {column}=%s WHERE project_id=%s AND {column}=%s RETURNING id",
                           (new_identifier, project_identifier, old_identifier))
            changed = cursor.fetchall()
            if not changed:
                raise ConsistencyConflict("原文件已不属于该项目，请刷新后重试。")
            # Existing relation triggers synchronize manifest, parsing status,
            # input revision and historical result references in this transaction.
            return {"replaced_relation_count": len(changed)}

    @access_check(relation_id='relation')
    @access_check(tender_document_identifier='document', business_bid_document_identifier='document', technical_bid_document_identifier='document')
    def update_relation(
        self,
        relation_id: int,
        tender_document_identifier: str,
        business_bid_document_identifier: str,
        technical_bid_document_identifier: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新已有的项目文档绑定关系。"""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, project_id
                    FROM xtjs_project_documents
                    WHERE id = %s
                    """,
                    (relation_id,),
                )
                relation = cursor.fetchone()
                if not relation:
                    return None

                cursor.execute("SELECT identifier_id FROM xtjs_projects WHERE identifier_id=%s FOR UPDATE", (relation["project_id"],))

                tender = self._get_required_document_record(
                    cursor,
                    tender_document_identifier,
                    role_label="招标文件",
                    allowed_types={DOCUMENT_TYPE_TENDER},
                )
                business_bid = self._get_required_document_record(
                    cursor,
                    business_bid_document_identifier,
                    role_label="商务标文件",
                    allowed_types=set(BUSINESS_BID_COMPATIBLE_TYPES),
                )
                normalized_technical_identifier = (
                    (technical_bid_document_identifier or "").strip() or None
                )
                technical_bid = None
                if normalized_technical_identifier:
                    technical_bid = self._get_required_document_record(
                        cursor,
                        normalized_technical_identifier,
                        role_label="技术标文件",
                        allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                    )

                # 检查新组合是否与其他记录冲突
                cursor.execute(
                    """
                    SELECT id
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND business_bid_document_id = %s
                      AND technical_bid_document_id IS NOT DISTINCT FROM %s
                      AND id <> %s
                    LIMIT 1
                    """,
                    (
                        relation["project_id"],
                        tender["identifier_id"],
                        business_bid["identifier_id"],
                        technical_bid["identifier_id"] if technical_bid else None,
                        relation_id,
                    ),
                )
                duplicated = cursor.fetchone()
                if duplicated:
                    raise ValueError(
                        "当前招标文件、商务标文件、技术标文件的关联关系已存在"
                    )

                cursor.execute(
                    """
                    UPDATE xtjs_project_documents
                    SET
                        tender_document_id = %s,
                        business_bid_document_id = %s,
                        technical_bid_document_id = %s
                    WHERE id = %s
                    RETURNING
                        id,
                        project_id,
                        tender_document_id,
                        business_bid_document_id,
                        technical_bid_document_id,
                        create_time
                    """,
                    (
                        tender["identifier_id"],
                        business_bid["identifier_id"],
                        technical_bid["identifier_id"] if technical_bid else None,
                        relation_id,
                    ),
                )
                updated = dict(cursor.fetchone())

                cursor.execute(
                    """
                    SELECT identifier_id
                    FROM xtjs_projects
                    WHERE identifier_id = %s
                    """,
                    (updated["project_id"],),
                )
                project = cursor.fetchone()
                project_identifier = project["identifier_id"] if project else ""
                return {
                    **updated,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender["identifier_id"],
                    "business_bid_document_identifier": business_bid["identifier_id"],
                    "technical_bid_document_identifier": (
                        technical_bid["identifier_id"] if technical_bid else None
                    ),
                }

    @access_check(project_identifier='project')
    @access_check(technical_bid_document_identifier='document')
    def attach_technical_bid_to_relation(
        self,
        *,
        project_identifier: str,
        business_bid_document_identifier: str,
        technical_bid_document_identifier: str,
        tender_document_identifier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """向已有的商务标绑定关系附加技术标（用于分阶段上传）。"""
        normalized_project_identifier = self._normalize_required_identifier(
            project_identifier,
            "project_identifier",
        )
        normalized_business_identifier = self._normalize_required_identifier(
            business_bid_document_identifier,
            "business_bid_document_identifier",
        )
        normalized_technical_identifier = self._normalize_required_identifier(
            technical_bid_document_identifier,
            "technical_bid_document_identifier",
        )
        normalized_tender_identifier = (tender_document_identifier or "").strip() or None

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                project = self._get_project_record(cursor, normalized_project_identifier)
                if not project:
                    raise ValueError(f"项目不存在：{normalized_project_identifier}")

                cursor.execute("SELECT identifier_id FROM xtjs_projects WHERE identifier_id=%s FOR UPDATE", (project["identifier_id"],))

                business_bid = self._get_required_document_record(
                    cursor,
                    normalized_business_identifier,
                    role_label="商务标文件",
                    allowed_types=set(BUSINESS_BID_COMPATIBLE_TYPES),
                )
                technical_bid = self._get_required_document_record(
                    cursor,
                    normalized_technical_identifier,
                    role_label="技术标文件",
                    allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                )

                values: list[Any] = [project["identifier_id"], business_bid["identifier_id"]]
                tender_filter = ""
                if normalized_tender_identifier:
                    tender_filter = "AND td.identifier_id = %s"
                    values.append(self._resolve_document_identifier(cursor, normalized_tender_identifier))

                cursor.execute(
                    f"""
                    SELECT
                        pd.id AS relation_id,
                        td.identifier_id AS tender_document_identifier,
                        bbd.identifier_id AS business_bid_document_identifier,
                        tbd.identifier_id AS technical_bid_document_identifier
                    FROM xtjs_project_documents pd
                    JOIN xtjs_documents td ON pd.tender_document_id = td.identifier_id AND td.deleted = FALSE
                    JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.identifier_id AND bbd.deleted = FALSE
                    LEFT JOIN xtjs_documents tbd
                        ON pd.technical_bid_document_id = tbd.identifier_id AND tbd.deleted = FALSE
                    WHERE pd.project_id = %s
                      AND bbd.identifier_id = %s
                      {tender_filter}
                    ORDER BY pd.id
                    """,
                    tuple(values),
                )
                rows = [dict(item) for item in cursor.fetchall()]

                if not rows:
                    raise ValueError(
                        "未找到可补充技术标的项目绑定关系，请先上传并绑定对应商务标。"
                    )
                if len(rows) > 1:
                    raise ValueError(
                        "同一商务标匹配到多条项目绑定关系，请传入 tender_document_identifier 指定招标文件。"
                    )

                relation = rows[0]
                existing_technical_identifier = (
                    str(relation.get("technical_bid_document_identifier") or "").strip() or None
                )
                if (
                    existing_technical_identifier
                    and existing_technical_identifier != technical_bid["identifier_id"]
                ):
                    raise ValueError(
                        "该商务标已绑定技术标，如需替换请使用更新关联接口。"
                    )

                resolved_tender_identifier = (
                    normalized_tender_identifier
                    or str(relation.get("tender_document_identifier") or "").strip()
                )
                return self.update_relation(
                    int(relation["relation_id"]),
                    resolved_tender_identifier,
                    business_bid["identifier_id"],
                    technical_bid["identifier_id"],
                )

    @access_check(project_identifier='project')
    def detach_technical_bid_documents_from_project(
        self,
        *,
        project_identifier: str,
        technical_bid_document_identifiers: list[str],
    ) -> int:
        """从项目绑定关系中剔除指定技术标，保留招标文件和商务标绑定。"""
        normalized_project_identifier = self._normalize_required_identifier(
            project_identifier,
            "project_identifier",
        )
        normalized_identifiers = [
            str(identifier or "").strip()
            for identifier in (technical_bid_document_identifiers or [])
            if str(identifier or "").strip()
        ]
        if not normalized_identifiers:
            return 0

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                project = self._get_project_record(cursor, normalized_project_identifier)
                if not project:
                    raise ValueError(f"项目不存在：{normalized_project_identifier}")

                detached_count = 0
                for document_identifier in dict.fromkeys(normalized_identifiers):
                    resolved_document_identifier = self._resolve_document_identifier(
                        cursor,
                        document_identifier,
                    )
                    cursor.execute(
                        """
                        UPDATE xtjs_project_documents
                        SET technical_bid_document_id = NULL
                        WHERE project_id = %s
                          AND technical_bid_document_id = %s
                        """,
                        (project["identifier_id"], resolved_document_identifier),
                    )
                    detached_count += int(cursor.rowcount or 0)
                return detached_count

    @access_check(relation_id='relation')
    def delete_relation(self, relation_id: int, *, remove_expected_group: bool = False) -> bool:
        return self.delete_relations([relation_id], remove_expected_group=remove_expected_group) > 0

    @access_check(relation_ids='relation')
    def delete_relations(self, relation_ids: list[int], *, remove_expected_group: bool = False) -> int:
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM xtjs_project_documents WHERE id=ANY(%s) ORDER BY project_id,id", (relation_ids,))
            rows = cursor.fetchall()
            pids = sorted({str(r["project_id"]) for r in rows})
            for pid in pids:
                cursor.execute("SELECT upload_manifest FROM xtjs_projects WHERE identifier_id=%s FOR UPDATE", (pid,))
                manifest = cursor.fetchone()["upload_manifest"]
                if remove_expected_group and manifest:
                    slots = {r["upload_group_slot"] for r in rows if str(r["project_id"]) == pid}
                    if None in slots:
                        raise ConsistencyConflict("无法唯一确定预期投标组，请先核查关联；未自动删除")
                    removed = [g for g in manifest["groups"] if (g.get("slot") or g["business_bid"]) in slots]
                    file_slots = {g[k] for g in removed for k in ("business_bid", "technical_bid")}
                    manifest["groups"] = [g for g in manifest["groups"] if g not in removed]
                    manifest["files"] = [f for f in manifest["files"] if f["slot"] not in file_slots]
                    cursor.execute("UPDATE xtjs_projects SET upload_manifest=%s WHERE identifier_id=%s", (Json(manifest),pid))
            cursor.execute("DELETE FROM xtjs_project_documents WHERE id=ANY(%s)", (relation_ids,))
            return cursor.rowcount

    @access_check(identifier_id='project')
    def get_project_detail(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """获取项目基本信息及其所有文档绑定关系。"""
        project = self.get_project_by_identifier(identifier_id)
        if not project:
            return None

        query = """
            SELECT
                pd.id AS relation_id,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                bbd.identifier_id AS business_bid_identifier_id,
                bbd.document_type AS business_bid_document_type,
                bbd.file_name AS business_bid_file_name,
                bbd.file_url AS business_bid_file_url,
                bbd.bidder_identity AS business_bid_identity,
                tbd.bidder_identity AS technical_bid_identity,
                tbd.identifier_id AS technical_bid_identifier_id,
                tbd.document_type AS technical_bid_document_type,
                tbd.file_name AS technical_bid_file_name,
                tbd.file_url AS technical_bid_file_url,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_documents td ON pd.tender_document_id = td.identifier_id AND td.deleted = FALSE
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.identifier_id AND bbd.deleted = FALSE
            LEFT JOIN xtjs_documents tbd
                ON pd.technical_bid_document_id = tbd.identifier_id AND tbd.deleted = FALSE
            WHERE pd.project_id = %s
            ORDER BY pd.create_time DESC
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["identifier_id"],))
                relations: List[Dict[str, Any]] = [self._relation_identity(item) for item in cursor.fetchall()]
        return {"project": project, "relations": relations}

    @staticmethod
    def _real_file_url(file_url: Any, oss_service: MinioService) -> str:
        """将内部 minio:// 地址转换成可直接访问的真实 URL。"""
        normalized = str(file_url or "").strip()
        if not normalized:
            return ""
        if normalized.startswith("minio://"):
            bucket_name, object_name = MinioService.bucket_and_object_from_file_url(normalized)
            normalized = oss_service.get_presigned_url(object_name, bucket_name)
        return normalized if normalized.startswith(("http://", "https://")) else ""

    @staticmethod
    def _file_name_lookup_keys(file_name: Any) -> list[str]:
        normalized = str(file_name or "").strip()
        if not normalized:
            return []
        keys = [normalized]
        basename = re.split(r"[\\/]", normalized)[-1]
        if basename and basename not in keys:
            keys.append(basename)
        return keys

    @staticmethod
    def _first_page_number(*values: Any) -> Optional[int]:
        def first(value: Any) -> Optional[int]:
            if value is None or isinstance(value, bool):
                return None
            if isinstance(value, int):
                return value if value > 0 else None
            if isinstance(value, float):
                return int(value) if value.is_integer() and value > 0 else None
            if isinstance(value, str):
                stripped = value.strip()
                if stripped.isdigit() and int(stripped) > 0:
                    return int(stripped)
                return None
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    page = first(item)
                    if page:
                        return page
                return None
            if isinstance(value, dict):
                for key in (
                    "source_page",
                    "page",
                    "pages",
                    "page_refs",
                    "section_pages",
                    "response_page",
                    "requirement_page",
                    "start_page",
                ):
                    page = first(value.get(key))
                    if page:
                        return page
            return None

        for raw_value in values:
            page = first(raw_value)
            if page:
                return page
        return None

    @classmethod
    def _register_document_source_ref(
        cls,
        index: dict[str, dict[str, dict[str, Any]]],
        ref: dict[str, Any],
    ) -> None:
        identifier = str(ref.get("identifier_id") or "").strip()
        if identifier:
            index["by_identifier"][identifier] = ref

        for key in cls._file_name_lookup_keys(ref.get("file_name")):
            index["by_file_name"].setdefault(key, ref)

        for key in (ref.get("raw_file_url"), ref.get("file_url")):
            normalized = str(key or "").strip()
            if normalized:
                index["by_file_url"][normalized] = ref

    @classmethod
    def _build_project_document_source_index(
        cls,
        project_detail: Optional[Dict[str, Any]],
    ) -> dict[str, dict[str, dict[str, Any]]]:
        index: dict[str, dict[str, dict[str, Any]]] = {
            "by_identifier": {},
            "by_file_name": {},
            "by_file_url": {},
        }
        oss_service = MinioService()
        field_groups = (
            (
                "tender",
                "tender_identifier_id",
                "tender_document_type",
                "tender_file_name",
                "tender_file_url",
            ),
            (
                "business_bid",
                "business_bid_identifier_id",
                "business_bid_document_type",
                "business_bid_file_name",
                "business_bid_file_url",
            ),
            (
                "technical_bid",
                "technical_bid_identifier_id",
                "technical_bid_document_type",
                "technical_bid_file_name",
                "technical_bid_file_url",
            ),
        )
        seen_identifiers: set[str] = set()
        for relation in (project_detail or {}).get("relations") or []:
            for role, identifier_field, document_type_field, file_name_field, file_url_field in field_groups:
                identifier = str(relation.get(identifier_field) or "").strip()
                raw_file_url = str(relation.get(file_url_field) or "").strip()
                if not identifier or not raw_file_url or identifier in seen_identifiers:
                    continue
                seen_identifiers.add(identifier)
                cls._register_document_source_ref(
                    index,
                    {
                        "identifier_id": identifier,
                        "relation_id": relation.get("relation_id"),
                        "role": role,
                        "document_type": relation.get(document_type_field),
                        "file_name": relation.get(file_name_field),
                        "raw_file_url": raw_file_url,
                        "file_url": cls._real_file_url(raw_file_url, oss_service),
                    },
                )
        return index

    @classmethod
    def _resolve_document_source_ref(
        cls,
        node: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
    ) -> Optional[dict[str, Any]]:
        for field_name in (
            "document_identifier_id",
            "document_id",
            "identifier_id",
        ):
            identifier = str(node.get(field_name) or "").strip()
            if identifier and identifier in index["by_identifier"]:
                return index["by_identifier"][identifier]

        for field_name in ("file_url", "file_path", "source_url"):
            file_url = str(node.get(field_name) or "").strip()
            if file_url and file_url in index["by_file_url"]:
                return index["by_file_url"][file_url]

        for field_name in ("file_name", "document_file_name"):
            for key in cls._file_name_lookup_keys(node.get(field_name)):
                if key in index["by_file_name"]:
                    return index["by_file_name"][key]
        return None

    @classmethod
    def _resolve_file_name_source_ref(
        cls,
        file_name: Any,
        index: dict[str, dict[str, dict[str, Any]]],
    ) -> Optional[dict[str, Any]]:
        for key in cls._file_name_lookup_keys(file_name):
            if key in index["by_file_name"]:
                return index["by_file_name"][key]
        return None

    @classmethod
    def _resolve_prefixed_document_source_refs(
        cls,
        node: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
        context: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        refs: dict[str, dict[str, Any]] = {}
        for prefix in (
            "left",
            "right",
            "tender",
            "business",
            "technical",
            "business_bid",
            "technical_bid",
        ):
            candidates = (
                f"{prefix}_document_identifier",
                f"{prefix}_document_identifier_id",
                f"{prefix}_document_id",
                f"{prefix}_identifier_id",
            )
            for field_name in candidates:
                identifier = str(node.get(field_name) or "").strip()
                if identifier and identifier in index["by_identifier"]:
                    refs[prefix] = index["by_identifier"][identifier]
                    break
            if prefix in refs:
                continue

            for key in cls._file_name_lookup_keys(node.get(f"{prefix}_file_name")):
                if key in index["by_file_name"]:
                    refs[prefix] = index["by_file_name"][key]
                    break
            if prefix in refs:
                continue

            file_url = str(node.get(f"{prefix}_file_url") or "").strip()
            if file_url and file_url in index["by_file_url"]:
                refs[prefix] = index["by_file_url"][file_url]
            elif prefix in context:
                refs[prefix] = context[prefix]
        return refs

    @classmethod
    def _context_from_documents_node(
        cls,
        node: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        documents = node.get("documents")
        if not isinstance(documents, dict):
            return {}

        context: dict[str, dict[str, Any]] = {}
        for role, document in documents.items():
            if not isinstance(document, dict):
                continue
            ref = cls._resolve_document_source_ref(document, index)
            if not ref:
                continue
            role_key = str(role or "").strip()
            if role_key:
                context[role_key] = ref
        return context

    @classmethod
    def _node_page_number(cls, node: dict[str, Any], prefix: Optional[str] = None) -> Optional[int]:
        if prefix:
            return cls._first_page_number(
                node.get(f"{prefix}_source_page"),
                node.get(f"{prefix}_page"),
                node.get(f"{prefix}_pages"),
                node.get(f"{prefix}_page_refs"),
            )
        return cls._first_page_number(
            node.get("source_page"),
            node.get("page"),
            node.get("pages"),
            node.get("page_refs"),
            node.get("section_pages"),
            node.get("response_page"),
            node.get("requirement_page"),
            node.get("evidence"),
        )

    @classmethod
    def _file_urls_by_file_for_node(
        cls,
        node: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
    ) -> dict[str, str]:
        file_names: list[str] = []

        def append_file_name(value: Any) -> None:
            file_name = str(value or "").strip()
            if file_name and file_name not in file_names:
                file_names.append(file_name)

        for file_name in node.get("files") or []:
            append_file_name(file_name)

        doc_ranges_by_file = node.get("doc_ranges_by_file")
        if isinstance(doc_ranges_by_file, dict):
            for file_name in doc_ranges_by_file.keys():
                append_file_name(file_name)

        docs_by_file = node.get("docs")
        if isinstance(docs_by_file, dict):
            for file_name in docs_by_file.keys():
                append_file_name(file_name)

        file_urls: dict[str, str] = {}
        for file_name in file_names:
            ref = cls._resolve_file_name_source_ref(file_name, index)
            file_url = str((ref or {}).get("file_url") or "").strip()
            if file_url:
                file_urls[file_name] = file_url
        return file_urls

    @classmethod
    def _append_file_keyed_source_maps(
        cls,
        enriched: dict[str, Any],
        original: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
    ) -> None:
        file_urls = cls._file_urls_by_file_for_node(original, index)
        if not file_urls:
            return
        enriched["file_urls_by_file"] = file_urls

    @classmethod
    def _enrich_file_keyed_document_map(
        cls,
        value: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
        context: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        enriched: dict[str, Any] = {}
        for file_name, item in value.items():
            enriched_item = cls._enrich_result_node_with_document_sources(item, index, context)
            ref = cls._resolve_file_name_source_ref(file_name, index)
            if ref and isinstance(enriched_item, dict):
                page = cls._first_page_number(
                    item.get("source_page") if isinstance(item, dict) else None,
                    item.get("page") if isinstance(item, dict) else None,
                    item.get("pages") if isinstance(item, dict) else None,
                    item.get("page_refs") if isinstance(item, dict) else None,
                )
                cls._append_single_source_fields(enriched_item, ref, page)
            enriched[file_name] = enriched_item
        return enriched

    @classmethod
    def _append_single_source_fields(
        cls,
        node: dict[str, Any],
        ref: dict[str, Any],
        page: Optional[int],
    ) -> None:
        file_url = str(ref.get("file_url") or "").strip()
        if not file_url:
            return
        node["file_url"] = file_url
        if page:
            node.setdefault("source_page", page)

    @classmethod
    def _append_prefixed_source_fields(
        cls,
        node: dict[str, Any],
        prefix: str,
        ref: dict[str, Any],
        page: Optional[int],
    ) -> None:
        file_url = str(ref.get("file_url") or "").strip()
        if not file_url:
            return
        node[f"{prefix}_file_url"] = file_url
        if page:
            node.setdefault(f"{prefix}_source_page", page)

    @classmethod
    def _is_legacy_source_url_field(cls, key: Any) -> bool:
        text = str(key)
        return (
            text in {
                "project_file_urls",
                "source_page_url",
                "source_page_urls_by_file",
                "source_location",
                "source_locations_by_file",
                "page_url",
            }
            or text.endswith("_source_page_url")
            or text.endswith("_source_location")
        )

    @classmethod
    def _is_generated_file_url_field(cls, key: Any) -> bool:
        text = str(key)
        return text == "file_url" or text == "file_urls_by_file" or text.endswith("_file_url")

    @classmethod
    def _enrich_result_node_with_document_sources(
        cls,
        value: Any,
        index: dict[str, dict[str, dict[str, Any]]],
        context: Optional[dict[str, dict[str, Any]]] = None,
    ) -> Any:
        if isinstance(value, list):
            inherited_context = dict(context or {})
            return [
                cls._enrich_result_node_with_document_sources(item, index, inherited_context)
                for item in value
            ]
        if not isinstance(value, dict):
            return value

        inherited_context = dict(context or {})
        local_context = dict(inherited_context)
        local_context.update(cls._context_from_documents_node(value, index))

        single_ref = cls._resolve_document_source_ref(value, index)
        if single_ref:
            local_context.setdefault("default", single_ref)

        prefix_refs = cls._resolve_prefixed_document_source_refs(value, index, local_context)
        local_context.update(prefix_refs)

        enriched: dict[str, Any] = {}
        for key, item in value.items():
            if cls._is_legacy_source_url_field(key) or cls._is_generated_file_url_field(key):
                continue
            if key == "docs" and isinstance(item, dict):
                enriched[key] = cls._enrich_file_keyed_document_map(item, index, local_context)
            else:
                enriched[key] = cls._enrich_result_node_with_document_sources(
                    item,
                    index,
                    local_context,
                )

        cls._append_file_keyed_source_maps(enriched, value, index)

        if single_ref:
            cls._append_single_source_fields(enriched, single_ref, cls._node_page_number(value))

        for prefix, ref in prefix_refs.items():
            page = cls._node_page_number(value, prefix=prefix)
            has_explicit_prefix = any(
                field_name in value
                for field_name in (
                    f"{prefix}_document_identifier",
                    f"{prefix}_document_identifier_id",
                    f"{prefix}_document_id",
                    f"{prefix}_identifier_id",
                    f"{prefix}_file_name",
                    f"{prefix}_file_url",
                )
            )
            if has_explicit_prefix or page is not None:
                cls._append_prefixed_source_fields(enriched, prefix, ref, page)

        cls._attach_standard_locations(enriched, value, index, local_context, single_ref)
        return enriched

    @classmethod
    def _attach_standard_locations(
        cls,
        enriched: dict[str, Any],
        original: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
        context: dict[str, dict[str, Any]],
        single_ref: Optional[dict[str, Any]],
    ) -> None:
        defaults = cls._location_defaults_for_node(
            enriched,
            original,
            index,
            context,
            single_ref,
        )
        locations: list[dict[str, Any]] = []
        seen_location_keys: set[str] = set()
        raw_locations = enriched.get("locations")
        if isinstance(raw_locations, dict):
            raw_location_items = [raw_locations]
        elif isinstance(raw_locations, list):
            raw_location_items = raw_locations
        else:
            raw_location_items = []
        for raw_location in raw_location_items:
            if not isinstance(raw_location, dict):
                continue
            location_defaults = cls._location_defaults_for_location(
                defaults,
                raw_location,
                context,
            )
            for location in normalize_locations(raw_location, defaults=location_defaults):
                append_location(locations, location, seen=seen_location_keys)
        for raw_location in collect_locations(original.get("evidence")):
            location_defaults = cls._location_defaults_for_location(
                defaults,
                raw_location,
                context,
            )
            for location in normalize_locations(raw_location, defaults=location_defaults):
                append_location(locations, location, seen=seen_location_keys)
        if not locations and cls._is_location_issue_node(original):
            append_location(
                locations,
                make_location(
                    document_identifier_id=defaults.get("document_identifier_id"),
                    file_name=defaults.get("file_name"),
                    page=defaults.get("page"),
                    bbox=defaults.get("bbox"),
                    text=defaults.get("text"),
                ),
                seen=seen_location_keys,
            )
        if locations:
            enriched["locations"] = locations

    @classmethod
    def _location_defaults_for_node(
        cls,
        enriched: dict[str, Any],
        original: dict[str, Any],
        index: dict[str, dict[str, dict[str, Any]]],
        context: dict[str, dict[str, Any]],
        single_ref: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        ref = (
            single_ref
            or cls._resolve_document_source_ref(enriched, index)
            or cls._default_location_context_ref(context)
        )
        document_identifier = (
            enriched.get("document_identifier_id")
            or enriched.get("document_id")
            or enriched.get("identifier_id")
            or original.get("document_identifier_id")
            or original.get("document_id")
            or original.get("identifier_id")
            or ((ref or {}).get("identifier_id"))
        )
        file_name = (
            enriched.get("file_name")
            or enriched.get("document_file_name")
            or original.get("file_name")
            or original.get("document_file_name")
            or ((ref or {}).get("file_name"))
        )
        return {
            "document_identifier_id": document_identifier,
            "file_name": file_name,
            "page": cls._node_page_number(enriched) or cls._node_page_number(original),
            "bbox": (
                enriched.get("bbox")
                or enriched.get("bbox_ocr")
                or enriched.get("box")
                or original.get("bbox")
                or original.get("bbox_ocr")
                or original.get("box")
            ),
            "text": cls._location_text_from_node(enriched) or cls._location_text_from_node(original),
        }

    @classmethod
    def _location_defaults_for_location(
        cls,
        defaults: dict[str, Any],
        location: dict[str, Any],
        context: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        ref = cls._context_ref_for_location(location, context)
        if not ref:
            return defaults
        merged = dict(defaults)
        merged["document_identifier_id"] = ref.get("identifier_id") or merged.get("document_identifier_id")
        merged["file_name"] = ref.get("file_name") or merged.get("file_name")
        return merged

    @classmethod
    def _context_ref_for_location(
        cls,
        location: dict[str, Any],
        context: dict[str, dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        raw_role = str(
            location.get("document")
            or location.get("role")
            or location.get("document_role")
            or location.get("document_type")
            or ""
        ).strip().lower()
        if not raw_role:
            return None
        if "tender" in raw_role:
            return context.get("tender")
        if "technical" in raw_role:
            return context.get("technical") or context.get("technical_bid")
        if "business" in raw_role or "bidder" in raw_role:
            return context.get("business") or context.get("business_bid")
        return None

    @classmethod
    def _default_location_context_ref(
        cls,
        context: dict[str, dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        for key in ("default", "business", "business_bid", "technical", "technical_bid", "tender"):
            ref = context.get(key)
            if ref:
                return ref
        return None

    @classmethod
    def _location_text_from_node(cls, node: dict[str, Any]) -> str:
        for key in (
            "display_text",
            "highlight_text",
            "matched_text",
            "wrong",
            "text",
            "preview",
            "message",
            "title",
            "reason",
            "description",
        ):
            value = node.get(key)
            if value not in (None, "", []):
                return str(value).strip()
        return ""

    @classmethod
    def _is_location_issue_node(cls, node: dict[str, Any]) -> bool:
        if not isinstance(node, dict):
            return False
        has_issue_signal = any(
            key in node
            for key in (
                "status",
                "severity",
                "title",
                "message",
                "matched_text",
                "suggestion",
                "reason",
                "issue_type",
                "risk_level",
                "check_name",
            )
        )
        has_location_signal = any(
            key in node
            for key in (
                "document_identifier_id",
                "document_id",
                "identifier_id",
                "file_name",
                "document_file_name",
                "source_page",
                "page",
                "pages",
                "bbox",
                "bbox_ocr",
                "box",
            )
        )
        return has_issue_signal and has_location_signal

    @classmethod
    def _strip_legacy_project_file_urls(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [cls._strip_legacy_project_file_urls(item) for item in value]
        if isinstance(value, dict):
            return {
                key: cls._strip_legacy_project_file_urls(item)
                for key, item in value.items()
                if not cls._is_legacy_source_url_field(key)
            }
        return value

    @classmethod
    def _sanitize_project_result_record(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record)
        # result 外置后 DB 内联列为 NULL，按对象键从 MinIO 取回，保持 result 透明可用。
        document_blob_store.hydrate_result_record(payload)
        if "result" in payload or payload.get("result_object_key"):
            result_payload = cls._strip_legacy_project_file_urls(payload.get("result"))
            payload["result"] = result_payload if isinstance(result_payload, dict) else {}
        payload[MANUAL_REVIEW_RESULTS_KEY] = manual_review_results_from_record(payload)
        return payload

    def _prepare_project_result_for_persistence(
        self,
        project_identifier_id: str,
        result: Dict[str, Any],
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        project_detail = self.get_project_detail(project_identifier_id)
        if project_detail and project_detail["project"].get("upload_complete") is False:
            raise ValueError("项目材料上传或关联不完整，请补齐后再执行检查")
        source_index = self._build_project_document_source_index(project_detail)
        enriched = self._enrich_result_node_with_document_sources(
            dict(result or {}),
            source_index,
        )
        manual_review_results = manual_review_results_from_record(existing)
        embedded_manual_results = dict(enriched.get(MANUAL_REVIEW_RESULTS_KEY) or {})
        if embedded_manual_results:
            manual_review_results = embedded_manual_results
        if manual_review_results.get("latest") or manual_review_results.get("workflow_scope"):
            enriched[MANUAL_REVIEW_RESULTS_KEY] = manual_review_results
        return enriched

    @classmethod
    def _duplicate_check_payload_has_text(cls, value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, list):
            return any(cls._duplicate_check_payload_has_text(item) for item in value)
        if isinstance(value, dict):
            for key in (
                "text",
                "raw_text",
                "content",
                "full_text",
                "markdown",
                "block_content",
                "html",
                "rows",
                "records",
                "headers",
            ):
                if cls._duplicate_check_payload_has_text(value.get(key)):
                    return True
        return False

    @classmethod
    def _duplicate_check_payload_has_usable_content(cls, value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if not isinstance(value, dict):
            return False

        container = value.get("data") if isinstance(value.get("data"), dict) else value
        if cls._duplicate_check_payload_has_text(container):
            return True

        for key in ("layout_sections", "logical_tables", "table_sections", "pages"):
            items = container.get(key)
            if isinstance(items, list) and any(
                cls._duplicate_check_payload_has_text(item) for item in items
            ):
                return True
        return False

    @classmethod
    def _choose_duplicate_check_content(cls, preferred: Any, fallback: Any) -> Any:
        if cls._duplicate_check_payload_has_usable_content(preferred):
            return preferred
        return fallback

    @staticmethod
    def _hydrate_duplicate_check_row(document: Dict[str, Any]) -> None:
        """就地把查重行的原文 content 与人工复核 review_content（投标、招标）从 MinIO 取回。

        content/review_content 外置后 DB 内联列为 NULL；仅在内联为空且有对象键时回填。
        每个对象键只读一次。
        """
        for field, key_col in (
            ("raw_content", "content_object_key"),
            ("tender_raw_content", "tender_content_object_key"),
            ("review_content", "review_content_object_key"),
            ("tender_review_content", "tender_review_content_object_key"),
        ):
            if document_blob_store._is_present_json(document.get(field)):
                continue
            blob = document_blob_store.read_blob(document.get(key_col))
            if blob is not None:
                document[field] = blob

    @staticmethod
    def _effective_from_review(review_content: Any) -> Any:
        """从 review_content 取 effective_content（等价原 SQL `review_content -> 'effective_content'`）。"""
        if isinstance(review_content, dict):
            effective = review_content.get("effective_content")
            if isinstance(effective, dict):
                return effective
        return None

    @access_check(identifier_id='project')
    def get_project_documents_for_duplicate_check(
        self,
        identifier_id: str,
    ) -> Optional[Dict[str, Any]]:
        """获取项目下所有文档记录（含内容），用于查重/审查服务。"""
        project = self.get_project_by_identifier(identifier_id)
        if not project:
            return None

        self.observe_input_revision(project["identifier_id"], project.get("input_revision", 0))

        query = """
            SELECT
                pd.id AS relation_id,
                'business_bid' AS relation_role,
                bbd.identifier_id AS document_id,
                bbd.identifier_id,
                bbd.document_type,
                bbd.file_name,
                bbd.file_url,
                bbd.extracted,
                bbd.content AS raw_content,
                bbd.content_object_key AS content_object_key,
                bbd.review_content,
                bbd.review_content_object_key AS review_content_object_key,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                td.extracted AS tender_extracted,
                td.content AS tender_raw_content,
                td.content_object_key AS tender_content_object_key,
                td.review_content AS tender_review_content,
                td.review_content_object_key AS tender_review_content_object_key,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_documents td
              ON pd.tender_document_id = td.identifier_id
             AND td.deleted = FALSE
            JOIN xtjs_documents bbd
              ON pd.business_bid_document_id = bbd.identifier_id
             AND bbd.deleted = FALSE
            WHERE pd.project_id = %s

            UNION ALL

            SELECT
                pd.id AS relation_id,
                'technical_bid' AS relation_role,
                tbd.identifier_id AS document_id,
                tbd.identifier_id,
                tbd.document_type,
                tbd.file_name,
                tbd.file_url,
                tbd.extracted,
                tbd.content AS raw_content,
                tbd.content_object_key AS content_object_key,
                tbd.review_content,
                tbd.review_content_object_key AS review_content_object_key,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                td.extracted AS tender_extracted,
                td.content AS tender_raw_content,
                td.content_object_key AS tender_content_object_key,
                td.review_content AS tender_review_content,
                td.review_content_object_key AS tender_review_content_object_key,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_documents td
              ON pd.tender_document_id = td.identifier_id
             AND td.deleted = FALSE
            JOIN xtjs_documents tbd
              ON pd.technical_bid_document_id = tbd.identifier_id
             AND tbd.deleted = FALSE
            WHERE pd.project_id = %s

            ORDER BY create_time DESC, relation_id DESC, document_id DESC
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["identifier_id"], project["identifier_id"]))
                documents: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]

        for document in documents:
            # content/review_content 外置后内联列为 NULL，按对象键从 MinIO 取回；
            # 再在 Python 侧用 effective_content（人工修订）优先、否则原文，等价原 SQL COALESCE。
            self._hydrate_duplicate_check_row(document)
            document["content"] = self._choose_duplicate_check_content(
                self._effective_from_review(document.get("review_content")),
                document.get("raw_content"),
            )
            document["tender_content"] = self._choose_duplicate_check_content(
                self._effective_from_review(document.get("tender_review_content")),
                document.get("tender_raw_content"),
            )

        result_record = self.get_project_result(project["identifier_id"])
        return {
            "project": project,
            "documents": documents,
            "workflow_scope": workflow_scope_from_result_record(result_record),
            MANUAL_REVIEW_RESULTS_KEY: manual_review_results_from_record(result_record),
        }

    def observe_input_revision(self, pid, revision):
        if not hasattr(self, "_input_revisions"):
            self._input_revisions = {}
        self._input_revisions.setdefault(str(pid), int(revision))

    @access_check(identifier_id='project')
    def expect_input_revision(self, identifier_id, revision):
        with self._get_connection() as conn, conn.cursor() as cursor:
            pid = self._resolve_project_identifier(cursor, identifier_id)
            cursor.execute("SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s AND deleted=FALSE", (pid,))
            row = cursor.fetchone()
            if row is None or int(row[0]) != revision:
                raise ConsistencyConflict()
        self.observe_input_revision(pid, revision)

    def assert_input_revision(self, pid, revision):
        expected = getattr(self, "_input_revisions", {}).get(str(pid))
        if expected is not None and expected != int(revision):
            raise ConsistencyConflict()

    # 分析结果管理
    @access_check(project_identifier_id='project')
    def get_project_result(self, project_identifier_id: str):
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            pid = self._resolve_project_identifier(cursor, project_identifier_id)
            cursor.execute("SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s AND deleted=FALSE", (pid,))
            project = cursor.fetchone()
            if not project:
                return None
            revision = project["input_revision"]
            self.assert_input_revision(pid, revision)
            self.observe_input_revision(pid, revision)
            cursor.execute("SELECT * FROM xtjs_result WHERE project_identifier_id=%s", (pid,))
            row = cursor.fetchone()
            if not row:
                return None
            row = dict(row)
            row["results_stale"] = row["input_revision"] != revision
            row["current_input_revision"] = revision
            if row["results_stale"]:
                row["historical_result_object_key"] = row.pop("result_object_key", None)
                row["result"] = {}
                row["result_keys"] = []
                row["result_summary"] = None
            return self._sanitize_project_result_record(row)

    def _get_project_review_head(self, cursor, project_identifier_id: str) -> dict[str, Any] | None:
        pid = self._resolve_project_identifier(cursor, project_identifier_id)
        cursor.execute(
            """SELECT p.identifier_id,p.project_name,p.input_revision,p.parsing_status,
                      r.result_version,r.review_summary,r.review_index_status,r.result_summary,
                      r.result_keys,r.result_object_key,r.input_revision AS result_input_revision,
                      r.update_time AS result_update_time
               FROM xtjs_projects p
               LEFT JOIN xtjs_result r ON r.project_identifier_id=p.identifier_id
               WHERE p.identifier_id=%s AND NOT p.deleted""",
            (pid,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    @staticmethod
    def _assert_review_version(head: dict[str, Any], result_version: str | None) -> None:
        expected = str(head.get("result_version") or "").strip()
        requested = str(result_version or "").strip()
        if requested and requested != expected:
            raise ConsistencyConflict("审查结果已更新，请重新加载摘要")
        if head.get("result_input_revision") is not None and int(head.get("result_input_revision") or 0) != int(head.get("input_revision") or 0):
            raise ConsistencyConflict("材料已变更，旧结果已过期")

    def _projected_duplicate_review_rows(
        self, project_identifier_id: str, result_version: str, result_key: str,
    ) -> list[dict[str, Any]] | None:
        """Build historical duplicate cards from immutable index blobs; never persist them."""
        if result_key not in DUPLICATE_REVIEW_KEYS:
            return None
        cache_key = (str(project_identifier_id), str(result_version), result_key)
        with _duplicate_projection_cache_lock:
            if cache_key in _duplicate_projection_cache:
                _duplicate_projection_cache.move_to_end(cache_key)
                return _duplicate_projection_cache[cache_key]
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """SELECT issue_id,detail_object_key,evidence_object_key
                   FROM xtjs_review_issues
                   WHERE project_identifier_id=%s AND result_version=%s AND result_key=%s
                   ORDER BY issue_order,issue_id""",
                (project_identifier_id, result_version, result_key),
            )
            rows = [dict(row) for row in cursor.fetchall()]
        if not rows or all(str(row["issue_id"]).startswith("dupgroup-") for row in rows):
            return _remember_duplicate_projection(cache_key, None)
        source_keys: dict[str, str] = {}
        issues: list[dict[str, Any]] = []
        for row in rows:
            detail = document_blob_store.read_blob(row["detail_object_key"])
            evidence = document_blob_store.read_blob(row["evidence_object_key"])
            if not isinstance(detail, dict) or not isinstance(evidence, dict):
                raise ValueError("review duplicate evidence is invalid")
            issue = {**detail, "occurrences": list(evidence.get("occurrences") or [])}
            issue["source_review_issue_ids"] = list(dict.fromkeys(
                [*issue.get("source_review_issue_ids", []), str(row["issue_id"])]))
            issues.append(issue)
            source_keys.update(evidence.get("source_item_object_keys") or {})
        if not any(
            any(
                (occurrence.get("evidence") or {}).get(key)
                for key in ("left_analysis_text", "right_analysis_text", "left_text", "right_text",
                            "left_preview", "right_preview", "left_rows", "right_rows",
                            "sample_rows", "text", "preview", "hash", "phash")
            ) or any((doc or {}).get("preview") for doc in (occurrence.get("docs") or {}).values())
            for issue in issues for occurrence in issue.get("occurrences") or []
            if isinstance(occurrence, dict)
        ):
            return _remember_duplicate_projection(cache_key, None)
        sources = {}
        for identifier, object_key in source_keys.items():
            value = document_blob_store.read_blob(object_key)
            if not isinstance(value, dict):
                raise ValueError(f"review source item is invalid: {identifier}")
            sources[str(identifier)] = value
        projected = project_duplicate_payload({
            "document_type": result_key,
            "source_items": sources,
            "issues": issues,
        })
        if len(projected["issues"]) == len(issues) and all(
            int(issue.get("source_evidence_count") or 0) <= 1
            for issue in projected["issues"]
        ):
            return _remember_duplicate_projection(cache_key, None)
        result = []
        for order, issue in enumerate(projected["issues"]):
            identifiers = list(dict.fromkeys(
                [*issue.get("source_issue_ids", []),
                 *[identifier for occurrence in issue.get("occurrences") or []
                   for identifier in occurrence.get("source_item_ids") or []]]))
            detail = {key: value for key, value in issue.items() if key != "occurrences"}
            evidence = {
                "issue_id": issue["cluster_id"],
                "occurrences": issue.get("occurrences") or [],
                "source_item_object_keys": {key: source_keys[key] for key in identifiers if key in source_keys},
            }
            status = str(issue.get("status") or "unclear")
            result.append({
                "result_key": result_key, "issue_id": issue["cluster_id"],
                "issue_order": order, "risk_level": issue.get("risk_level") or "none",
                "status": status, "check_code": "duplicate_check",
                "title": issue.get("title") or "疑似重复内容",
                "description": f"共 {issue.get('occurrence_count', 0)} 条重复证据",
                "file_names": issue.get("files") or [],
                "list_payload": {key: value for key, value in detail.items()
                                 if key in {"cluster_id", "title", "family", "mode", "risk_level",
                                            "score_display", "score_value", "similarity", "files", "file_count",
                                            "metrics", "doc_ranges_by_file", "occurrence_count",
                                            "source_evidence_count", "participants", "participant_documents",
                                            "source_review_issue_ids",
                                            "pair_scores", "status", "review_only", "review_projection_version"}},
                "evidence_count": len(evidence["occurrences"]),
                "_projection_detail": detail, "_projection_evidence": evidence,
            })
        return _remember_duplicate_projection(cache_key, result)

    @access_check(project_identifier_id='project')
    def get_project_review_summary(self, project_identifier_id: str) -> dict[str, Any]:
        """Read the first-screen summary without hydrating the full result object."""
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
        result_exists = bool(head.get("result_object_key") or head.get("result_keys"))
        stale = result_exists and head.get("result_input_revision") != head.get("input_revision")
        indexed = (
            settings.XTJS_REVIEW_INDEX_ENABLED
            and head.get("review_index_status") == "ready"
            and isinstance(head.get("review_summary"), dict)
            and bool(head.get("result_version"))
            and not stale
        )
        if indexed:
            summary = _canonical_review_summary(head["review_summary"])
            with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """WITH excluded_titles AS (
                           SELECT DISTINCT title
                           FROM xtjs_review_issues
                           WHERE project_identifier_id=%s AND result_version=%s
                             AND title IS NOT NULL
                             AND (
                               title=%s
                               OR COALESCE(list_payload #>> '{evidence,is_optional}', '')='true'
                               OR COALESCE(list_payload #>> '{evidence,requirements,is_optional}', '')='true'
                               OR COALESCE(list_payload #>> '{evidence,applicability_status}', '') IN ('optional','not_applicable')
                               OR COALESCE(list_payload #>> '{evidence,skip_reason,type}', '')='optional_attachment_not_provided'
                               OR description LIKE '%%列为可选%%'
                               OR title ~ '(本项目|不项目|本项日)[[:space:]]*(为)?[[:space:]]*不适用'
                             )
                       )
                       SELECT result_key,risk_level,status,COUNT(*) AS count
                       FROM xtjs_review_issues
                       WHERE project_identifier_id=%s AND result_version=%s
                         AND title IN (SELECT title FROM excluded_titles)
                       GROUP BY result_key,risk_level,status""",
                    (
                        head["identifier_id"],
                        head.get("result_version"),
                        REMOVED_BUSINESS_SCOPE_ISSUE_TITLE,
                        head["identifier_id"],
                        head.get("result_version"),
                    ),
                )
                removed_rows = [dict(row) for row in cursor.fetchall()]
            summary = _subtract_removed_review_issue_counts(summary, removed_rows)
            categories = []
            for category in summary.get("categories") or []:
                category = dict(category)
                projected = self._projected_duplicate_review_rows(
                    str(head["identifier_id"]), str(head["result_version"]),
                    str(category.get("result_key") or ""),
                )
                if projected is not None:
                    risks = {key: 0 for key in ("high", "medium", "low", "none")}
                    statuses = {key: 0 for key in ("pass", "fail", "unclear", "not_applicable")}
                    for row in projected:
                        risks[str(row.get("risk_level") or "none")] += 1
                        canonical = _canonical_review_issue_status(row.get("status"))
                        if canonical in statuses:
                            statuses[canonical] += 1
                    category.update({
                        "issue_count": len(projected), "risk_counts": risks,
                        "status_counts": statuses, "review_item_count": sum(statuses.values()),
                        "inconsistent_count": statuses["fail"], "unclear_count": statuses["unclear"],
                        "not_applicable_count": statuses["not_applicable"],
                        "has_risk": any(risks[key] for key in ("high", "medium", "low")),
                    })
                categories.append(category)
            summary["categories"] = categories
            summary["issue_count"] = sum(int(item.get("issue_count") or 0) for item in categories)
            summary["risk_counts"] = {
                key: sum(int((item.get("risk_counts") or {}).get(key) or 0) for item in categories)
                for key in ("high", "medium", "low", "none")
            }
            summary["status_counts"] = {
                key: sum(int((item.get("status_counts") or {}).get(key) or 0) for item in categories)
                for key in ("pass", "fail", "unclear", "not_applicable")
            }
            summary["review_item_count"] = sum(summary["status_counts"].values())
            summary["inconsistent_count"] = summary["status_counts"]["fail"]
            summary["unclear_count"] = summary["status_counts"]["unclear"]
            summary["not_applicable_count"] = summary["status_counts"]["not_applicable"]
        else:
            basic = head.get("result_summary") or {}
            summary = {
                "schema_version": 0,
                "result_version": head.get("result_version"),
                "status": "stale" if stale else ("legacy" if result_exists else "unavailable"),
                "issue_count": None,
                "risk_counts": {},
                "categories": [
                    {
                        "result_key": key,
                        "status": "legacy",
                        "issue_count": None,
                        "risk_counts": {},
                        "has_risk": bool(basic.get("has_suspicious")),
                    }
                    for key in (basic.get("result_keys") or head.get("result_keys") or [])
                    if is_result_key_visible(str(key))
                ],
            }
        return {
            "project": {
                "identifier_id": head["identifier_id"],
                "project_name": head.get("project_name"),
                "parsing_status": head.get("parsing_status"),
                "input_revision": head.get("input_revision"),
            },
            "result_version": head.get("result_version"),
            "result_update_time": head.get("result_update_time"),
            "results_stale": bool(stale),
            "compatibility_mode": not indexed,
            **summary,
        }

    @access_check(project_identifier_id='project')
    def assert_project_review_version(
        self,
        project_identifier_id: str,
        result_version: str | None,
    ) -> dict[str, Any]:
        """Validate an immutable result version before consulting a response cache."""
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
            self._assert_review_version(head, result_version)
            return {
                "project_identifier_id": str(head["identifier_id"]),
                "input_revision": int(head.get("input_revision") or 0),
                "result_version": str(head.get("result_version") or ""),
            }

    @access_check(project_identifier_id='project')
    def get_project_review_component(
        self,
        project_identifier_id: str,
        result_key: str,
        result_version: str | None,
    ) -> dict[str, Any]:
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
            self._assert_review_version(head, result_version)
            cursor.execute(
                """SELECT object_key FROM xtjs_result_components
                   WHERE project_identifier_id=%s AND result_version=%s AND result_key=%s""",
                (head["identifier_id"], head.get("result_version"), result_key),
            )
            row = cursor.fetchone()
        if not row:
            raise KeyError(result_key)
        payload = document_blob_store.read_blob(row["object_key"])
        if not isinstance(payload, dict):
            raise ValueError("review component is invalid")
        projected = self._projected_duplicate_review_rows(
            str(head["identifier_id"]), str(head["result_version"]), result_key,
        )
        if projected is not None:
            payload = {**payload, "issue_count": len(projected), "review_projection_version": 1}
            if isinstance(payload.get("summary"), dict):
                payload["summary"] = {**payload["summary"], "cluster_count": len(projected)}
        return {
            "result_version": head.get("result_version"),
            "result_key": result_key,
            "result": payload,
        }

    @access_check(project_identifier_id='project')
    def list_project_review_issues(
        self,
        project_identifier_id: str,
        *,
        result_version: str | None,
        result_key: str | None = None,
        risk_level: str | None = None,
        status: str | None = None,
        check_code: str | None = None,
        file_name: str | None = None,
        limit: int = 20,
        offset: int = 0,
        ids_only: bool = False,
        _projection_enabled: bool = True,
        _all_rows: bool = False,
    ) -> dict[str, Any]:
        normalized_limit = max(1, min(int(limit), 100))
        normalized_offset = max(0, int(offset))
        if _projection_enabled and result_key is None:
            with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                head = self._get_project_review_head(cursor, project_identifier_id)
                if not head:
                    raise ValueError("project not found")
                self._assert_review_version(head, result_version)
            projected_by_key = {
                key: self._projected_duplicate_review_rows(
                    str(head["identifier_id"]), str(head["result_version"]), key,
                ) for key in DUPLICATE_REVIEW_KEYS
            }
            if any(value is not None for value in projected_by_key.values()):
                raw = self.list_project_review_issues(
                    project_identifier_id, result_version=result_version,
                    _projection_enabled=False, _all_rows=True,
                )
                replacement_keys = {key for key, value in projected_by_key.items() if value is not None}
                combined = [row for row in raw["items"] if row.get("result_key") not in replacement_keys]
                combined.extend(row for value in projected_by_key.values() for row in value or [])
                combined.sort(key=lambda row: (str(row.get("result_key") or ""),
                                               int(row.get("issue_order") or 0), str(row.get("issue_id") or "")))
                wanted_status = _canonical_review_issue_status(status)
                filtered = []
                for row in combined:
                    if risk_level and str(row.get("risk_level")) != str(risk_level):
                        continue
                    if check_code and str(row.get("check_code")) != str(check_code):
                        continue
                    if file_name and file_name not in (row.get("file_names") or []):
                        continue
                    actual = _canonical_review_issue_status((row.get("list_payload") or {}).get("status") or row.get("status"))
                    if wanted_status and not (wanted_status == "not_pass" and actual != "pass") and actual != wanted_status:
                        continue
                    filtered.append(row)
                selected = filtered if ids_only else filtered[normalized_offset:normalized_offset + normalized_limit]
                return {"result_version": head.get("result_version"), "total": len(filtered),
                        "limit": len(filtered) if ids_only else normalized_limit,
                        "offset": 0 if ids_only else normalized_offset,
                        "items": [{"issue_id": row["issue_id"]} if ids_only else
                                  _canonical_review_issue_row({key: value for key, value in row.items()
                                                               if not key.startswith("_projection_")})
                                  for row in selected]}
        if _projection_enabled and result_key in DUPLICATE_REVIEW_KEYS:
            with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                head = self._get_project_review_head(cursor, project_identifier_id)
                if not head:
                    raise ValueError("project not found")
                self._assert_review_version(head, result_version)
            projected = self._projected_duplicate_review_rows(
                str(head["identifier_id"]), str(head["result_version"]), result_key,
            )
            if projected is not None:
                filtered = []
                wanted_status = _canonical_review_issue_status(status)
                for row in projected:
                    if risk_level and str(row.get("risk_level")) != str(risk_level):
                        continue
                    if check_code and str(row.get("check_code")) != str(check_code):
                        continue
                    if file_name and file_name not in (row.get("file_names") or []):
                        continue
                    actual = _canonical_review_issue_status(row.get("status"))
                    if wanted_status and not (wanted_status == "not_pass" and actual != "pass") and actual != wanted_status:
                        continue
                    filtered.append(row)
                selected = filtered if ids_only else filtered[normalized_offset:normalized_offset + normalized_limit]
                return {
                    "result_version": head.get("result_version"), "total": len(filtered),
                    "limit": len(filtered) if ids_only else normalized_limit,
                    "offset": 0 if ids_only else normalized_offset,
                    "items": [{"issue_id": row["issue_id"]} if ids_only else
                              _canonical_review_issue_row({key: value for key, value in row.items()
                                                           if not key.startswith("_projection_")})
                              for row in selected],
                }
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
            self._assert_review_version(head, result_version)
            conditions = ["project_identifier_id=%s", "result_version=%s"]
            values: list[Any] = [head["identifier_id"], head.get("result_version")]
            conditions.append(
                """(title IS NULL OR title NOT IN (
                       SELECT DISTINCT optional_issue.title
                       FROM xtjs_review_issues AS optional_issue
                       WHERE optional_issue.project_identifier_id=%s
                         AND optional_issue.result_version=%s
                         AND optional_issue.title IS NOT NULL
                         AND (
                           optional_issue.title=%s
                           OR COALESCE(optional_issue.list_payload #>> '{evidence,is_optional}', '')='true'
                           OR COALESCE(optional_issue.list_payload #>> '{evidence,requirements,is_optional}', '')='true'
                           OR COALESCE(optional_issue.list_payload #>> '{evidence,applicability_status}', '') IN ('optional','not_applicable')
                           OR COALESCE(optional_issue.list_payload #>> '{evidence,skip_reason,type}', '')='optional_attachment_not_provided'
                           OR optional_issue.description LIKE '%%列为可选%%'
                           OR optional_issue.title ~ '(本项目|不项目|本项日)[[:space:]]*(为)?[[:space:]]*不适用'
                         )
                     ))"""
            )
            values.extend([
                head["identifier_id"],
                head.get("result_version"),
                REMOVED_BUSINESS_SCOPE_ISSUE_TITLE,
            ])
            for column, value in (
                ("result_key", result_key),
                ("risk_level", risk_level),
                ("check_code", check_code),
            ):
                normalized = str(value or "").strip()
                if normalized:
                    conditions.append(f"{column}=%s")
                    values.append(normalized)
            normalized_status = str(status or "").strip().lower()
            if normalized_status:
                # Prefer the payload status for indexes produced before duplicate
                # review-only states were preserved in the status column.
                effective_status = "COALESCE(NULLIF(list_payload->>'status', ''), status)"
                normalized_status = _canonical_review_issue_status(normalized_status)
                if normalized_status == "not_pass":
                    conditions.append(f"{effective_status} NOT IN (%s,%s)")
                    values.extend(["pass", "passed"])
                elif normalized_status == "fail":
                    conditions.append(f"LOWER({effective_status}) IN (%s,%s,%s,%s)")
                    values.extend(["fail", "failed", "missing", "error"])
                elif normalized_status == "not_applicable":
                    conditions.append(f"LOWER({effective_status}) IN (%s,%s,%s)")
                    values.extend(["not_applicable", "skipped", "optional"])
                elif normalized_status == "unclear":
                    conditions.append(f"LOWER({effective_status}) IN (%s,%s,%s,%s)")
                    values.extend(["unclear", "pending", "ambiguous", "review"])
                elif normalized_status == "pass":
                    conditions.append(f"LOWER({effective_status}) IN (%s,%s,%s,%s)")
                    values.extend(["pass", "passed", "success", "ok"])
                else:
                    conditions.append(f"{effective_status}=%s")
                    values.append(normalized_status)
            normalized_file = str(file_name or "").strip()
            if normalized_file:
                conditions.append("file_names ? %s")
                values.append(normalized_file)
            where = " AND ".join(conditions)
            cursor.execute(f"SELECT COUNT(*) AS total FROM xtjs_review_issues WHERE {where}", tuple(values))
            total = int(cursor.fetchone()["total"])
            selected = "issue_id" if ids_only else "issue_id,result_key,issue_order,risk_level,status,check_code,title,description,file_names,list_payload,evidence_count"
            paging = "" if ids_only or _all_rows else " LIMIT %s OFFSET %s"
            params = tuple(values if ids_only or _all_rows else values + [normalized_limit, normalized_offset])
            cursor.execute(
                f"SELECT {selected} FROM xtjs_review_issues WHERE {where} ORDER BY result_key,issue_order,issue_id{paging}",
                params,
            )
            items = [_canonical_review_issue_row(dict(row)) for row in cursor.fetchall()]
        return {
            "result_version": head.get("result_version"),
            "total": total,
            "limit": total if ids_only or _all_rows else normalized_limit,
            "offset": 0 if ids_only or _all_rows else normalized_offset,
            "items": items,
        }

    @access_check(project_identifier_id='project')
    def get_project_review_issue(
        self,
        project_identifier_id: str,
        issue_id: str,
        *,
        result_version: str | None,
        evidence: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
            self._assert_review_version(head, result_version)
            cursor.execute(
                """SELECT result_key,issue_id,risk_level,status,title,description,
                          detail_object_key,evidence_object_key,evidence_count
                   FROM xtjs_review_issues
                   WHERE project_identifier_id=%s AND result_version=%s AND issue_id=%s""",
                (head["identifier_id"], head.get("result_version"), issue_id),
            )
            row = cursor.fetchone()
        if not row and str(issue_id).startswith("dupgroup-"):
            for duplicate_key in DUPLICATE_REVIEW_KEYS:
                projected = self._projected_duplicate_review_rows(
                    str(head["identifier_id"]), str(head["result_version"]), duplicate_key,
                )
                match = next((item for item in projected or [] if item["issue_id"] == issue_id), None)
                if match is None:
                    continue
                payload = match["_projection_evidence"] if evidence else match["_projection_detail"]
                if evidence:
                    normalized_limit = max(1, min(int(limit), 100))
                    normalized_offset = max(0, int(offset))
                    all_occurrences = payload["occurrences"]
                    occurrences = all_occurrences[normalized_offset:normalized_offset + normalized_limit]
                    referenced_ids = {str(identifier) for occurrence in occurrences
                                      for identifier in occurrence.get("source_item_ids") or []}
                    source_items = {}
                    for identifier, object_key in payload["source_item_object_keys"].items():
                        if identifier in referenced_ids:
                            source_items[identifier] = document_blob_store.read_blob(object_key)
                    payload = {"issue_id": issue_id, "occurrences": occurrences,
                               "source_items": source_items, "total": len(all_occurrences),
                               "limit": normalized_limit, "offset": normalized_offset}
                return {"result_version": head.get("result_version"), "result_key": duplicate_key,
                        "issue_id": issue_id, "evidence_count": match["evidence_count"], "data": payload}
        if not row:
            raise KeyError(issue_id)
        object_key = row["evidence_object_key"] if evidence else row["detail_object_key"]
        payload = document_blob_store.read_blob(object_key)
        if evidence and isinstance(payload, dict) and isinstance(payload.get("occurrences"), list):
            normalized_limit = max(1, min(int(limit), 100))
            normalized_offset = max(0, int(offset))
            all_occurrences = payload.get("occurrences") or []
            occurrences = all_occurrences[normalized_offset:normalized_offset + normalized_limit]
            referenced_ids = {
                str(occurrence.get("source_item_id"))
                for occurrence in occurrences
                if isinstance(occurrence, dict) and occurrence.get("source_item_id")
            }
            referenced_ids.update(
                str(identifier)
                for occurrence in occurrences if isinstance(occurrence, dict)
                for identifier in occurrence.get("source_item_ids") or []
                if identifier
            )
            source_items: dict[str, Any] = {}
            for identifier, source_key in (payload.get("source_item_object_keys") or {}).items():
                if referenced_ids and str(identifier) not in referenced_ids:
                    continue
                source_item = document_blob_store.read_blob(source_key)
                if not isinstance(source_item, dict):
                    raise ValueError(f"review source item is invalid: {identifier}")
                source_items[str(identifier)] = source_item
            payload = {
                "issue_id": payload.get("issue_id"),
                "occurrences": occurrences,
                "source_items": source_items,
                "total": len(all_occurrences),
                "limit": normalized_limit,
                "offset": normalized_offset,
            }
        return {
            "result_version": head.get("result_version"),
            "result_key": row["result_key"],
            "issue_id": row["issue_id"],
            "evidence_count": row["evidence_count"],
            "data": payload,
        }

    @access_check(project_identifier_id='project')
    def get_project_review_export_payload(
        self,
        project_identifier_id: str,
        *,
        result_version: str,
        review_statuses: Optional[dict[str, dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """Build a complete lightweight export list without browser pagination."""
        with self._get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            head = self._get_project_review_head(cursor, project_identifier_id)
            if not head:
                raise ValueError("project not found")
            self._assert_review_version(head, result_version)
            cursor.execute(
                """SELECT issue_id,result_key,risk_level,status,title,description,file_names,list_payload,
                          detail_object_key,evidence_object_key,evidence_count
                   FROM xtjs_review_issues
                   WHERE project_identifier_id=%s AND result_version=%s
                   ORDER BY result_key,issue_order,issue_id""",
                (head["identifier_id"], head.get("result_version")),
            )
            rows = [dict(row) for row in cursor.fetchall()]
        for duplicate_key in DUPLICATE_REVIEW_KEYS:
            projected = self._projected_duplicate_review_rows(
                str(head["identifier_id"]), str(head["result_version"]), duplicate_key,
            )
            if projected is not None:
                rows = [row for row in rows if row.get("result_key") != duplicate_key] + projected
        rows.sort(key=lambda row: (str(row.get("result_key") or ""), int(row.get("issue_order") or 0), str(row.get("issue_id") or "")))
        statuses = review_statuses or {}
        items: list[dict[str, Any]] = []
        object_cache: dict[str, Any] = {}
        source_items: dict[str, dict[str, Any]] = {}

        def hide_unverified_typos(value: Any) -> Any:
            if isinstance(value, list):
                return [hide_unverified_typos(item) for item in value]
            if not isinstance(value, dict):
                return value
            clean = {}
            for key, nested in value.items():
                if key == "typo_review_candidates":
                    continue
                if key == "short_duplicate_typo_issues":
                    clean[key] = [
                        hide_unverified_typos(item)
                        for item in nested or []
                        if isinstance(item, dict)
                        and item.get("verification_status") == "confirmed"
                        and len(str(item.get("original_word") or "")) >= 2
                        and item.get("occurrences")
                    ]
                elif key == "review_candidate_count":
                    clean[key] = 0
                else:
                    clean[key] = hide_unverified_typos(nested)
            return clean

        def read_once(object_key: Any) -> Any:
            normalized_key = str(object_key or "").strip()
            if not normalized_key:
                return None
            if normalized_key not in object_cache:
                object_cache[normalized_key] = document_blob_store.read_blob(normalized_key)
            return object_cache[normalized_key]

        for row in rows:
            row = _canonical_review_issue_row(row)
            payload = dict(row.get("list_payload") or {})
            if (
                "duplicate" in str(row.get("result_key") or "")
                and payload.get("review_only")
                and str(payload.get("risk_level") or row.get("risk_level") or "none") == "none"
            ):
                continue
            detail = row.get("_projection_detail") or read_once(row.get("detail_object_key"))
            evidence = row.get("_projection_evidence") or read_once(row.get("evidence_object_key"))
            if isinstance(evidence, dict) and evidence.get("source_item_object_keys"):
                evidence = dict(evidence)
                source_object_keys = evidence.pop("source_item_object_keys", {})
                evidence["source_item_ids"] = list(source_object_keys)
                for source_id, source_key in source_object_keys.items():
                    source_item = read_once(source_key)
                    if not isinstance(source_item, dict):
                        raise ValueError(f"review source item is invalid: {source_id}")
                    source_items[str(source_id)] = source_item
            item = {
                **payload,
                "id": row["issue_id"],
                "issue_id": row["issue_id"],
                "result_key": row["result_key"],
                "source_result_key": row["result_key"],
                "title": row.get("title") or payload.get("title"),
                "summary": row.get("description") or payload.get("summary"),
                "risk_level": row.get("risk_level"),
                # Payload status repairs older duplicate indexes where a
                # review-only item was stored as passed solely because risk=none.
                "source_status": _canonical_review_issue_status(payload.get("status") or row.get("status")),
                "file_names": row.get("file_names") or payload.get("files") or [],
                "issue": detail if isinstance(detail, dict) else payload,
                "evidence": evidence if isinstance(evidence, dict) else {},
            }
            status = statuses.get(row["issue_id"])
            if isinstance(status, dict):
                item["frontend_review_status"] = status.get("status")
                item["frontend_reviewed_at"] = status.get("reviewedAt") or status.get("reviewed_at")
                if status.get("note"):
                    item["frontend_review_note"] = status["note"]
            items.append(hide_unverified_typos(item) if "duplicate" in str(row.get("result_key") or "") else item)
        return {"result": items, "source_items": hide_unverified_typos(source_items)}

    def list_project_results(
        self,
        limit: int = 20,
        offset: int = 0,
        keyword: Optional[str] = None,
    ) -> Dict[str, Any]:
        """分页查询项目结果记录列表。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        normalized_keyword = (keyword or "").strip()
        conditions = ["p.deleted = FALSE", "r.input_revision=p.input_revision", scope_sql("project", "p")]
        values: List[Any] = []
        if normalized_keyword:
            keyword_like = f"%{normalized_keyword}%"
            conditions.append("(r.project_identifier_id::text ILIKE %s OR p.project_name ILIKE %s)")
            values.extend([keyword_like, keyword_like])
        where_clause = " AND ".join(conditions)

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM xtjs_result r
            JOIN xtjs_projects p ON r.project_identifier_id = p.identifier_id
            WHERE {where_clause}
        """
        data_query = f"""
            SELECT
                r.project_identifier_id,
                p.project_name,
                r.result,
                r.result_object_key,
                r.create_time,
                r.update_time
            FROM xtjs_result r
            JOIN xtjs_projects p ON r.project_identifier_id = p.identifier_id
            WHERE {where_clause}
            ORDER BY r.update_time DESC, r.project_identifier_id DESC
            LIMIT %s OFFSET %s
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(count_query, tuple(values))
                total = int(cursor.fetchone()["total"])
                cursor.execute(data_query, tuple(values + [normalized_limit, normalized_offset]))
                items: List[Dict[str, Any]] = [
                    self._sanitize_project_result_record(dict(item))
                    for item in cursor.fetchall()
                ]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )

    # 结果外置：把完整 result 写 MinIO，DB 行只留 result_object_key + 轻量 result_keys，
    # result 列置 NULL。
    _RESULT_UPSERT_SQL = """
        INSERT INTO xtjs_result (
            project_identifier_id, result, result_object_key, result_keys,
            result_summary, workflow_scope, input_revision, result_version,
            review_summary, review_index_status
        )
        VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (project_identifier_id)
        DO UPDATE
        SET
            result = NULL,
            result_object_key = EXCLUDED.result_object_key,
            result_keys = EXCLUDED.result_keys,
            result_summary = EXCLUDED.result_summary,
            workflow_scope = EXCLUDED.workflow_scope,
            input_revision = EXCLUDED.input_revision,
            result_version = EXCLUDED.result_version,
            review_summary = EXCLUDED.review_summary,
            review_index_status = EXCLUDED.review_index_status,
            update_time = CURRENT_TIMESTAMP
        RETURNING
            id,
            project_identifier_id,
            result,
            result_object_key,
            result_version,
            review_summary,
            review_index_status,
            create_time,
            update_time
    """

    def _persist_project_result(
        self,
        cursor,
        project: Dict[str, Any],
        full_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """把完整 result 写 MinIO 并落库对象键（result 列置 NULL），返回规范化记录。

        MinIO 写在 SQL 之前；若 MinIO 失败抛错→事务回滚，不会留下指向坏对象的行。
        同时落一份轻量顶层键列表（result_keys），供项目列表统计分析项。
        """
        pid = str(project["identifier_id"])
        encoded = jsonable_encoder(full_result)
        review_summary = None
        component_rows: list[dict[str, Any]] = []
        issue_rows: list[dict[str, Any]] = []
        review_index_status = "missing"
        if settings.XTJS_REVIEW_INDEX_ENABLED and UUID_TEXT_PATTERN.fullmatch(pid):
            encoded, result_version, review_summary, component_rows, issue_rows = prepare_review_storage(
                encoded,
                project_identifier_id=pid,
            )
            review_index_status = "ready"
        else:
            result_version = build_result_version(encoded)
        result_object_key = document_blob_store.save_project_result(
            encoded,
            project_name=project.get("project_name"),
            project_identifier_id=pid,
        )
        result_keys = sorted(k for k in (encoded or {}).keys()) if isinstance(encoded, dict) else []
        if review_index_status == "ready":
            cursor.execute("DELETE FROM xtjs_review_issues WHERE project_identifier_id=%s", (pid,))
            cursor.execute("DELETE FROM xtjs_result_components WHERE project_identifier_id=%s", (pid,))
            if component_rows:
                cursor.executemany(
                    """INSERT INTO xtjs_result_components
                       (project_identifier_id,result_version,result_key,object_key,summary,issue_count)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    [
                        (
                            pid,
                            result_version,
                            item["result_key"],
                            item["object_key"],
                            Json(item["summary"]),
                            item["issue_count"],
                        )
                        for item in component_rows
                    ],
                )
            if issue_rows:
                cursor.executemany(
                    """INSERT INTO xtjs_review_issues
                       (project_identifier_id,result_version,result_key,issue_id,issue_order,
                        risk_level,status,check_code,title,description,file_names,list_payload,
                        detail_object_key,evidence_object_key,evidence_count)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [
                        (
                            pid,
                            result_version,
                            item["result_key"],
                            item["issue_id"],
                            item["issue_order"],
                            item["risk_level"],
                            item["status"],
                            item["check_code"],
                            item["title"],
                            item["description"],
                            Json(item["file_names"]),
                            Json(item["list_payload"]),
                            item["detail_object_key"],
                            item["evidence_object_key"],
                            item["evidence_count"],
                        )
                        for item in issue_rows
                    ],
                )
        cursor.execute(self._RESULT_UPSERT_SQL, (
            pid,
            result_object_key,
            Json(result_keys),
            Json(build_project_result_summary(encoded)),
            Json(workflow_scope_from_result_record({"result": encoded})),
            project.get("input_revision", 0),
            result_version,
            Json(review_summary) if review_summary is not None else None,
            review_index_status,
        ))
        record = dict(cursor.fetchone())
        # 注入刚写入的内容，避免 _sanitize 立刻再读一次 MinIO。
        record["result"] = encoded
        record["result_object_key"] = result_object_key
        return self._sanitize_project_result_record(record)

    @contextmanager
    def _locked_project_result(self, identifier_id: str):
        """Serialize read/merge/write across processes, including the first result insert."""
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                pid = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute("""SELECT identifier_id,project_name,upload_manifest,input_revision FROM xtjs_projects
                    WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE""", (pid,))
                project = cursor.fetchone()
                if not project:
                    raise ValueError(f"项目不存在：{identifier_id}")
                cursor.execute("SELECT * FROM xtjs_result WHERE project_identifier_id=%s", (pid,))
                row = cursor.fetchone()
                self.assert_input_revision(pid, project.get("input_revision", 0))
                if row and row.get("input_revision", 0) != project.get("input_revision", 0):
                    # History was archived in the material-change transaction. Only the scope survives.
                    row = {"result": {}, "workflow_scope": row.get("workflow_scope")}
                existing = self._sanitize_project_result_record(dict(row)) if row else {}
                yield cursor, dict(project), existing

    @access_check(project_identifier_id='project')
    def create_or_replace_project_result(
        self,
        project_identifier_id: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建或完全覆盖项目的分析结果。"""
        if not isinstance(result, dict):
            raise ValueError("result must be a JSON object")
        with self._locked_project_result(project_identifier_id) as (cursor, project, existing):
            persisted_result = self._prepare_project_result_for_persistence(
                str(project["identifier_id"]), result, existing=existing,
            )
            return self._persist_project_result(cursor, project, persisted_result)

    @access_check(project_identifier_id='project')
    def delete_project_result(self, project_identifier_id: str) -> bool:
        """删除项目分析结果记录。"""
        query = """
            DELETE FROM xtjs_result
            WHERE project_identifier_id = %s
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_project_identifier = self._resolve_project_identifier(
                    cursor,
                    project_identifier_id,
                )
                cursor.execute(
                    "DELETE FROM xtjs_review_issues WHERE project_identifier_id = %s",
                    (normalized_project_identifier,),
                )
                cursor.execute(
                    "DELETE FROM xtjs_result_components WHERE project_identifier_id = %s",
                    (normalized_project_identifier,),
                )
                cursor.execute(query, (normalized_project_identifier,))
                return cursor.rowcount > 0

    @access_check(project_identifier_ids='project')
    def delete_project_results(self, project_identifier_ids: list[str]) -> int:
        """批量删除项目分析结果记录。"""
        normalized_ids = [
            self._normalize_required_identifier(project_identifier_id, "project_identifier_id")
            for project_identifier_id in project_identifier_ids
            if str(project_identifier_id or "").strip()
        ]
        if not normalized_ids:
            return 0
        query = """
            DELETE FROM xtjs_result
            WHERE project_identifier_id = ANY(%s::uuid[])
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (normalized_ids,))
                return int(cursor.rowcount or 0)

    @access_check(project_identifier_id='project')
    def upsert_project_result_item(
        self,
        project_identifier_id: str,
        result_key: str,
        result_value: Dict[str, Any],
    ) -> Dict[str, Any]:
        """向项目结果中合并一个键值对（保留已有键）。"""
        normalized_result_key = self._normalize_required_identifier(result_key, "result_key")
        if not isinstance(result_value, dict):
            raise ValueError("result_value must be a JSON object")

        with self._locked_project_result(project_identifier_id) as (cursor, project, existing):
            payload = self._prepare_project_result_for_persistence(
                str(project["identifier_id"]), {normalized_result_key: result_value}, existing=existing,
            )
            merged = dict(existing.get("result") or {})
            merged.pop("project_file_urls", None)
            merged.update(payload)
            return self._persist_project_result(cursor, project, merged)

    @access_check(project_identifier_id='project')
    def update_project_manual_review_result(
        self,
        project_identifier_id: str,
        result_key: str,
        result_value: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Save a rerun/manual judgment result under result.manual_review_results.latest."""
        normalized_result_key = self._normalize_required_identifier(result_key, "result_key")
        if not isinstance(result_value, dict):
            raise ValueError("result_value must be a JSON object")

        with self._locked_project_result(project_identifier_id) as (cursor, project, existing):
            existing_result = dict(existing.get("result") or {})
            manual_review_results = build_manual_review_results(
                manual_review_results_from_record(existing),
                latest_key=normalized_result_key,
                latest_value=result_value,
            )
            existing_result[MANUAL_REVIEW_RESULTS_KEY] = manual_review_results
            return self._persist_project_result(cursor, project, existing_result)

    @access_check(project_identifier_id='project')
    def clear_project_manual_review_latest_result(
        self,
        project_identifier_id: str,
        result_key: str,
    ) -> Dict[str, Any]:
        """Remove one result key from result.manual_review_results.latest."""
        normalized_result_key = self._normalize_required_identifier(result_key, "result_key")

        with self._locked_project_result(project_identifier_id) as (cursor, project, existing):
            existing_result = dict(existing.get("result") or {})
            manual_review_results = manual_review_results_from_record(existing)
            latest = dict(manual_review_results.get("latest") or {})
            if normalized_result_key not in latest:
                return existing

            latest.pop(normalized_result_key, None)
            manual_review_results["latest"] = latest
            manual_review_results["updated_at"] = utc_now_iso()

            if latest or manual_review_results.get("workflow_scope"):
                existing_result[MANUAL_REVIEW_RESULTS_KEY] = manual_review_results
            else:
                existing_result.pop(MANUAL_REVIEW_RESULTS_KEY, None)
            return self._persist_project_result(cursor, project, existing_result)

    @access_check(project_identifier_id='project')
    def update_project_manual_review_workflow_scope(
        self,
        project_identifier_id: str,
        workflow_scope: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Save project-level manual workflow scope under result.manual_review_results."""
        if not isinstance(workflow_scope, dict):
            raise ValueError("workflow_scope must be a JSON object")

        with self._locked_project_result(project_identifier_id) as (cursor, project, existing):
            existing_result = dict(existing.get("result") or {})
            manual_review_results = build_manual_review_results(
                manual_review_results_from_record(existing),
                workflow_scope=workflow_scope,
            )
            existing_result[MANUAL_REVIEW_RESULTS_KEY] = manual_review_results
            return self._persist_project_result(cursor, project, existing_result)
