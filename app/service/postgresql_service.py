# -*- coding: utf-8 -*-
"""
PostgreSQL 数据访问服务模块。

提供连接池管理及项目、文档、关联关系、分析结果的 CRUD 操作。
"""

import logging
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from uuid import uuid4

import psycopg2
from fastapi.encoders import jsonable_encoder
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

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

logger = logging.getLogger(__name__)

UUID_TEXT = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID_TEXT_PATTERN = re.compile(rf"(?i)\b{UUID_TEXT}\b")
UUID_SUFFIX_PATTERN = re.compile(rf"(?i)\(({UUID_TEXT})\)\s*$")
MISSING_UUID_SENTINEL = "00000000-0000-0000-0000-000000000000"

# 全局连接池（模块级单例）
_db_pool = None


def get_db_pool():
    """返回 PostgreSQL 线程安全连接池，首次调用时初始化。"""
    global _db_pool
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


class PostgreSQLService:
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
        conn = pool.getconn()
        try:
            with conn:
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
    def _normalize_identifier(identifier_id: Optional[str]) -> str:
        """若传入标识为空则自动生成 UUID。"""
        identifier = PostgreSQLService._extract_identifier(identifier_id)
        return identifier or str(uuid4())

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
        normalized = self._normalize_required_identifier(identifier_or_name, "identifier_id")
        if UUID_TEXT_PATTERN.fullmatch(normalized):
            return normalized

        cursor.execute(
            """
            SELECT identifier_id
            FROM xtjs_projects
            WHERE project_name = %s AND deleted = FALSE
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
        normalized = self._normalize_required_identifier(identifier_or_file_name, "identifier_id")
        if UUID_TEXT_PATTERN.fullmatch(normalized):
            return normalized

        cursor.execute(
            """
            SELECT identifier_id
            FROM xtjs_documents
            WHERE file_name = %s AND deleted = FALSE
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
                RETURNING identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
            """
            values = (normalized_identifier, normalized_project_name, self.PARSING_STATUS_UPLOADED)
        else:
            query = """
                INSERT INTO xtjs_projects (project_name, parsing_status)
                VALUES (%s, %s)
                RETURNING identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
            """
            values = (normalized_project_name, self.PARSING_STATUS_UPLOADED)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, values)
                return self._decorate_project_record(dict(cursor.fetchone()))

    def get_project_by_name(self, project_name: str) -> Optional[Dict[str, Any]]:
        """根据项目名称获取未删除项目。"""
        normalized_project_name = self._normalize_project_name(project_name)
        query = """
            SELECT identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE project_name = %s AND deleted = FALSE
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
        conditions = ["p.deleted = FALSE"]
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
                        p.parsing_status,
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
                        res.result_update_time
                    FROM xtjs_projects p
                    LEFT JOIN LATERAL (
                        SELECT
                            COUNT(*) AS relation_count,
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
                        SELECT
                            (COALESCE(r.result, '{{}}'::jsonb) <> '{{}}'::jsonb) AS result_available,
                            COALESCE(
                                (
                                    SELECT COUNT(*)
                                    FROM jsonb_object_keys(COALESCE(r.result, '{{}}'::jsonb)) AS result_key
                                ),
                                0
                            ) AS analysis_result_count,
                            COALESCE(
                                (
                                    SELECT jsonb_agg(result_key ORDER BY result_key)
                                    FROM jsonb_object_keys(COALESCE(r.result, '{{}}'::jsonb)) AS result_key
                                ),
                                '[]'::jsonb
                            ) AS available_result_keys,
                            r.update_time AS result_update_time
                        FROM xtjs_result r
                        WHERE r.project_identifier_id = p.identifier_id
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

    def list_project_identifiers(self) -> List[str]:
        """获取所有未删除项目的标识列表。"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT identifier_id
                    FROM xtjs_projects
                    WHERE deleted = FALSE
                    ORDER BY create_time DESC, identifier_id DESC
                    """
                )
                return [str(identifier_id) for (identifier_id,) in cursor.fetchall()]

    def list_project_display_choices(self) -> List[str]:
        """获取 Swagger 使用的项目下拉显示值。"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT project_name
                    FROM xtjs_projects
                    WHERE deleted = FALSE
                    ORDER BY create_time DESC, identifier_id DESC
                    """
                )
                return [str(project_name) for (project_name,) in cursor.fetchall()]

    def list_document_display_choices(
        self,
        document_type: Optional[str] = None,
    ) -> List[str]:
        """获取 Swagger 使用的文档下拉显示值；文件名重复时附带 UUID。"""
        normalized_type = (document_type or "").strip().lower()
        conditions = ["deleted = FALSE"]
        values: List[Any] = []
        if normalized_type:
            conditions.append("document_type = %s")
            values.append(self._normalize_document_type(normalized_type))
        where_clause = " AND ".join(conditions)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        identifier_id,
                        file_name,
                        COUNT(*) OVER (PARTITION BY file_name) AS same_name_count
                    FROM xtjs_documents
                    WHERE {where_clause}
                    ORDER BY create_time DESC, identifier_id DESC
                    """,
                    tuple(values),
                )
                choices: List[str] = []
                for row in cursor.fetchall():
                    file_name = str(row["file_name"])
                    if int(row.get("same_name_count") or 0) > 1:
                        choices.append(f"{file_name} ({row['identifier_id']})")
                    else:
                        choices.append(file_name)
                return choices

    def get_project_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """根据标识获取项目记录。"""
        query = """
            SELECT identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (resolved_identifier,))
                result = cursor.fetchone()
                return self._decorate_project_record(dict(result)) if result else None

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
            RETURNING identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, tuple(values + [resolved_identifier]))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

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
            RETURNING identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                resolved_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (normalized_report_url, resolved_identifier))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    def update_project_parsing_status(
        self,
        identifier_id: str,
        parsing_status: int,
    ) -> Optional[Dict[str, Any]]:
        # 路由层统一通过这里同步项目 OCR 阶段状态。
        normalized_status = self._normalize_parsing_status(parsing_status)
        query = """
            UPDATE xtjs_projects
            SET parsing_status = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING identifier_id, project_name, parsing_status, report_url, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(query, (normalized_status, normalized_identifier))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    def refresh_project_parsing_status(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """按项目下文档 extracted 状态重算 0/1/2/3 的 OCR 阶段。"""
        status_query = """
            WITH project_row AS (
                SELECT identifier_id
                FROM xtjs_projects
                WHERE identifier_id = %s AND deleted = FALSE
            ),
            document_stats AS (
                SELECT
                    -- 这里按“文档类型整体是否全部 extracted”来推进项目阶段。
                    COUNT(DISTINCT td.identifier_id) AS tender_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(td.extracted, FALSE) = TRUE THEN td.identifier_id END) AS tender_extracted_count,
                    COUNT(DISTINCT bbd.identifier_id) AS business_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(bbd.extracted, FALSE) = TRUE THEN bbd.identifier_id END) AS business_extracted_count,
                    COUNT(DISTINCT CASE WHEN pd.technical_bid_document_id IS NOT NULL THEN tbd.identifier_id END) AS technical_count,
                    COUNT(
                        DISTINCT CASE
                            WHEN pd.technical_bid_document_id IS NOT NULL
                             AND COALESCE(tbd.extracted, FALSE) = TRUE
                            THEN tbd.identifier_id
                        END
                    ) AS technical_extracted_count
                FROM project_row pr
                JOIN xtjs_project_documents pd
                  ON pd.project_id = pr.identifier_id
                JOIN xtjs_documents td
                  ON td.identifier_id = pd.tender_document_id
                 AND td.deleted = FALSE
                JOIN xtjs_documents bbd
                  ON bbd.identifier_id = pd.business_bid_document_id
                 AND bbd.deleted = FALSE
                LEFT JOIN xtjs_documents tbd
                  ON tbd.identifier_id = pd.technical_bid_document_id
                 AND tbd.deleted = FALSE
            )
            SELECT CASE
                -- 招标文件没完成前，整个项目仍视为未开始 OCR。
                WHEN tender_count = 0
                  OR tender_extracted_count < tender_count
                THEN 0
                -- 招标完成后，只要商务标没全部完成，就停留在状态 1。
                WHEN business_count = 0
                  OR business_extracted_count < business_count
                THEN 1
                -- 商务标完成后，若没有技术标或技术标未全部完成，则停留在状态 2。
                WHEN technical_count = 0
                  OR technical_extracted_count < technical_count
                THEN 2
                ELSE 3
            END AS parsing_status
            FROM document_stats
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_project_identifier(cursor, identifier_id)
                cursor.execute(status_query, (normalized_identifier,))
                row = cursor.fetchone()
                if row is None:
                    return None
                return self.update_project_parsing_status(
                    normalized_identifier,
                    int(row.get("parsing_status") or 0),
                )

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
                cursor.execute(query, (normalized_ids,))
                return int(cursor.rowcount or 0)

    # 文档 CRUD
    def create_document(
        self,
        file_name: str,
        file_url: str,
        document_type: str,
        identifier_id: Optional[str] = None,
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
                        INSERT INTO xtjs_documents (identifier_id, document_type, file_name, file_url)
                        VALUES (%s, %s, %s, %s)
                        RETURNING
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            extracted,
                            content,
                            deleted,
                            create_time,
                            update_time
                        """,
                        (
                            identifier,
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (document_type, file_name, file_url)
                        VALUES (%s, %s, %s)
                        RETURNING
                            identifier_id,
                            document_type,
                            file_name,
                            file_url,
                            extracted,
                            content,
                            deleted,
                            create_time,
                            update_time
                        """,
                        (
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
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
                        INSERT INTO xtjs_documents (identifier_id, document_type, file_name, file_url)
                        VALUES (%s, %s, %s, %s)
                        RETURNING identifier_id, document_type
                        """,
                        (
                            identifier,
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO xtjs_documents (document_type, file_name, file_url)
                        VALUES (%s, %s, %s)
                        RETURNING identifier_id, document_type
                        """,
                        (
                            normalized_document_type,
                            normalized_file_name,
                            normalized_file_url,
                        ),
                    )
                document = dict(cursor.fetchone())

                cursor.execute(
                    """
                    UPDATE xtjs_documents
                    SET content = %s, extracted = TRUE, update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s
                    RETURNING
                        identifier_id,
                        document_type,
                        file_name,
                        file_url,
                        extracted,
                        content,
                        deleted,
                        create_time,
                        update_time
                    """,
                    (Json(recognition_content), document["identifier_id"]),
                )
                updated_document = dict(cursor.fetchone())

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
        conditions = ["deleted = FALSE"]
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
                        file_name,
                        file_url,
                        extracted,
                        content,
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

    def get_document_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """根据标识获取文档完整信息。"""
        query = """
            SELECT
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
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
                return dict(result) if result else None

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
                deleted,
                create_time,
                update_time
        """

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                values.append(self._resolve_document_identifier(cursor, identifier_id))
                cursor.execute(query, tuple(values))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def update_document_content(
        self,
        identifier_id: str,
        recognition_content: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """覆盖写入文档的识别内容，并标记为已提取。"""
        if not isinstance(recognition_content, dict):
            raise ValueError("recognition_content 必须是 JSON 对象")

        query = """
            UPDATE xtjs_documents
            SET content = %s, extracted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                identifier_id,
                document_type,
                file_name,
                file_url,
                extracted,
                content,
                deleted,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_identifier = self._resolve_document_identifier(cursor, identifier_id)
                cursor.execute(query, (Json(recognition_content), normalized_identifier))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

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

    # 项目-文档关系管理
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
                    SELECT id
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
                    raise ValueError(
                        "当前招标文件、商务标文件、技术标文件的关联关系已存在"
                    )

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
                return {
                    **binding,
                    "project_identifier": project["identifier_id"],
                    "tender_document_identifier": tender["identifier_id"],
                    "business_bid_document_identifier": business_bid["identifier_id"],
                    "technical_bid_document_identifier": (
                        technical_bid["identifier_id"] if technical_bid else None
                    ),
                }

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
                return dict(relation) if relation else None

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
                items: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return self._build_paginated_response(
            total=total,
            limit=normalized_limit,
            offset=normalized_offset,
            items=items,
        )

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

    def delete_relation(self, relation_id: int) -> bool:
        """物理删除一条项目文档关系。"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM xtjs_project_documents
                    WHERE id = %s
                    """,
                    (relation_id,),
                )
                return cursor.rowcount > 0

    def delete_relations(self, relation_ids: list[int]) -> int:
        """批量物理删除项目文档关系。"""
        normalized_ids = [int(relation_id) for relation_id in relation_ids if relation_id is not None]
        if not normalized_ids:
            return 0
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM xtjs_project_documents
                    WHERE id = ANY(%s)
                    """,
                    (normalized_ids,),
                )
                return int(cursor.rowcount or 0)

    # 项目详情及查重/审查文档集
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
                relations: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
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
                append_location(locations, location)
        for raw_location in collect_locations(original.get("evidence")):
            location_defaults = cls._location_defaults_for_location(
                defaults,
                raw_location,
                context,
            )
            for location in normalize_locations(raw_location, defaults=location_defaults):
                append_location(locations, location)
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
        if "result" in payload:
            payload["result"] = cls._strip_legacy_project_file_urls(payload.get("result"))
        if payload.get("result_fot_frontend") is None:
            payload["result_fot_frontend"] = {}
        return payload

    def _prepare_project_result_for_persistence(
        self,
        project_identifier_id: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        project_detail = self.get_project_detail(project_identifier_id)
        source_index = self._build_project_document_source_index(project_detail)
        return self._enrich_result_node_with_document_sources(
            dict(result or {}),
            source_index,
        )

    def get_project_documents_for_duplicate_check(
        self,
        identifier_id: str,
    ) -> Optional[Dict[str, Any]]:
        """获取项目下所有文档记录（含内容），用于查重/审查服务。"""
        project = self.get_project_by_identifier(identifier_id)
        if not project:
            return None

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
                bbd.content,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                td.extracted AS tender_extracted,
                td.content AS tender_content,
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
                tbd.content,
                td.identifier_id AS tender_identifier_id,
                td.document_type AS tender_document_type,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                td.extracted AS tender_extracted,
                td.content AS tender_content,
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

        return {"project": project, "documents": documents}

    # 分析结果管理
    def get_project_result(self, project_identifier_id: str) -> Optional[Dict[str, Any]]:
        """获取项目分析结果记录。"""
        query = """
            SELECT
                id,
                project_identifier_id,
                result,
                result_fot_frontend,
                create_time,
                update_time
            FROM xtjs_result
            WHERE project_identifier_id = %s
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                normalized_project_identifier = self._resolve_project_identifier(
                    cursor,
                    project_identifier_id,
                )
                cursor.execute(query, (normalized_project_identifier,))
                result = cursor.fetchone()
                return self._sanitize_project_result_record(dict(result)) if result else None

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
        conditions = ["p.deleted = FALSE"]
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
                r.result_fot_frontend,
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

    def create_or_replace_project_result(
        self,
        project_identifier_id: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建或完全覆盖项目的分析结果。"""
        if not isinstance(result, dict):
            raise ValueError("result must be a JSON object")
        project = self.get_project_by_identifier(project_identifier_id)
        if not project:
            raise ValueError(f"项目不存在：{project_identifier_id}")
        normalized_project_identifier = str(project["identifier_id"])
        persisted_result = self._prepare_project_result_for_persistence(
            normalized_project_identifier,
            result,
        )

        query = """
            INSERT INTO xtjs_result (project_identifier_id, result)
            VALUES (%s, %s)
            ON CONFLICT (project_identifier_id)
            DO UPDATE
            SET
                result = EXCLUDED.result,
                update_time = CURRENT_TIMESTAMP
            RETURNING
                id,
                project_identifier_id,
                result,
                result_fot_frontend,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        normalized_project_identifier,
                        Json(jsonable_encoder(persisted_result)),
                    ),
                )
                return self._sanitize_project_result_record(dict(cursor.fetchone()))

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
                cursor.execute(query, (normalized_project_identifier,))
                return cursor.rowcount > 0

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

    def update_project_result_for_frontend(
        self,
        project_identifier_id: str,
        result_fot_frontend: Dict[str, Any],
    ) -> Dict[str, Any]:
        """单独更新前端删减后的项目结果，不改动原始分析 result。"""
        if not isinstance(result_fot_frontend, dict):
            raise ValueError("result_fot_frontend must be a JSON object")

        project = self.get_project_by_identifier(project_identifier_id)
        if not project:
            raise ValueError(f"项目不存在：{project_identifier_id}")
        normalized_project_identifier = str(project["identifier_id"])

        query = """
            INSERT INTO xtjs_result (project_identifier_id, result, result_fot_frontend)
            VALUES (%s, '{}'::jsonb, %s)
            ON CONFLICT (project_identifier_id)
            DO UPDATE
            SET
                result_fot_frontend = EXCLUDED.result_fot_frontend,
                update_time = CURRENT_TIMESTAMP
            RETURNING
                id,
                project_identifier_id,
                result,
                result_fot_frontend,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        normalized_project_identifier,
                        Json(jsonable_encoder(result_fot_frontend)),
                    ),
                )
                return self._sanitize_project_result_record(dict(cursor.fetchone()))

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

        project = self.get_project_by_identifier(project_identifier_id)
        if not project:
            raise ValueError(f"项目不存在：{project_identifier_id}")
        normalized_project_identifier = str(project["identifier_id"])

        payload = self._prepare_project_result_for_persistence(
            normalized_project_identifier,
            {normalized_result_key: result_value},
        )
        query = """
            INSERT INTO xtjs_result (project_identifier_id, result)
            VALUES (%s, %s)
            ON CONFLICT (project_identifier_id)
            DO UPDATE
            SET
                result = (COALESCE(xtjs_result.result, '{}'::jsonb) - 'project_file_urls') || EXCLUDED.result,
                update_time = CURRENT_TIMESTAMP
            RETURNING
                id,
                project_identifier_id,
                result,
                result_fot_frontend,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        normalized_project_identifier,
                        Json(jsonable_encoder(payload)),
                    ),
                )
                return self._sanitize_project_result_record(dict(cursor.fetchone()))
