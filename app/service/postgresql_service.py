# -*- coding: utf-8 -*-
"""
PostgreSQL 数据访问服务模块。

提供连接池管理及项目、文档、关联关系、分析结果的 CRUD 操作。
"""

import logging
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

logger = logging.getLogger(__name__)

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
    # 项目解析状态：0 表示未全部完成 OCR，1 表示项目关联的招标文件、商务标、技术标均已完成 OCR。
    PARSING_STATUS_PENDING = 0
    PARSING_STATUS_COMPLETED = 1
    # 兼容旧代码里曾用到的常量名，实际仍只保留“未全部完成 / 全部完成”两种状态。
    PARSING_STATUS_UPLOADED = PARSING_STATUS_PENDING
    PARSING_STATUS_BUSINESS_OCR_COMPLETED = PARSING_STATUS_COMPLETED
    PARSING_STATUS_TECHNICAL_OCR_COMPLETED = PARSING_STATUS_COMPLETED
    PARSING_STATUS_LABELS = {
        PARSING_STATUS_PENDING: "pending",
        PARSING_STATUS_COMPLETED: "completed",
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
    def _normalize_identifier(identifier_id: Optional[str]) -> str:
        """若传入标识为空则自动生成 UUID。"""
        identifier = (identifier_id or "").strip()
        return identifier or str(uuid4())

    @staticmethod
    def _normalize_required_identifier(identifier_id: str, field_name: str) -> str:
        """验证标识非空并返回清理后的值。"""
        normalized = (identifier_id or "").strip()
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

    @classmethod
    def _normalize_parsing_status(cls, parsing_status: Optional[int]) -> int:
        # 兼容历史遗留值；当前统一只使用 0=未全部完成 OCR、1=全部完成 OCR。
        try:
            normalized = int(parsing_status or 0)
        except (TypeError, ValueError):
            normalized = cls.PARSING_STATUS_PENDING
        normalized = cls.PARSING_STATUS_COMPLETED if normalized > 0 else cls.PARSING_STATUS_PENDING
        return normalized

    @classmethod
    def _decorate_project_record(cls, project: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not project:
            return project
        decorated = dict(project)
        # 对所有项目查询结果补充状态标签，避免路由层重复拼装。
        normalized = cls._normalize_parsing_status(decorated.get("parsing_status"))
        decorated["parsing_status"] = normalized
        decorated["parsing_status_label"] = cls.PARSING_STATUS_LABELS[normalized]
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
    def _get_project_record(self, cursor, identifier_id: str) -> Optional[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT id, identifier_id, parsing_status
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
            """,
            (identifier_id,),
        )
        project = cursor.fetchone()
        return self._decorate_project_record(dict(project)) if project else None

    def _get_document_record(self, cursor, identifier_id: str) -> Optional[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT id, identifier_id, document_type
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
            ORDER BY id ASC
            LIMIT 1
            """,
            (identifier_id,),
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
        identifier_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建项目，支持自动生成标识。"""
        identifier = self._normalize_identifier(identifier_id)
        query = """
            INSERT INTO xtjs_projects (identifier_id, parsing_status)
            VALUES (%s, %s)
            RETURNING id, identifier_id, parsing_status, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier, self.PARSING_STATUS_UPLOADED))
                return self._decorate_project_record(dict(cursor.fetchone()))

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
            conditions.append("p.identifier_id ILIKE %s")
            keyword_like = f"%{normalized_keyword}%"
            values.append(keyword_like)
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
                        p.id,
                        p.identifier_id,
                        p.parsing_status,
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
                            WHERE pd2.project_id = p.id
                            UNION
                            SELECT pd2.business_bid_document_id AS doc_id
                            FROM xtjs_project_documents pd2
                            WHERE pd2.project_id = p.id
                            UNION
                            SELECT pd2.technical_bid_document_id AS doc_id
                            FROM xtjs_project_documents pd2
                            WHERE pd2.project_id = p.id AND pd2.technical_bid_document_id IS NOT NULL
                        ) docs ON TRUE
                        LEFT JOIN xtjs_documents doc_meta
                            ON doc_meta.id = docs.doc_id
                           AND doc_meta.deleted = FALSE
                        WHERE pd.project_id = p.id
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
                    ORDER BY p.create_time DESC, p.id DESC
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
                    ORDER BY create_time DESC, id DESC
                    """
                )
                return [str(identifier_id) for (identifier_id,) in cursor.fetchall()]

    def get_project_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """根据标识获取项目记录。"""
        query = """
            SELECT id, identifier_id, parsing_status, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier_id,))
                result = cursor.fetchone()
                return self._decorate_project_record(dict(result)) if result else None

    def update_project(
        self,
        identifier_id: str,
        new_identifier_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新项目标识（仅允许修改标识）。"""
        updates: List[str] = []
        values: List[Any] = []
        if new_identifier_id is not None:
            updates.append("identifier_id = %s")
            values.append(self._normalize_required_identifier(new_identifier_id, "new_identifier_id"))
        if not updates:
            raise ValueError("at least one project field must be provided")

        query = f"""
            UPDATE xtjs_projects
            SET {", ".join(updates)}, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING id, identifier_id, parsing_status, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(values + [identifier_id]))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    def update_project_parsing_status(
        self,
        identifier_id: str,
        parsing_status: int,
    ) -> Optional[Dict[str, Any]]:
        # 提供统一入口给路由层同步项目“是否全部完成 OCR”的状态。
        normalized_identifier = self._normalize_required_identifier(identifier_id, "identifier_id")
        normalized_status = self._normalize_parsing_status(parsing_status)
        query = """
            UPDATE xtjs_projects
            SET parsing_status = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING id, identifier_id, parsing_status, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized_status, normalized_identifier))
                updated = cursor.fetchone()
                return self._decorate_project_record(dict(updated)) if updated else None

    def refresh_project_parsing_status(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """按项目下文档的 extracted 状态重算 parsing_status：全完成为 1，否则为 0。"""
        normalized_identifier = self._normalize_required_identifier(identifier_id, "identifier_id")
        status_query = """
            WITH project_row AS (
                SELECT id
                FROM xtjs_projects
                WHERE identifier_id = %s AND deleted = FALSE
            ),
            document_flags AS (
                SELECT
                    COALESCE(td.extracted, FALSE) AS tender_extracted,
                    COALESCE(bbd.extracted, FALSE) AS business_extracted,
                    CASE
                        WHEN pd.technical_bid_document_id IS NULL THEN TRUE
                        ELSE COALESCE(tbd.extracted, FALSE)
                    END AS technical_extracted
                FROM project_row pr
                JOIN xtjs_project_documents pd
                  ON pd.project_id = pr.id
                JOIN xtjs_documents td
                  ON td.id = pd.tender_document_id
                 AND td.deleted = FALSE
                JOIN xtjs_documents bbd
                  ON bbd.id = pd.business_bid_document_id
                 AND bbd.deleted = FALSE
                LEFT JOIN xtjs_documents tbd
                  ON tbd.id = pd.technical_bid_document_id
                 AND tbd.deleted = FALSE
            )
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM document_flags)
                 AND NOT EXISTS (
                     SELECT 1
                     FROM document_flags
                     WHERE tender_extracted = FALSE
                        OR business_extracted = FALSE
                        OR technical_extracted = FALSE
                 )
                THEN 1
                ELSE 0
            END AS parsing_status
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
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
                cursor.execute(query, (identifier_id,))
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
            WHERE identifier_id = ANY(%s) AND deleted = FALSE
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
        """创建文档记录（不含识别内容）。"""
        identifier = self._normalize_identifier(identifier_id)
        normalized_file_name = self._normalize_file_value(file_name, "file_name")
        normalized_file_url = self._normalize_file_value(file_url, "file_url")
        normalized_document_type = self._normalize_document_type(document_type)

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                existing = self._get_document_record(cursor, identifier)
                if existing:
                    raise ValueError(f"文档标识已存在：{identifier}")

                cursor.execute(
                    """
                    INSERT INTO xtjs_documents (identifier_id, document_type, file_name, file_url)
                    VALUES (%s, %s, %s, %s)
                    RETURNING
                        id,
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
                return dict(cursor.fetchone())

    def create_document_with_content(
        self,
        file_name: str,
        file_url: str,
        document_type: str,
        recognition_content: Dict[str, Any],
        identifier_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建文档记录并同时写入识别内容。"""
        identifier = self._normalize_identifier(identifier_id)
        normalized_file_name = self._normalize_file_value(file_name, "file_name")
        normalized_file_url = self._normalize_file_value(file_url, "file_url")
        normalized_document_type = self._normalize_document_type(document_type)

        if not isinstance(recognition_content, dict):
            raise ValueError("recognition_content 必须是 JSON 对象")

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                existing = self._get_document_record(cursor, identifier)
                if existing:
                    raise ValueError(f"文档标识已存在：{identifier}")

                cursor.execute(
                    """
                    INSERT INTO xtjs_documents (identifier_id, document_type, file_name, file_url)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, identifier_id, document_type
                    """,
                    (
                        identifier,
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
                    WHERE id = %s
                    RETURNING
                        id,
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
                    (Json(recognition_content), document["id"]),
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
            conditions.append("(identifier_id ILIKE %s OR file_name ILIKE %s)")
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
                        id,
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
                    ORDER BY create_time DESC, id DESC
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
                id,
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
            ORDER BY id ASC
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier_id,))
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
        values.append(identifier_id)
        query = f"""
            UPDATE xtjs_documents
            SET {", ".join(updates)}
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                id,
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
                cursor.execute(query, tuple(values))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def update_document_content(
        self,
        identifier_id: str,
        recognition_content: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """覆盖写入文档的识别内容，并标记为已提取。"""
        normalized_identifier = self._normalize_required_identifier(identifier_id, "identifier_id")
        if not isinstance(recognition_content, dict):
            raise ValueError("recognition_content 必须是 JSON 对象")

        query = """
            UPDATE xtjs_documents
            SET content = %s, extracted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING
                id,
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
                cursor.execute(query, (identifier_id,))
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
            WHERE identifier_id = ANY(%s) AND deleted = FALSE
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
                        project["id"],
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"] if technical_bid else None,
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
                        project["id"],
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"] if technical_bid else None,
                    ),
                )
                binding = dict(cursor.fetchone())
                return {
                    **binding,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender_document_identifier,
                    "business_bid_document_identifier": business_bid_document_identifier,
                    "technical_bid_document_identifier": normalized_technical_identifier,
                }

    def get_relation_by_id(self, relation_id: int) -> Optional[Dict[str, Any]]:
        """根据关系 ID 获取绑定详情。"""
        query = """
            SELECT
                pd.id AS relation_id,
                p.identifier_id AS project_identifier,
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
            JOIN xtjs_projects p ON pd.project_id = p.id AND p.deleted = FALSE
            JOIN xtjs_documents td ON pd.tender_document_id = td.id AND td.deleted = FALSE
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.id AND bbd.deleted = FALSE
            LEFT JOIN xtjs_documents tbd
                ON pd.technical_bid_document_id = tbd.id AND tbd.deleted = FALSE
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
            conditions.append("p.identifier_id = %s")
            values.append(normalized_project_identifier)
        if normalized_keyword:
            keyword_like = f"%{normalized_keyword}%"
            conditions.append(
                """
                (
                    p.identifier_id ILIKE %s
                    OR td.identifier_id ILIKE %s
                    OR td.file_name ILIKE %s
                    OR bbd.identifier_id ILIKE %s
                    OR bbd.file_name ILIKE %s
                    OR COALESCE(tbd.identifier_id, '') ILIKE %s
                    OR COALESCE(tbd.file_name, '') ILIKE %s
                )
                """
            )
            values.extend([keyword_like] * 7)
        where_clause = " AND ".join(conditions)

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM xtjs_project_documents pd
            JOIN xtjs_projects p ON pd.project_id = p.id
            JOIN xtjs_documents td ON pd.tender_document_id = td.id
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.id
            LEFT JOIN xtjs_documents tbd ON pd.technical_bid_document_id = tbd.id
            WHERE {where_clause}
        """
        data_query = f"""
            SELECT
                pd.id AS relation_id,
                p.identifier_id AS project_identifier,
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
            JOIN xtjs_projects p ON pd.project_id = p.id
            JOIN xtjs_documents td ON pd.tender_document_id = td.id
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.id
            LEFT JOIN xtjs_documents tbd ON pd.technical_bid_document_id = tbd.id
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
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"] if technical_bid else None,
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
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"] if technical_bid else None,
                        relation_id,
                    ),
                )
                updated = dict(cursor.fetchone())

                cursor.execute(
                    """
                    SELECT identifier_id
                    FROM xtjs_projects
                    WHERE id = %s
                    """,
                    (updated["project_id"],),
                )
                project = cursor.fetchone()
                project_identifier = project["identifier_id"] if project else ""
                return {
                    **updated,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender_document_identifier,
                    "business_bid_document_identifier": business_bid_document_identifier,
                    "technical_bid_document_identifier": normalized_technical_identifier,
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

                self._get_required_document_record(
                    cursor,
                    normalized_business_identifier,
                    role_label="商务标文件",
                    allowed_types=set(BUSINESS_BID_COMPATIBLE_TYPES),
                )
                self._get_required_document_record(
                    cursor,
                    normalized_technical_identifier,
                    role_label="技术标文件",
                    allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                )

                values: list[Any] = [project["id"], normalized_business_identifier]
                tender_filter = ""
                if normalized_tender_identifier:
                    tender_filter = "AND td.identifier_id = %s"
                    values.append(normalized_tender_identifier)

                cursor.execute(
                    f"""
                    SELECT
                        pd.id AS relation_id,
                        td.identifier_id AS tender_document_identifier,
                        bbd.identifier_id AS business_bid_document_identifier,
                        tbd.identifier_id AS technical_bid_document_identifier
                    FROM xtjs_project_documents pd
                    JOIN xtjs_documents td ON pd.tender_document_id = td.id AND td.deleted = FALSE
                    JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.id AND bbd.deleted = FALSE
                    LEFT JOIN xtjs_documents tbd
                        ON pd.technical_bid_document_id = tbd.id AND tbd.deleted = FALSE
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
                    and existing_technical_identifier != normalized_technical_identifier
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
                    normalized_business_identifier,
                    normalized_technical_identifier,
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
            JOIN xtjs_documents td ON pd.tender_document_id = td.id AND td.deleted = FALSE
            JOIN xtjs_documents bbd ON pd.business_bid_document_id = bbd.id AND bbd.deleted = FALSE
            LEFT JOIN xtjs_documents tbd
                ON pd.technical_bid_document_id = tbd.id AND tbd.deleted = FALSE
            WHERE pd.project_id = %s
            ORDER BY pd.create_time DESC
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["id"],))
                relations: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return {"project": project, "relations": relations}

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
                bbd.id AS document_id,
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
              ON pd.tender_document_id = td.id
             AND td.deleted = FALSE
            JOIN xtjs_documents bbd
              ON pd.business_bid_document_id = bbd.id
             AND bbd.deleted = FALSE
            WHERE pd.project_id = %s

            UNION ALL

            SELECT
                pd.id AS relation_id,
                'technical_bid' AS relation_role,
                tbd.id AS document_id,
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
              ON pd.tender_document_id = td.id
             AND td.deleted = FALSE
            JOIN xtjs_documents tbd
              ON pd.technical_bid_document_id = tbd.id
             AND tbd.deleted = FALSE
            WHERE pd.project_id = %s

            ORDER BY create_time DESC, relation_id DESC, document_id DESC
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["id"], project["id"]))
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
                create_time,
                update_time
            FROM xtjs_result
            WHERE project_identifier_id = %s
            LIMIT 1
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project_identifier_id,))
                result = cursor.fetchone()
                return dict(result) if result else None

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
            conditions.append("r.project_identifier_id ILIKE %s")
            values.append(keyword_like)
        where_clause = " AND ".join(conditions)

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM xtjs_result r
            JOIN xtjs_projects p ON r.project_identifier_id = p.identifier_id
            WHERE {where_clause}
        """
        data_query = f"""
            SELECT
                r.id,
                r.project_identifier_id,
                r.result,
                r.create_time,
                r.update_time
            FROM xtjs_result r
            JOIN xtjs_projects p ON r.project_identifier_id = p.identifier_id
            WHERE {where_clause}
            ORDER BY r.update_time DESC, r.id DESC
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

    def create_or_replace_project_result(
        self,
        project_identifier_id: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建或完全覆盖项目的分析结果。"""
        normalized_project_identifier = self._normalize_required_identifier(
            project_identifier_id,
            "project_identifier_id",
        )
        if not isinstance(result, dict):
            raise ValueError("result must be a JSON object")
        project = self.get_project_by_identifier(normalized_project_identifier)
        if not project:
            raise ValueError(f"项目不存在：{normalized_project_identifier}")

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
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized_project_identifier, Json(jsonable_encoder(result))))
                return dict(cursor.fetchone())

    def delete_project_result(self, project_identifier_id: str) -> bool:
        """删除项目分析结果记录。"""
        normalized_project_identifier = self._normalize_required_identifier(
            project_identifier_id,
            "project_identifier_id",
        )
        query = """
            DELETE FROM xtjs_result
            WHERE project_identifier_id = %s
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
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
            WHERE project_identifier_id = ANY(%s)
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (normalized_ids,))
                return int(cursor.rowcount or 0)

    def upsert_project_result_item(
        self,
        project_identifier_id: str,
        result_key: str,
        result_value: Dict[str, Any],
    ) -> Dict[str, Any]:
        """向项目结果中合并一个键值对（保留已有键）。"""
        normalized_project_identifier = self._normalize_required_identifier(
            project_identifier_id,
            "project_identifier_id",
        )
        normalized_result_key = self._normalize_required_identifier(result_key, "result_key")
        if not isinstance(result_value, dict):
            raise ValueError("result_value must be a JSON object")

        project = self.get_project_by_identifier(normalized_project_identifier)
        if not project:
            raise ValueError(f"项目不存在：{normalized_project_identifier}")

        payload = jsonable_encoder({normalized_result_key: result_value})
        query = """
            INSERT INTO xtjs_result (project_identifier_id, result)
            VALUES (%s, %s)
            ON CONFLICT (project_identifier_id)
            DO UPDATE
            SET
                result = COALESCE(xtjs_result.result, '{}'::jsonb) || EXCLUDED.result,
                update_time = CURRENT_TIMESTAMP
            RETURNING
                id,
                project_identifier_id,
                result,
                create_time,
                update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        normalized_project_identifier,
                        Json(payload),
                    ),
                )
                return dict(cursor.fetchone())
