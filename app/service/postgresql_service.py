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

_db_pool = None


def get_db_pool():
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
    ACTIVE_DOCUMENT_TYPES = set(ACTIVE_DOCUMENT_TYPES)
    SUPPORTED_DOCUMENT_TYPES = set(SUPPORTED_DOCUMENT_TYPES)

    @contextmanager
    def _get_connection(self):
        pool = get_db_pool()
        conn = pool.getconn()
        try:
            with conn:
                yield conn
        finally:
            pool.putconn(conn)

    @staticmethod
    def _normalize_identifier(identifier_id: Optional[str]) -> str:
        identifier = (identifier_id or "").strip()
        return identifier or str(uuid4())

    @staticmethod
    def _normalize_required_identifier(identifier_id: str, field_name: str) -> str:
        normalized = (identifier_id or "").strip()
        if not normalized:
            raise ValueError(f"{field_name} cannot be empty")
        return normalized

    @staticmethod
    def _normalize_file_value(value: Optional[str], field_name: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError(f"{field_name} cannot be empty")
        return normalized

    @classmethod
    def _normalize_document_type(cls, document_type: str) -> str:
        normalized = (document_type or "").strip().lower()
        if normalized not in cls.ACTIVE_DOCUMENT_TYPES:
            allowed = ", ".join(sorted(cls.ACTIVE_DOCUMENT_TYPES))
            raise ValueError(f"document_type 必须是以下之一：{allowed}")
        return normalized

    def _get_project_record(self, cursor, identifier_id: str) -> Optional[Dict[str, Any]]:
        cursor.execute(
            """
            SELECT id, identifier_id
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
            """,
            (identifier_id,),
        )
        project = cursor.fetchone()
        return dict(project) if project else None

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

    def create_project(self, identifier_id: Optional[str] = None) -> Dict[str, Any]:
        identifier = self._normalize_identifier(identifier_id)
        query = """
            INSERT INTO xtjs_projects (identifier_id)
            VALUES (%s)
            RETURNING id, identifier_id, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier,))
                return dict(cursor.fetchone())

    def list_projects(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM xtjs_projects
                    WHERE deleted = FALSE
                    """
                )
                total = int(cursor.fetchone()["total"])
                cursor.execute(
                    """
                    SELECT id, identifier_id, deleted, create_time, update_time
                    FROM xtjs_projects
                    WHERE deleted = FALSE
                    ORDER BY create_time DESC, id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (normalized_limit, normalized_offset),
                )
                items: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return {
            "total": total,
            "limit": normalized_limit,
            "offset": normalized_offset,
            "items": items,
        }

    def list_project_identifiers(self) -> List[str]:
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
        query = """
            SELECT id, identifier_id, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier_id,))
                result = cursor.fetchone()
                return dict(result) if result else None

    def update_project_identifier(
        self,
        identifier_id: str,
        new_identifier_id: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_new_identifier = self._normalize_required_identifier(
            new_identifier_id,
            "new_identifier_id",
        )
        query = """
            UPDATE xtjs_projects
            SET identifier_id = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING id, identifier_id, deleted, create_time, update_time
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized_new_identifier, identifier_id))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def soft_delete_project(self, identifier_id: str) -> bool:
        query = """
            UPDATE xtjs_projects
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (identifier_id,))
                return cursor.rowcount > 0

    def create_document(
        self,
        file_name: str,
        file_url: str,
        document_type: str,
        identifier_id: Optional[str] = None,
    ) -> Dict[str, Any]:
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

    def list_documents(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM xtjs_documents
                    WHERE deleted = FALSE
                    """
                )
                total = int(cursor.fetchone()["total"])
                cursor.execute(
                    """
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
                    WHERE deleted = FALSE
                    ORDER BY create_time DESC, id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (normalized_limit, normalized_offset),
                )
                items: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return {
            "total": total,
            "limit": normalized_limit,
            "offset": normalized_offset,
            "items": items,
        }

    def get_document_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
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

    def soft_delete_document(self, identifier_id: str) -> bool:
        query = """
            UPDATE xtjs_documents
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (identifier_id,))
                return cursor.rowcount > 0

    def bind_project_documents(
        self,
        project_identifier: str,
        tender_document_identifier: str,
        business_bid_document_identifier: str,
        technical_bid_document_identifier: str,
    ) -> Dict[str, Any]:
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
                technical_bid = self._get_required_document_record(
                    cursor,
                    technical_bid_document_identifier,
                    role_label="技术标文件",
                    allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                )

                cursor.execute(
                    """
                    SELECT id
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND business_bid_document_id = %s
                      AND technical_bid_document_id = %s
                    LIMIT 1
                    """,
                    (
                        project["id"],
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"],
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
                        technical_bid["id"],
                    ),
                )
                binding = dict(cursor.fetchone())
                return {
                    **binding,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender_document_identifier,
                    "business_bid_document_identifier": business_bid_document_identifier,
                    "technical_bid_document_identifier": technical_bid_document_identifier,
                }

    def get_relation_by_id(self, relation_id: int) -> Optional[Dict[str, Any]]:
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

    def update_relation(
        self,
        relation_id: int,
        tender_document_identifier: str,
        business_bid_document_identifier: str,
        technical_bid_document_identifier: str,
    ) -> Optional[Dict[str, Any]]:
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
                technical_bid = self._get_required_document_record(
                    cursor,
                    technical_bid_document_identifier,
                    role_label="技术标文件",
                    allowed_types=set(TECHNICAL_BID_COMPATIBLE_TYPES),
                )

                cursor.execute(
                    """
                    SELECT id
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND business_bid_document_id = %s
                      AND technical_bid_document_id = %s
                      AND id <> %s
                    LIMIT 1
                    """,
                    (
                        relation["project_id"],
                        tender["id"],
                        business_bid["id"],
                        technical_bid["id"],
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
                        technical_bid["id"],
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
                    "technical_bid_document_identifier": technical_bid_document_identifier,
                }

    def delete_relation(self, relation_id: int) -> bool:
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

    def get_project_detail(self, identifier_id: str) -> Optional[Dict[str, Any]]:
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

    def get_project_result(self, project_identifier_id: str) -> Optional[Dict[str, Any]]:
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

    def upsert_project_result_item(
        self,
        project_identifier_id: str,
        result_key: str,
        result_value: Dict[str, Any],
    ) -> Dict[str, Any]:
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
