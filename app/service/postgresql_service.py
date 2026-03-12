from typing import Any, Dict, List, Optional
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config.postgresql import PostgresConfig


class PostgreSQLService:
    """PostgreSQL 业务服务：封装项目/文档 CRUD 与关联操作。"""

    def _connect(self):
        """创建数据库连接。"""
        return psycopg2.connect(PostgresConfig.DATABASE_URL)

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
            SELECT id, identifier_id
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
            ORDER BY id ASC
            LIMIT 1
            """,
            (identifier_id,),
        )
        document = cursor.fetchone()
        return dict(document) if document else None

    def create_project(self, identifier_id: Optional[str] = None) -> Dict[str, Any]:
        """创建项目记录。"""
        identifier = self._normalize_identifier(identifier_id)
        query = """
            INSERT INTO xtjs_projects (identifier_id)
            VALUES (%s)
            RETURNING id, identifier_id, deleted, create_time, update_time
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier,))
                return dict(cursor.fetchone())

    def list_projects(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """分页查询项目列表（仅未删除）。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        with self._connect() as conn:
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

    def get_project_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """按业务标识查询未删除项目。"""
        query = """
            SELECT id, identifier_id, deleted, create_time, update_time
            FROM xtjs_projects
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier_id,))
                result = cursor.fetchone()
                return dict(result) if result else None

    def update_project_identifier(
        self, identifier_id: str, new_identifier_id: str
    ) -> Optional[Dict[str, Any]]:
        """更新项目业务标识。"""
        normalized_new_identifier = self._normalize_required_identifier(
            new_identifier_id, "new_identifier_id"
        )
        query = """
            UPDATE xtjs_projects
            SET identifier_id = %s, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING id, identifier_id, deleted, create_time, update_time
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized_new_identifier, identifier_id))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def soft_delete_project(self, identifier_id: str) -> bool:
        """逻辑删除项目。"""
        query = """
            UPDATE xtjs_projects
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (identifier_id,))
                return cursor.rowcount > 0

    def create_document(
        self, file_name: str, file_url: str, identifier_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """创建文档记录。"""
        identifier = self._normalize_identifier(identifier_id)
        normalized_file_name = self._normalize_file_value(file_name, "file_name")
        normalized_file_url = self._normalize_file_value(file_url, "file_url")

        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                existing = self._get_document_record(cursor, identifier)
                if existing:
                    raise ValueError(f"Document identifier already exists: {identifier}")
                cursor.execute(
                    """
                    INSERT INTO xtjs_documents (identifier_id, file_name, file_url)
                    VALUES (%s, %s, %s)
                    RETURNING id, identifier_id, file_name, file_url, deleted, create_time, update_time
                    """,
                    (identifier, normalized_file_name, normalized_file_url),
                )
                return dict(cursor.fetchone())

    def list_documents(self, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """分页查询文档列表（仅未删除）。"""
        normalized_limit = max(1, min(limit, 200))
        normalized_offset = max(0, offset)
        with self._connect() as conn:
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
                    SELECT id, identifier_id, file_name, file_url, deleted, create_time, update_time
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
        """按业务标识查询未删除文档。"""
        query = """
            SELECT id, identifier_id, file_name, file_url, deleted, create_time, update_time
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
            ORDER BY id ASC
            LIMIT 1
        """
        with self._connect() as conn:
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
        """更新文档元数据。"""
        updates: List[str] = []
        values: List[Any] = []

        if file_name is not None:
            updates.append("file_name = %s")
            values.append(self._normalize_file_value(file_name, "file_name"))
        if file_url is not None:
            updates.append("file_url = %s")
            values.append(self._normalize_file_value(file_url, "file_url"))
        if not updates:
            raise ValueError("At least one field of file_name/file_url is required")

        updates.append("update_time = CURRENT_TIMESTAMP")
        values.append(identifier_id)
        query = f"""
            UPDATE xtjs_documents
            SET {", ".join(updates)}
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING id, identifier_id, file_name, file_url, deleted, create_time, update_time
        """

        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(values))
                updated = cursor.fetchone()
                return dict(updated) if updated else None

    def soft_delete_document(self, identifier_id: str) -> bool:
        """逻辑删除文档记录。"""
        query = """
            UPDATE xtjs_documents
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (identifier_id,))
                return cursor.rowcount > 0

    def bind_project_documents(
        self,
        project_identifier: str,
        tender_document_identifier: str,
        bid_document_identifier: str,
    ) -> Dict[str, Any]:
        """创建项目与招标/投标文档之间的关联记录。"""
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                project = self._get_project_record(cursor, project_identifier)
                if not project:
                    raise ValueError(f"Project not found: {project_identifier}")

                tender = self._get_document_record(cursor, tender_document_identifier)
                if not tender:
                    raise ValueError(
                        f"Tender document not found: {tender_document_identifier}"
                    )

                bid = self._get_document_record(cursor, bid_document_identifier)
                if not bid:
                    raise ValueError(f"Bid document not found: {bid_document_identifier}")

                cursor.execute(
                    """
                    SELECT id
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND bid_document_id = %s
                    LIMIT 1
                    """,
                    (project["id"], tender["id"], bid["id"]),
                )
                duplicated = cursor.fetchone()
                if duplicated:
                    raise ValueError(
                        "Project-document relation already exists for this document pair"
                    )

                cursor.execute(
                    """
                    INSERT INTO xtjs_project_documents (project_id, tender_document_id, bid_document_id)
                    VALUES (%s, %s, %s)
                    RETURNING id, project_id, tender_document_id, bid_document_id, create_time
                    """,
                    (project["id"], tender["id"], bid["id"]),
                )
                binding = dict(cursor.fetchone())
                return {
                    **binding,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender_document_identifier,
                    "bid_document_identifier": bid_document_identifier,
                }

    def get_relation_by_id(self, relation_id: int) -> Optional[Dict[str, Any]]:
        """按关联 ID 查询项目文档关联详情。"""
        query = """
            SELECT
                pd.id AS relation_id,
                p.identifier_id AS project_identifier,
                td.identifier_id AS tender_identifier_id,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                bd.identifier_id AS bid_identifier_id,
                bd.file_name AS bid_file_name,
                bd.file_url AS bid_file_url,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_projects p ON pd.project_id = p.id AND p.deleted = FALSE
            JOIN xtjs_documents td ON pd.tender_document_id = td.id AND td.deleted = FALSE
            JOIN xtjs_documents bd ON pd.bid_document_id = bd.id AND bd.deleted = FALSE
            WHERE pd.id = %s
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (relation_id,))
                relation = cursor.fetchone()
                return dict(relation) if relation else None

    def update_relation(
        self,
        relation_id: int,
        tender_document_identifier: str,
        bid_document_identifier: str,
    ) -> Optional[Dict[str, Any]]:
        """更新项目文档关联（替换招标/投标文档）。"""
        with self._connect() as conn:
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

                tender = self._get_document_record(cursor, tender_document_identifier)
                if not tender:
                    raise ValueError(
                        f"Tender document not found: {tender_document_identifier}"
                    )

                bid = self._get_document_record(cursor, bid_document_identifier)
                if not bid:
                    raise ValueError(f"Bid document not found: {bid_document_identifier}")

                cursor.execute(
                    """
                    SELECT id
                    FROM xtjs_project_documents
                    WHERE project_id = %s
                      AND tender_document_id = %s
                      AND bid_document_id = %s
                      AND id <> %s
                    LIMIT 1
                    """,
                    (relation["project_id"], tender["id"], bid["id"], relation_id),
                )
                duplicated = cursor.fetchone()
                if duplicated:
                    raise ValueError(
                        "Project-document relation already exists for this document pair"
                    )

                cursor.execute(
                    """
                    UPDATE xtjs_project_documents
                    SET tender_document_id = %s, bid_document_id = %s
                    WHERE id = %s
                    RETURNING id, project_id, tender_document_id, bid_document_id, create_time
                    """,
                    (tender["id"], bid["id"], relation_id),
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
                    "bid_document_identifier": bid_document_identifier,
                }

    def delete_relation(self, relation_id: int) -> bool:
        """删除项目文档关联。"""
        with self._connect() as conn:
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
        """查询项目详情及其关联文档列表。"""
        project = self.get_project_by_identifier(identifier_id)
        if not project:
            return None

        query = """
            SELECT
                pd.id AS relation_id,
                td.identifier_id AS tender_identifier_id,
                td.file_name AS tender_file_name,
                td.file_url AS tender_file_url,
                bd.identifier_id AS bid_identifier_id,
                bd.file_name AS bid_file_name,
                bd.file_url AS bid_file_url,
                pd.create_time
            FROM xtjs_project_documents pd
            JOIN xtjs_documents td ON pd.tender_document_id = td.id AND td.deleted = FALSE
            JOIN xtjs_documents bd ON pd.bid_document_id = bd.id AND bd.deleted = FALSE
            WHERE pd.project_id = %s
            ORDER BY pd.create_time DESC
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["id"],))
                relations: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return {"project": project, "relations": relations}
