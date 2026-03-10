from typing import Any, Dict, List, Optional
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config.postgresql import PostgresConfig


class PostgreSQLService:
    """PostgreSQL 业务服务：封装项目/文档的 CRUD 与关联操作。"""

    def _connect(self):
        """创建数据库连接。"""
        return psycopg2.connect(PostgresConfig.DATABASE_URL)

    def create_project(self, identifier_id: Optional[str] = None) -> Dict[str, Any]:
        """创建项目记录。"""
        identifier = identifier_id or str(uuid4())
        query = """
            INSERT INTO xtjs_projects (identifier_id)
            VALUES (%s)
            RETURNING id, identifier_id, deleted, create_time, update_time
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier,))
                return dict(cursor.fetchone())

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

    def create_document(
        self, file_name: str, file_url: str, identifier_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """创建文档记录。"""
        identifier = identifier_id or str(uuid4())
        query = """
            INSERT INTO xtjs_documents (identifier_id, file_name, file_url)
            VALUES (%s, %s, %s)
            RETURNING id, identifier_id, file_name, file_url, deleted, create_time, update_time
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier, file_name, file_url))
                return dict(cursor.fetchone())

    def get_document_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """按业务标识查询未删除文档。"""
        query = """
            SELECT id, identifier_id, file_name, file_url, deleted, create_time, update_time
            FROM xtjs_documents
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (identifier_id,))
                result = cursor.fetchone()
                return dict(result) if result else None

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
                # 先校验项目存在。
                cursor.execute(
                    """
                    SELECT id, identifier_id
                    FROM xtjs_projects
                    WHERE identifier_id = %s AND deleted = FALSE
                    """,
                    (project_identifier,),
                )
                project = cursor.fetchone()
                if not project:
                    raise ValueError(f"Project not found: {project_identifier}")

                # 校验招标文档存在。
                cursor.execute(
                    """
                    SELECT id, identifier_id
                    FROM xtjs_documents
                    WHERE identifier_id = %s AND deleted = FALSE
                    """,
                    (tender_document_identifier,),
                )
                tender = cursor.fetchone()
                if not tender:
                    raise ValueError(
                        f"Tender document not found: {tender_document_identifier}"
                    )

                # 校验投标文档存在。
                cursor.execute(
                    """
                    SELECT id, identifier_id
                    FROM xtjs_documents
                    WHERE identifier_id = %s AND deleted = FALSE
                    """,
                    (bid_document_identifier,),
                )
                bid = cursor.fetchone()
                if not bid:
                    raise ValueError(f"Bid document not found: {bid_document_identifier}")

                # 三方均存在后写入关联表。
                query = """
                    INSERT INTO xtjs_project_documents (project_id, tender_document_id, bid_document_id)
                    VALUES (%s, %s, %s)
                    RETURNING id, project_id, tender_document_id, bid_document_id, create_time
                """
                cursor.execute(query, (project["id"], tender["id"], bid["id"]))
                binding = dict(cursor.fetchone())
                return {
                    **binding,
                    "project_identifier": project_identifier,
                    "tender_document_identifier": tender_document_identifier,
                    "bid_document_identifier": bid_document_identifier,
                }

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
            JOIN xtjs_documents td ON pd.tender_document_id = td.id
            JOIN xtjs_documents bd ON pd.bid_document_id = bd.id
            WHERE pd.project_id = %s
            ORDER BY pd.create_time DESC
        """
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (project["id"],))
                relations: List[Dict[str, Any]] = [dict(item) for item in cursor.fetchall()]
        return {"project": project, "relations": relations}
