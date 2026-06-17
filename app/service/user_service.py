# -*- coding: utf-8 -*-
"""
用户数据访问服务模块。

封装 xtjs_users 表的 CRUD、登录失败锁定状态维护等操作，
复用 PostgreSQL 连接池（与 PostgreSQLService 同源）。
"""

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from psycopg2 import errors as pg_errors
from psycopg2.extras import RealDictCursor

from app.config.settings import settings
from app.core.security import hash_password
from app.service.postgresql_service import get_db_pool

logger = logging.getLogger(__name__)

# 角色等级与中文标签
ROLE_LEVEL_LABELS = {
    1: "普通用户",
    2: "中级用户",
    3: "高级用户",
    4: "管理员",
}
ROLE_LEVEL_ADMIN = 4

_USER_PUBLIC_COLUMNS = (
    "identifier_id, username, role_level, display_name, is_active, "
    "last_login_at, create_time, update_time"
)


class UsernameAlreadyExistsError(Exception):
    """用户名已存在（未删除范围内唯一）。"""


class UserService:
    """xtjs_users 数据库服务层。"""

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

    @staticmethod
    def role_label(role_level: int) -> str:
        """返回角色等级对应的中文标签。"""
        return ROLE_LEVEL_LABELS.get(int(role_level), "未知")

    @staticmethod
    def _public_view(record: Dict[str, Any]) -> Dict[str, Any]:
        """把数据库记录转换为对外可暴露的用户视图（不含口令哈希）。"""
        return {
            "identifier_id": str(record["identifier_id"]),
            "username": record["username"],
            "role_level": int(record["role_level"]),
            "role_label": UserService.role_label(record["role_level"]),
            "display_name": record.get("display_name"),
            "is_active": bool(record["is_active"]),
            "last_login_at": record.get("last_login_at"),
            "create_time": record.get("create_time"),
            "update_time": record.get("update_time"),
        }

    # —— 查询 ——

    def get_auth_record_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """按用户名获取含口令哈希与锁定状态的完整记录（仅供登录校验内部使用）。"""
        normalized = (username or "").strip()
        if not normalized:
            return None
        query = """
            SELECT identifier_id, username, hashed_password, role_level, display_name,
                   is_active, failed_attempts, locked_until, last_login_at
            FROM xtjs_users
            WHERE username = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (normalized,))
                row = cursor.fetchone()
                return dict(row) if row else None

    def get_public_by_identifier(self, identifier_id: str) -> Optional[Dict[str, Any]]:
        """按 identifier_id 获取对外用户视图（用于令牌解析后加载当前用户）。"""
        query = f"""
            SELECT {_USER_PUBLIC_COLUMNS}
            FROM xtjs_users
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (str(identifier_id),))
                row = cursor.fetchone()
                return self._public_view(dict(row)) if row else None

    def list_users(self) -> List[Dict[str, Any]]:
        """列出所有未删除用户（对外视图）。"""
        query = f"""
            SELECT {_USER_PUBLIC_COLUMNS}
            FROM xtjs_users
            WHERE deleted = FALSE
            ORDER BY role_level DESC, create_time DESC, identifier_id DESC
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return [self._public_view(dict(row)) for row in cursor.fetchall()]

    def count_admins(self) -> int:
        """统计未删除且启用的管理员数量（用于保护最后一个管理员）。"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM xtjs_users
                    WHERE deleted = FALSE AND is_active = TRUE AND role_level = %s
                    """,
                    (ROLE_LEVEL_ADMIN,),
                )
                return int(cursor.fetchone()[0])

    # —— 写入 ——

    def create_user(
        self,
        username: str,
        password: str,
        role_level: int = 1,
        display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """创建用户，口令以 bcrypt 哈希存储。用户名重复时抛 UsernameAlreadyExistsError。"""
        normalized = (username or "").strip()
        query = f"""
            INSERT INTO xtjs_users (username, hashed_password, role_level, display_name)
            VALUES (%s, %s, %s, %s)
            RETURNING {_USER_PUBLIC_COLUMNS}
        """
        values = (
            normalized,
            hash_password(password),
            int(role_level),
            (display_name or "").strip() or None,
        )
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, values)
                    return self._public_view(dict(cursor.fetchone()))
        except pg_errors.UniqueViolation as exc:
            raise UsernameAlreadyExistsError(normalized) from exc

    def update_user(
        self,
        identifier_id: str,
        role_level: Optional[int] = None,
        is_active: Optional[bool] = None,
        display_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """更新角色等级 / 启停状态 / 展示名。返回更新后的对外视图，记录不存在返回 None。"""
        sets: List[str] = []
        values: List[Any] = []
        if role_level is not None:
            sets.append("role_level = %s")
            values.append(int(role_level))
        if is_active is not None:
            sets.append("is_active = %s")
            values.append(bool(is_active))
        if display_name is not None:
            sets.append("display_name = %s")
            values.append((display_name or "").strip() or None)
        if not sets:
            return self.get_public_by_identifier(identifier_id)
        sets.append("update_time = CURRENT_TIMESTAMP")
        query = f"""
            UPDATE xtjs_users SET {', '.join(sets)}
            WHERE identifier_id = %s AND deleted = FALSE
            RETURNING {_USER_PUBLIC_COLUMNS}
        """
        values.append(str(identifier_id))
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(values))
                row = cursor.fetchone()
                return self._public_view(dict(row)) if row else None

    def reset_password(self, identifier_id: str, new_password: str) -> bool:
        """重置指定用户口令，并解除其登录失败锁定。"""
        query = """
            UPDATE xtjs_users
            SET hashed_password = %s, failed_attempts = 0, locked_until = NULL,
                update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (hash_password(new_password), str(identifier_id)))
                return cursor.rowcount > 0

    def delete_user(self, identifier_id: str) -> bool:
        """逻辑删除用户。"""
        query = """
            UPDATE xtjs_users
            SET deleted = TRUE, update_time = CURRENT_TIMESTAMP
            WHERE identifier_id = %s AND deleted = FALSE
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (str(identifier_id),))
                return cursor.rowcount > 0

    # —— 登录状态维护 ——

    def record_login_success(self, identifier_id: str) -> None:
        """登录成功：清零失败计数、解除锁定、记录登录时间。"""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE xtjs_users
                    SET failed_attempts = 0, locked_until = NULL,
                        last_login_at = CURRENT_TIMESTAMP, update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s
                    """,
                    (str(identifier_id),),
                )

    def record_login_failure(self, identifier_id: str) -> None:
        """
        登录失败：累加失败计数；达到阈值时锁定账号一段时间。
        """
        lock_until = datetime.now(timezone.utc) + timedelta(
            minutes=settings.AUTH_LOCK_MINUTES
        )
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE xtjs_users
                    SET failed_attempts = failed_attempts + 1,
                        locked_until = CASE
                            WHEN failed_attempts + 1 >= %s THEN %s
                            ELSE locked_until
                        END,
                        update_time = CURRENT_TIMESTAMP
                    WHERE identifier_id = %s
                    """,
                    (settings.AUTH_MAX_FAILED_ATTEMPTS, lock_until, str(identifier_id)),
                )

    # —— 初始化 ——

    def ensure_initial_admin(self) -> None:
        """若配置了初始管理员口令且该账号不存在，则创建一个管理员账号。"""
        password = (settings.AUTH_INITIAL_ADMIN_PASSWORD or "").strip()
        username = (settings.AUTH_INITIAL_ADMIN_USERNAME or "").strip()
        if not password or not username:
            return
        if self.get_auth_record_by_username(username) is not None:
            return
        try:
            self.create_user(
                username=username,
                password=password,
                role_level=ROLE_LEVEL_ADMIN,
                display_name="系统管理员",
            )
            logger.info("已自动创建初始管理员账号：%s", username)
        except UsernameAlreadyExistsError:
            # 并发场景下已被其它进程创建，忽略即可。
            pass
