"""Per-request resource authorization, shared by API and asynchronous workers."""
from contextvars import ContextVar
from contextlib import nullcontext
from functools import wraps
from inspect import signature
from fastapi import HTTPException

actor_context = ContextVar("xtjs_actor", default=None)


def actor_id():
    return str((actor_context.get() or {}).get("identifier_id") or "")


def restricted():
    return int((actor_context.get() or {}).get("role_level") or 0) == 2


def cache_scope():
    actor = actor_context.get() or {}
    return f"{actor.get('identifier_id', 'internal')}:{actor.get('role_level', 0)}"


def scope_sql(kind, alias):
    if not restricted():
        return "TRUE"
    function = {"project": "xtjs_can_access_project", "document": "xtjs_can_access_document"}[kind]
    return f"{function}({alias}.identifier_id, nullif(current_setting('xtjs.actor_id', true), '')::uuid)"


def access_check(**resources):
    """Check every target before executing a method, including all batch members."""
    def decorate(func):
        sig = signature(func)
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            if restricted():
                bound = sig.bind(self, *args, **kwargs)
                for parameter, kind in resources.items():
                    value = bound.arguments.get(parameter)
                    values = value if isinstance(value, (list, tuple, set)) else [value]
                    for target in values:
                        if target is not None and str(target).strip():
                            self.assert_resource_access(kind, target)
            return func(self, *args, **kwargs)
        return wrapped
    return decorate


class ResourceAccessMixin:
    def assert_resource_access(self, kind, identifier, *, cursor=None):
        if not restricted():
            return
        with (self._get_connection() if cursor is None else nullcontext(None)) as conn:
            with (conn.cursor() if cursor is None else nullcontext(cursor)) as active_cursor:
                return self._check_resource_cursor(active_cursor, kind, identifier)

    def _check_resource_cursor(self, cursor, kind, identifier):
        value = self._extract_identifier(str(identifier))
        if kind == "project":
            cursor.execute("SELECT 1 FROM xtjs_projects p WHERE NOT p.deleted AND (p.identifier_id::text=%s OR p.project_name=%s) AND " + scope_sql("project", "p"), (value, value))
        elif kind in ("document", "document_write"):
            cursor.execute("SELECT d.identifier_id FROM xtjs_documents d WHERE NOT d.deleted AND (d.identifier_id::text=%s OR d.file_name=%s) AND " + scope_sql("document", "d"), (value, value))
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise HTTPException(403, "无权访问该文件，或名称不唯一，请使用文件标识")
            document_id = str(rows[0][0] if not isinstance(rows[0], dict) else rows[0]["identifier_id"])
            if kind == "document_write":
                cursor.execute("""SELECT 1 FROM xtjs_project_documents pd JOIN xtjs_projects p ON p.identifier_id=pd.project_id
                    WHERE NOT p.deleted AND %s::uuid IN (pd.tender_document_id,pd.business_bid_document_id,pd.technical_bid_document_id)
                    AND NOT xtjs_can_access_project(p.identifier_id,%s::uuid) LIMIT 1""", (document_id, actor_id()))
                if cursor.fetchone():
                    raise HTTPException(403, "文件还被无权操作的项目引用，请使用项目内替换文件")
            return
        elif kind == "relation":
            cursor.execute("SELECT 1 FROM xtjs_project_documents pd JOIN xtjs_projects p ON p.identifier_id=pd.project_id WHERE pd.id=%s AND NOT p.deleted AND " + scope_sql("project", "p"), (identifier,))
        elif kind == "review":
            cursor.execute("SELECT 1 FROM xtjs_tender_reviews r JOIN xtjs_documents d ON d.identifier_id=r.document_identifier_id WHERE r.identifier_id::text=%s AND NOT r.deleted AND NOT d.deleted AND " + scope_sql("document", "d"), (value,))
        else:
            raise RuntimeError(f"Unknown access resource: {kind}")
        if not cursor.fetchone():
            raise HTTPException(403, "无权访问该资源，或资源不存在")
