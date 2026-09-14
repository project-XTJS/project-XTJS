# -*- coding: utf-8 -*-
"""文档识别内容 / 项目分析结果的大 JSON 对象存储访问器。

背景：`xtjs_documents.content`（OCR 识别 JSON）与 `xtjs_result.result`
（分析结果 JSON）原先直接存 PostgreSQL，随项目增长线性膨胀。本模块把这两类
大 JSON 外置到 MinIO：
- 写入：gzip 压缩存对象，数据库只保留对象键（`content_object_key` /
  `result_object_key`），JSONB 列置空，达到“DB 瘦身”。
- 读取：优先用对象键 → MinIO 取回；缺键时回退数据库内联 JSON，兼容尚未迁移
  的历史行。读取统一走本模块，调用方无需感知数据落在 DB 还是 MinIO。

派生数据（content）键放在 `JSON识别/content/` 前缀下，便于对象生命周期规则
（短 TTL，可重建）。结果每次写入新的 `<项目>/JSON识别/result.<版本>.json.gz`，
数据库提交后才切换对象键，避免保存失败破坏旧结果。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from uuid import uuid4

from app.service.minio_service import MinioService

logger = logging.getLogger(__name__)

# content 派生 JSON 的对象键前缀（可重建，受生命周期短 TTL 管理）
CONTENT_OBJECT_PREFIX = "JSON识别/content"
# review_content（人工复核工作副本）的对象键前缀
REVIEW_OBJECT_PREFIX = "JSON识别/review"
# 独立招标文件审查结果对象前缀
TENDER_REVIEW_OBJECT_PREFIX = "JSON识别/tender-review"

_minio_singleton: Optional[MinioService] = None


class BlobReadError(RuntimeError):
    """A referenced object could not be read; callers must not treat it as empty."""


from contextlib import contextmanager
from contextvars import ContextVar

_transaction_objects = ContextVar("xtjs_transaction_objects", default=None)

@contextmanager
def transaction_objects():
    keys = []
    token = _transaction_objects.set(keys)
    try:
        yield
    except BaseException:
        for key in keys:
            logger.warning("blob_pending_reconciliation key=%s; confirm references before cleanup", key)
        raise
    finally:
        _transaction_objects.reset(token)

def _put_new_object(key, value):
    keys = _transaction_objects.get()
    if keys is not None:
        keys.append(key)
    # Outside a SQL scope (independent tender review), retain a log for reference reconciliation.
    else:
        logger.info("blob_version_created key=%s; reference commit follows", key)
    _client().put_json_gz(key, value)


def _read_required_json(key: str):
    try:
        blob = _client().get_json_gz(key)
        if not isinstance(blob, dict):
            raise ValueError("Referenced JSON object is missing or invalid")
        return blob
    except Exception as exc:
        logger.warning("读取已保存对象失败 key=%s type=%s", key, type(exc).__name__)
        raise BlobReadError("已保存内容暂时无法读取，本次操作未继续，请稍后重试") from exc


def _client() -> MinioService:
    """惰性创建并复用 MinioService 单例。"""
    global _minio_singleton
    if _minio_singleton is None:
        _minio_singleton = MinioService()
    return _minio_singleton


def _non_empty_str(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _is_present_json(value: Any) -> bool:
    """判断数据库内联 JSON 是否“有内容”（非 None、非空 dict/list/str）。"""
    if value is None:
        return False
    if isinstance(value, (dict, list, str)):
        return bool(value)
    return True


# ----------------------------------------------------------------------------
# 对象键构建
# ----------------------------------------------------------------------------

def build_content_object_key(identifier_id: Any, file_name: Any = None) -> str:
    """构建文档识别内容的对象键：`JSON识别/content/<安全文件名>_<doc-id>.json.gz`。

    以文档 UUID 保证全局唯一、避免覆盖；附带安全化文件名仅为可读性。
    """
    doc_id = MinioService._safe_segment(identifier_id or "doc", maxlen=80)
    stem = _non_empty_str(file_name)
    if stem:
        stem = MinioService._safe_segment(stem, maxlen=80)
        leaf = f"{stem}_{doc_id}.json.gz"
    else:
        leaf = f"{doc_id}.json.gz"
    return f"{CONTENT_OBJECT_PREFIX}/{leaf}"


def build_review_content_object_key(identifier_id: Any, file_name: Any = None) -> str:
    """构建人工复核工作副本的对象键：`JSON识别/review/<安全文件名>_<doc-id>.json.gz`。"""
    doc_id = MinioService._safe_segment(identifier_id or "doc", maxlen=80)
    stem = _non_empty_str(file_name)
    if stem:
        stem = MinioService._safe_segment(stem, maxlen=80)
        leaf = f"{stem}_{doc_id}.json.gz"
    else:
        leaf = f"{doc_id}.json.gz"
    return f"{REVIEW_OBJECT_PREFIX}/{leaf}"


def build_result_object_key(project_name: Any, project_identifier_id: Any) -> str:
    """构建项目结果的对象键：`<项目名>/JSON识别/result.json.gz`。

    键持久化在 `result_object_key`，读取不依赖再次推导，因此项目名做安全化即可。
    项目名为空时退回用项目标识，保证非空。
    """
    name = _non_empty_str(project_name) or _non_empty_str(str(project_identifier_id or "")) or "project"
    return MinioService.build_project_object_key(name, role="result", kind="json")


def build_tender_review_result_object_key(review_identifier_id: Any, file_name: Any = None) -> str:
    """构建独立招标文件审查结果对象键。"""
    review_id = MinioService._safe_segment(review_identifier_id or "review", maxlen=80)
    stem = _non_empty_str(file_name)
    if stem:
        stem = MinioService._safe_segment(stem, maxlen=80)
        leaf = f"{stem}_{review_id}.json.gz"
    else:
        leaf = f"{review_id}.json.gz"
    return f"{TENDER_REVIEW_OBJECT_PREFIX}/{leaf}"


# ----------------------------------------------------------------------------
# content（文档识别内容）存取
# ----------------------------------------------------------------------------

def save_document_content(
    content: Dict[str, Any],
    *,
    identifier_id: Any,
    file_name: Any = None,
) -> str:
    """把文档识别内容写入 MinIO，返回对象键。失败抛异常由调用方处理。"""
    key = build_content_object_key(identifier_id, file_name).removesuffix(".json.gz") + f".{uuid4().hex}.json.gz"
    _put_new_object(key, content)
    return key


def read_blob(object_key: Any) -> Any:
    """缺键返回 None；已引用的对象无法读取时明确失败，避免误当空内容。"""
    key = _non_empty_str(object_key)
    if not key:
        return None
    return _read_required_json(key)


def get_document_content(document: Optional[Dict[str, Any]]) -> Any:
    """优先读取已引用的对象；仅没有对象键时兼容 DB 内联 `content`。"""
    if not isinstance(document, dict):
        return None
    key = _non_empty_str(document.get("content_object_key"))
    if key:
        return _read_required_json(key)
    return document.get("content")


def hydrate_document_content(document: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """就地补全 `document['content']`：当内联 content 缺失但有对象键时取 MinIO。

    返回同一个 dict，便于链式调用。调用方后续读 `document['content']` 即透明可用。
    """
    if not isinstance(document, dict):
        return document
    if not _is_present_json(document.get("content")):
        key = _non_empty_str(document.get("content_object_key"))
        if key:
            document["content"] = _read_required_json(key)
    return document


# ----------------------------------------------------------------------------
# review_content（人工复核工作副本）存取
# ----------------------------------------------------------------------------

def save_document_review_content(
    review_content: Dict[str, Any],
    *,
    identifier_id: Any,
    file_name: Any = None,
) -> str:
    """把人工复核工作副本写入 MinIO，返回对象键。"""
    key = build_review_content_object_key(identifier_id, file_name).removesuffix(".json.gz") + f".{uuid4().hex}.json.gz"
    _put_new_object(key, review_content)
    return key


def get_document_review_content_blob(document: Optional[Dict[str, Any]]) -> Any:
    """返回 review_content：优先对象键 → MinIO；缺键/缺对象回退 DB `review_content`。"""
    if not isinstance(document, dict):
        return None
    key = _non_empty_str(document.get("review_content_object_key"))
    if key:
        blob = read_blob(key)
        if blob is not None:
            return blob
    return document.get("review_content")


def hydrate_document_review_content(document: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """就地补全 `document['review_content']`：内联缺失但有对象键时取 MinIO。返回同一 dict。"""
    if not isinstance(document, dict):
        return document
    if not _is_present_json(document.get("review_content")):
        key = _non_empty_str(document.get("review_content_object_key"))
        if key:
            blob = read_blob(key)
            if blob is not None:
                document["review_content"] = blob
    return document


# ----------------------------------------------------------------------------
# result（项目分析结果）存取
# ----------------------------------------------------------------------------

def save_project_result(
    result: Dict[str, Any],
    *,
    project_name: Any,
    project_identifier_id: Any,
) -> str:
    """把项目分析结果写入 MinIO，返回对象键。"""
    key = build_result_object_key(project_name, project_identifier_id)
    # Immutable versions: SQL rollback must leave the previously referenced object intact.
    key = key.removesuffix(".json.gz") + f".{uuid4().hex}.json.gz"
    _put_new_object(key, result)
    return key


def get_result_payload(record: Optional[Dict[str, Any]]) -> Any:
    """从结果记录解析完整 result：优先 `result_object_key` → MinIO；回退 DB `result`。"""
    if not isinstance(record, dict):
        return None
    key = _non_empty_str(record.get("result_object_key"))
    if key:
        return _read_required_json(key)
    return record.get("result")


def hydrate_result_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """就地补全 `record['result']`：内联 result 缺失但有对象键时取 MinIO。返回同一 dict。"""
    if not isinstance(record, dict):
        return record
    if not _is_present_json(record.get("result")):
        key = _non_empty_str(record.get("result_object_key"))
        if key:
            record["result"] = _read_required_json(key)
    return record


# ----------------------------------------------------------------------------
# tender review（独立招标文件审查结果）存取
# ----------------------------------------------------------------------------

def save_tender_review_result(
    result: Dict[str, Any],
    *,
    review_identifier_id: Any,
    file_name: Any = None,
) -> str:
    """保存独立招标文件审查结果并返回对象键。"""
    key = build_tender_review_result_object_key(review_identifier_id, file_name).removesuffix(".json.gz") + f".{uuid4().hex}.json.gz"
    _put_new_object(key, result)
    return key


def get_tender_review_result(record: Optional[Dict[str, Any]]) -> Any:
    """读取独立招标文件审查结果。"""
    if not isinstance(record, dict):
        return None
    return read_blob(record.get("result_object_key"))
