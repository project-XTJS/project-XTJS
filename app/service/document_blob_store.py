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
（短 TTL，可重建）。结果（result）键按项目维度放在 `<项目>/JSON识别/result.json.gz`，
便于浏览。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from app.service.minio_service import MinioService

logger = logging.getLogger(__name__)

# content 派生 JSON 的对象键前缀（可重建，受生命周期短 TTL 管理）
CONTENT_OBJECT_PREFIX = "JSON识别/content"
# review_content（人工复核工作副本）的对象键前缀
REVIEW_OBJECT_PREFIX = "JSON识别/review"

_minio_singleton: Optional[MinioService] = None


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
    key = build_content_object_key(identifier_id, file_name)
    _client().put_json_gz(key, content)
    return key


def read_blob(object_key: Any) -> Any:
    """按对象键直接从 MinIO 取回 JSON；缺键/缺对象/失败均返回 None。"""
    key = _non_empty_str(object_key)
    if not key:
        return None
    try:
        return _client().get_json_gz(key)
    except Exception:  # noqa: BLE001
        logger.warning("read_blob 失败 key=%s", key, exc_info=True)
        return None


def get_document_content(document: Optional[Dict[str, Any]]) -> Any:
    """返回文档识别内容：优先对象键 → MinIO；缺键/缺对象回退 DB `content`。"""
    if not isinstance(document, dict):
        return None
    key = _non_empty_str(document.get("content_object_key"))
    if key:
        try:
            blob = _client().get_json_gz(key)
        except Exception:  # noqa: BLE001 - 读对象失败回退 DB，保证可用性
            logger.warning("读取 content 对象失败，回退数据库内联 JSON key=%s", key, exc_info=True)
            blob = None
        if blob is not None:
            return blob
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
            try:
                blob = _client().get_json_gz(key)
            except Exception:  # noqa: BLE001
                logger.warning("hydrate content 失败 key=%s", key, exc_info=True)
                blob = None
            if blob is not None:
                document["content"] = blob
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
    key = build_review_content_object_key(identifier_id, file_name)
    _client().put_json_gz(key, review_content)
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
    _client().put_json_gz(key, result)
    return key


def get_result_payload(record: Optional[Dict[str, Any]]) -> Any:
    """从结果记录解析完整 result：优先 `result_object_key` → MinIO；回退 DB `result`。"""
    if not isinstance(record, dict):
        return None
    key = _non_empty_str(record.get("result_object_key"))
    if key:
        try:
            blob = _client().get_json_gz(key)
        except Exception:  # noqa: BLE001
            logger.warning("读取 result 对象失败，回退数据库内联 JSON key=%s", key, exc_info=True)
            blob = None
        if blob is not None:
            return blob
    return record.get("result")


def hydrate_result_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """就地补全 `record['result']`：内联 result 缺失但有对象键时取 MinIO。返回同一 dict。"""
    if not isinstance(record, dict):
        return record
    if not _is_present_json(record.get("result")):
        key = _non_empty_str(record.get("result_object_key"))
        if key:
            try:
                blob = _client().get_json_gz(key)
            except Exception:  # noqa: BLE001
                logger.warning("hydrate result 失败 key=%s", key, exc_info=True)
                blob = None
            if blob is not None:
                record["result"] = blob
    return record
