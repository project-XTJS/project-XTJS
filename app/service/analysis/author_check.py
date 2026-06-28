# -*- coding: utf-8 -*-
"""作者查重预警：OCR 前检测不同投标公司的 PDF 是否由同一作者/创建人生成。

围标/串标的常见痕迹之一是不同公司的标书出自同一台电脑/同一人之手，PDF 元数据
（author/creator）会留下相同值。本模块在绑定完成、OCR 之前读取每份投标 PDF 的
元数据，若发现**不同公司**之间出现相同的非空 author/creator，则记为冲突，供前端
弹窗预警（默认 warn-only，不阻断后续 OCR）。

仅比对投标文件（商务标/技术标），不含招标文件（招标文件由招标方统一出具，作者相同
属正常）。每个项目文档关联（relation）视为一家投标公司。
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

import fitz  # PyMuPDF

from app.service.minio_service import MinioService

logger = logging.getLogger(__name__)

# 默认比对的元数据字段；producer 常被排版软件统一写入，误报高，默认不参与。
DEFAULT_META_FIELDS = ("author", "creator")


def _resolve_object_location(file_url: str) -> Optional[tuple[str, str]]:
    """把文档 file_url 解析为 (bucket, object)；不支持的形态返回 None。"""
    url = str(file_url or "").strip()
    if not url:
        return None
    try:
        if url.startswith("minio://"):
            return MinioService.bucket_and_object_from_file_url(url)
        if MinioService.is_presigned_url(url):
            return MinioService.bucket_and_object_from_presigned_url(url)
    except Exception:  # noqa: BLE001
        logger.warning("解析文档对象路径失败 file_url=%s", url, exc_info=True)
    return None


def _read_pdf_meta(oss_service: MinioService, file_url: str, fields: tuple) -> Dict[str, str]:
    """读取 PDF 元数据中的指定字段（小写归一、去空白）。失败返回空字典。"""
    location = _resolve_object_location(file_url)
    if not location:
        return {}
    bucket, object_name = location
    try:
        data, _ = oss_service.get_object_bytes(object_name, bucket)
    except Exception:  # noqa: BLE001
        logger.warning("下载 PDF 失败 object=%s", object_name, exc_info=True)
        return {}
    try:
        with fitz.open(stream=data, filetype="pdf") as doc:
            meta = doc.metadata or {}
    except Exception:  # noqa: BLE001
        logger.warning("读取 PDF 元数据失败 object=%s", object_name, exc_info=True)
        return {}
    out: Dict[str, str] = {}
    for field in fields:
        value = str(meta.get(field) or "").strip()
        if value:
            out[field] = value
    return out


def _company_label(relation: Dict[str, Any]) -> str:
    """以投标文件名作为公司展示标签（命名约定为“公司名+标书名”）。"""
    for key in ("business_bid_file_name", "technical_bid_file_name"):
        name = str(relation.get(key) or "").strip()
        if name:
            return name
    return f"投标方#{relation.get('relation_id')}"


def check_project_author_conflicts(
    db_service,
    oss_service: MinioService,
    project_identifier: str,
    *,
    fields: tuple = DEFAULT_META_FIELDS,
) -> Optional[Dict[str, Any]]:
    """检测项目内不同公司投标 PDF 的作者/创建人冲突。

    返回 {project_identifier, has_conflict, checked_documents, conflicts:[...]}；
    项目不存在返回 None。conflicts 每项为
    {field, value, companies:[{relation_id, company, documents:[{role, file_name}]}]}。
    """
    detail = db_service.get_project_detail(project_identifier)
    if not detail:
        return None
    project = detail.get("project") or {}
    relations = detail.get("relations") or []

    # 收集每份投标 PDF 的作者元数据
    documents: List[Dict[str, Any]] = []
    for relation in relations:
        relation_id = str(relation.get("relation_id"))
        company = _company_label(relation)
        for role_prefix, role_label in (
            ("business_bid", "商务标"),
            ("technical_bid", "技术标"),
        ):
            file_url = relation.get(f"{role_prefix}_file_url")
            file_name = relation.get(f"{role_prefix}_file_name")
            if not file_url:
                continue
            meta = _read_pdf_meta(oss_service, file_url, fields)
            documents.append({
                "relation_id": relation_id,
                "company": company,
                "role": role_label,
                "file_name": file_name,
                "meta": meta,
            })

    # 按字段 → 值 → 公司 分组，找跨公司同值
    conflicts: List[Dict[str, Any]] = []
    for field in fields:
        value_to_companies: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for doc in documents:
            value = doc["meta"].get(field)
            if not value:
                continue
            value_to_companies[value][doc["relation_id"]].append(doc)
        for value, by_company in value_to_companies.items():
            if len(by_company) < 2:
                continue  # 同一公司内部相同不算冲突
            companies_payload = []
            for relation_id, docs in by_company.items():
                companies_payload.append({
                    "relation_id": relation_id,
                    "company": docs[0]["company"],
                    "documents": [{"role": d["role"], "file_name": d["file_name"]} for d in docs],
                })
            conflicts.append({
                "field": field,
                "value": value,
                "companies": companies_payload,
            })

    return {
        "project_identifier": str(project.get("identifier_id") or project_identifier),
        "project_name": project.get("project_name"),
        "has_conflict": bool(conflicts),
        "checked_documents": len(documents),
        "fields": list(fields),
        "conflicts": conflicts,
    }
