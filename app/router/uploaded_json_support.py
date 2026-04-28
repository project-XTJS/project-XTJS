from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException, UploadFile
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DOCUMENT_TYPE_TENDER,
)
from app.service.document_ingest_service import compact_document_payload
from app.service.postgresql_service import PostgreSQLService

BUSINESS_JSON_SUFFIX_RE = re.compile(r"[\s_-]*商务标\s*$", re.IGNORECASE)
TECHNICAL_JSON_SUFFIX_RE = re.compile(r"[\s_-]*技术标\s*$", re.IGNORECASE)


def parse_optional_string_array_json(
    raw_value: Optional[str],
    *,
    field_name: str,
    expected_length: int,
) -> Optional[list[str]]:
    if raw_value is None or not str(raw_value).strip():
        return None

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是合法的 JSON 数组。") from exc

    if not isinstance(parsed, list):
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是 JSON 数组。")
    if len(parsed) != expected_length:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} 的长度必须与上传文件数量一致。",
        )

    return ["" if item is None else str(item).strip() for item in parsed]


async def read_uploaded_json_file(upload: UploadFile, *, field_name: str) -> dict[str, Any]:
    file_name = str(upload.filename or "").strip() or field_name
    raw_bytes = await upload.read()
    if not raw_bytes:
        raise HTTPException(status_code=400, detail=f"{field_name} 不能为空。")

    try:
        payload = json.loads(raw_bytes.decode("utf-8-sig"))
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是 UTF-8 编码的 JSON。") from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} 必须包含合法的 JSON 内容。") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是 JSON 对象。")

    return {
        "file_name": file_name,
        "raw_bytes": raw_bytes,
        "payload": payload,
    }


def derive_uploaded_bidder_key(file_name: str, index: int, *, role: str) -> str:
    stem = Path(str(file_name or "").strip()).stem.strip()
    if role == DOCUMENT_TYPE_BUSINESS_BID:
        normalized = BUSINESS_JSON_SUFFIX_RE.sub("", stem).strip()
    elif role == DOCUMENT_TYPE_TECHNICAL_BID:
        normalized = TECHNICAL_JSON_SUFFIX_RE.sub("", stem).strip()
    else:
        normalized = stem
    return normalized or f"bidder_{index + 1}"


def ensure_unique_bidder_key(candidate: str, used: set[str], index: int) -> str:
    base = str(candidate or "").strip() or f"bidder_{index + 1}"
    unique = base
    suffix = 2
    while unique in used:
        unique = f"{base}_{suffix}"
        suffix += 1
    used.add(unique)
    return unique


async def load_uploaded_bid_json_documents(
    uploads: Optional[list[UploadFile]],
    *,
    field_name: str,
    role: str,
    provided_bidder_keys: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    normalized_uploads = [upload for upload in (uploads or []) if upload is not None]
    if provided_bidder_keys is not None and len(provided_bidder_keys) != len(normalized_uploads):
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} 的 bidder_keys 长度必须与上传文件数量一致。",
        )

    documents: list[dict[str, Any]] = []
    used_bidder_keys: set[str] = set()
    for index, upload in enumerate(normalized_uploads):
        document = await read_uploaded_json_file(
            upload,
            field_name=f"{field_name}[{index}]",
        )
        candidate_key = (
            provided_bidder_keys[index]
            if provided_bidder_keys is not None
            else derive_uploaded_bidder_key(document["file_name"], index, role=role)
        )
        document["bidder_key"] = ensure_unique_bidder_key(candidate_key, used_bidder_keys, index)
        document["document_type"] = role
        document["uploaded_index"] = index + 1
        documents.append(document)
    return documents


async def ensure_upload_project(
    db_service: PostgreSQLService,
    project_identifier: Optional[str],
) -> tuple[dict[str, Any], bool]:
    normalized_identifier = (project_identifier or "").strip()
    if normalized_identifier:
        existing = await run_in_threadpool(
            db_service.get_project_by_identifier,
            normalized_identifier,
        )
        if existing:
            return existing, False
        try:
            created = await run_in_threadpool(
                db_service.create_project,
                normalized_identifier,
            )
            return created, True
        except PsycopgError as exc:
            if getattr(exc, "pgcode", None) == "23505":
                existing = await run_in_threadpool(
                    db_service.get_project_by_identifier,
                    normalized_identifier,
                )
                if existing:
                    return existing, False
            raise

    created = await run_in_threadpool(db_service.create_project)
    return created, True


def _build_uploaded_json_file_url(
    *,
    project_identifier: str,
    document_type: str,
    file_name: str,
    bidder_key: Optional[str] = None,
) -> str:
    safe_project_identifier = str(project_identifier or "project").strip() or "project"
    safe_file_name = Path(str(file_name or "document.json").strip() or "document.json").name
    prefix = bidder_key.strip() if bidder_key else "shared"
    return f"json-upload://{safe_project_identifier}/{document_type}/{prefix}/{safe_file_name}"


async def _create_uploaded_json_document(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    uploaded_document: dict[str, Any],
    document_type: str,
) -> dict[str, Any]:
    created = await run_in_threadpool(
        db_service.create_document_with_content,
        uploaded_document["file_name"],
        _build_uploaded_json_file_url(
            project_identifier=project_identifier,
            document_type=document_type,
            file_name=uploaded_document["file_name"],
            bidder_key=uploaded_document.get("bidder_key"),
        ),
        document_type,
        uploaded_document["payload"],
    )
    document = dict(created["document"])
    return {
        **uploaded_document,
        "document": document,
        "document_summary": compact_document_payload(document),
    }


async def persist_uploaded_json_project_documents(
    *,
    db_service: PostgreSQLService,
    tender_document: dict[str, Any],
    business_bid_documents: Optional[list[dict[str, Any]]] = None,
    technical_bid_documents: Optional[list[dict[str, Any]]] = None,
    project_identifier: Optional[str] = None,
) -> dict[str, Any]:
    project, project_created = await ensure_upload_project(db_service, project_identifier)
    resolved_project_identifier = str(project["identifier_id"])

    persisted_tender = await _create_uploaded_json_document(
        db_service=db_service,
        project_identifier=resolved_project_identifier,
        uploaded_document=tender_document,
        document_type=DOCUMENT_TYPE_TENDER,
    )
    persisted_business = [
        await _create_uploaded_json_document(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            uploaded_document=document,
            document_type=DOCUMENT_TYPE_BUSINESS_BID,
        )
        for document in (business_bid_documents or [])
    ]
    persisted_technical = [
        await _create_uploaded_json_document(
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            uploaded_document=document,
            document_type=DOCUMENT_TYPE_TECHNICAL_BID,
        )
        for document in (technical_bid_documents or [])
    ]

    relation_items: list[dict[str, Any]] = []
    skipped_items: list[dict[str, Any]] = []
    bound_relation_ids: dict[str, int] = {}

    if persisted_business and persisted_technical:
        if len(persisted_business) != len(persisted_technical):
            skipped_items.append(
                {
                    "reason": "business_technical_count_mismatch",
                    "business_bid_count": len(persisted_business),
                    "technical_bid_count": len(persisted_technical),
                }
            )
        else:
            for index, (business_document, technical_document) in enumerate(
                zip(persisted_business, persisted_technical),
                start=1,
            ):
                try:
                    relation = await run_in_threadpool(
                        db_service.bind_project_documents,
                        resolved_project_identifier,
                        persisted_tender["document"]["identifier_id"],
                        business_document["document"]["identifier_id"],
                        technical_document["document"]["identifier_id"],
                    )
                    relation_id = int(relation["id"])
                    bound_relation_ids[business_document["document"]["identifier_id"]] = relation_id
                    bound_relation_ids[technical_document["document"]["identifier_id"]] = relation_id
                    relation_items.append(
                        {
                            "index": index,
                            "status": "bound",
                            "relation": relation,
                            "business_bid_document_identifier": business_document["document"]["identifier_id"],
                            "technical_bid_document_identifier": technical_document["document"]["identifier_id"],
                            "bidder_key": business_document.get("bidder_key") or technical_document.get("bidder_key"),
                        }
                    )
                except ValueError as exc:
                    relation_items.append(
                        {
                            "index": index,
                            "status": "failed",
                            "error": str(exc),
                            "business_bid_document_identifier": business_document["document"]["identifier_id"],
                            "technical_bid_document_identifier": technical_document["document"]["identifier_id"],
                            "bidder_key": business_document.get("bidder_key") or technical_document.get("bidder_key"),
                        }
                    )
                except PsycopgError as exc:
                    relation_items.append(
                        {
                            "index": index,
                            "status": "failed",
                            "error": f"数据库错误：{exc}",
                            "business_bid_document_identifier": business_document["document"]["identifier_id"],
                            "technical_bid_document_identifier": technical_document["document"]["identifier_id"],
                            "bidder_key": business_document.get("bidder_key") or technical_document.get("bidder_key"),
                        }
                    )
    else:
        skipped_items.append(
            {
                "reason": "missing_business_or_technical_documents",
                "business_bid_count": len(persisted_business),
                "technical_bid_count": len(persisted_technical),
            }
        )

    return {
        "project": project,
        "project_created": project_created,
        "tender_document": persisted_tender,
        "business_bid_documents": persisted_business,
        "technical_bid_documents": persisted_technical,
        "binding": {
            "input_mode": "uploaded_json_files",
            "binding_mode": "paired_by_upload_order",
            "summary": {
                "project_created": project_created,
                "persisted_document_count": 1 + len(persisted_business) + len(persisted_technical),
                "business_bid_document_count": len(persisted_business),
                "technical_bid_document_count": len(persisted_technical),
                "relation_bound_count": sum(
                    1 for item in relation_items if item.get("status") == "bound"
                ),
                "relation_failed_count": sum(
                    1 for item in relation_items if item.get("status") == "failed"
                ),
                "relation_skipped_count": len(skipped_items),
            },
            "tender_document": persisted_tender["document_summary"],
            "business_bid_documents": [
                {
                    "index": item["uploaded_index"],
                    "bidder_key": item.get("bidder_key"),
                    "document": item["document_summary"],
                    "relation_id": bound_relation_ids.get(item["document"]["identifier_id"]),
                }
                for item in persisted_business
            ],
            "technical_bid_documents": [
                {
                    "index": item["uploaded_index"],
                    "bidder_key": item.get("bidder_key"),
                    "document": item["document_summary"],
                    "relation_id": bound_relation_ids.get(item["document"]["identifier_id"]),
                }
                for item in persisted_technical
            ],
            "relations": relation_items,
            "skipped_items": skipped_items,
        },
    }


def build_uploaded_project_document_records(
    persisted_documents: dict[str, Any],
) -> list[dict[str, Any]]:
    tender_document = dict(persisted_documents["tender_document"]["document"])
    business_documents = list(persisted_documents.get("business_bid_documents") or [])
    technical_documents = list(persisted_documents.get("technical_bid_documents") or [])

    relation_id_by_document: dict[str, Any] = {}
    for item in (persisted_documents.get("binding") or {}).get("relations") or []:
        if item.get("status") != "bound":
            continue
        relation = item.get("relation") or {}
        relation_id = relation.get("id")
        business_identifier = item.get("business_bid_document_identifier")
        technical_identifier = item.get("technical_bid_document_identifier")
        if business_identifier:
            relation_id_by_document[str(business_identifier)] = relation_id
        if technical_identifier:
            relation_id_by_document[str(technical_identifier)] = relation_id

    records: list[dict[str, Any]] = []
    for role, documents in (
        (DOCUMENT_TYPE_BUSINESS_BID, business_documents),
        (DOCUMENT_TYPE_TECHNICAL_BID, technical_documents),
    ):
        for item in documents:
            document = dict(item["document"])
            identifier_id = str(document.get("identifier_id") or "").strip()
            records.append(
                {
                    "relation_id": relation_id_by_document.get(identifier_id),
                    "relation_role": role,
                    "identifier_id": identifier_id,
                    "document_type": role,
                    "file_name": document.get("file_name"),
                    "file_url": document.get("file_url"),
                    "extracted": document.get("extracted"),
                    "content": document.get("content"),
                    "create_time": document.get("create_time"),
                    "bidder_key": item.get("bidder_key"),
                    "uploaded_index": item.get("uploaded_index"),
                    "tender_identifier_id": tender_document.get("identifier_id"),
                    "tender_document_type": tender_document.get("document_type"),
                    "tender_file_name": tender_document.get("file_name"),
                    "tender_file_url": tender_document.get("file_url"),
                    "tender_extracted": tender_document.get("extracted"),
                    "tender_content": tender_document.get("content"),
                }
            )
    return records
